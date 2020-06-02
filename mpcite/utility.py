from models import ConnectionModel, ELinkPostResponseModel, ELinkGetResponseModel, DOIRecordModel, \
    ExplorerGetJSONResponseModel, ElinkResponseStatusEnum
from abc import abstractmethod, ABCMeta
from typing import Union, List, Dict
import logging
import requests
from urllib3.exceptions import HTTPError
from xmltodict import parse
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
import json


class Adapter(metaclass=ABCMeta):
    def __init__(self, config: ConnectionModel):
        self.config = config
        logging.getLogger("urllib3").setLevel(logging.ERROR)  # forcefully disable logging from urllib3
        logging.getLogger("dicttoxml").setLevel(logging.ERROR)  # forcefully disable logging from dicttoxml
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def post(self, data):
        pass

    @abstractmethod
    def get(self, params):
        pass


class ELinkAdapter(Adapter):
    INVALID_URL_STATUS_MESSAGE = "URL entered is invalid or unreachable."

    def post(self, data: bytes) -> List[ELinkPostResponseModel]:
        r = requests.post(self.config.endpoint, auth=(self.config.username, self.config.password), data=data)
        if r.status_code != 200:
            self.logger.error(f"POST for {data} failed")
            raise HTTPError(f"POST for {data} failed")
        else:
            content: Dict[str, Dict[str, ELinkPostResponseModel]] = parse(r.content)
            if content["records"] is None:
                raise HTTPError(f"POST for {data} failed because there's no data to post")
            to_return = []
            for _, elink_responses in content["records"].items():
                if type(elink_responses) == list:
                    for elink_response in elink_responses:
                        e = self.parse_obj_to_elink_post_response_model(elink_response)
                        if e is not None:
                            to_return.append(e)
                else:
                    e = self.parse_obj_to_elink_post_response_model(elink_responses)
                    if e is not None:
                        to_return.append(e)
            return to_return

    def parse_obj_to_elink_post_response_model(self, obj) -> Union[None, ELinkPostResponseModel]:
        try:
            elink_response_record = ELinkPostResponseModel.parse_obj(obj)
            return elink_response_record
        except Exception as e:
            self.logger.error(f"Skipping. Error:{e}.\n Cannot Parse the received Elink Response: \n{obj} ")
            return None

    @classmethod
    def prep_posting_data(cls, items: List[dict]) -> bytes:
        """
        using dicttoxml and customized xml configuration to generate posting data according to Elink Specification
        :param items: list of dictionary of data
        :return:
            xml data in bytes, ready to be sent via request module
        """

        xml = dicttoxml(items, custom_root='records', attr_type=False)
        records_xml = parseString(xml)
        items = records_xml.getElementsByTagName('item')
        for item in items:
            records_xml.renameNode(item, '', item.parentNode.nodeName[:-1])
        return records_xml.toxml().encode('utf-8')

    def get(self, mpid_or_ostiid: str) -> Union[None, ELinkGetResponseModel]:
        """
        get a single ELinkGetResponseModel from mpid
        :param mpid_or_ostiid: mpid to query
        :return:
            ELinkGetResponseModel
        """
        key = 'site_unique_id' if 'mp-' in mpid_or_ostiid or 'mvc-' in mpid_or_ostiid else 'osti_id'
        payload = {key: mpid_or_ostiid}
        self.logger.debug('GET from {} w/i payload = {} ...'.format(self.config.endpoint, payload))
        r = requests.get(self.config.endpoint, auth=(self.config.username, self.config.password), params=payload)
        if r.status_code == 200:
            elink_response_xml = r.content
            return ELinkGetResponseModel.parse_obj(parse(elink_response_xml)["records"]["record"])
        else:
            msg = f"Error code from GET is {r.status_code}"
            self.logger.error(msg)
            raise HTTPError(msg)

    def get_multiple(self, mp_ids: List[str]) -> List[ELinkGetResponseModel]:
        """
        get a list of elink responses from mpid-s
        :param mp_ids: list of mpids
        :return:
            list of ELinkGetResponseModel
        """
        payload = {"accession_num": "(" + " ".join(mp_ids) + ")"}
        r = requests.get(self.config.endpoint, auth=(self.config.username, self.config.password), params=payload)
        if r.status_code == 200:
            elink_response_xml = r.content
            return [ELinkGetResponseModel.parse_obj(record) for record in
                    parse(elink_response_xml)["records"]["record"]]
        else:
            msg = f"Error code from GET is {r.status_code} for {mp_ids}"
            self.logger.error(msg)
            raise HTTPError(msg)

    @classmethod
    def list_to_dict(cls, responses: List[ELinkGetResponseModel]) -> Dict[str, ELinkGetResponseModel]:
        """
        helper method to turn a list of ELinkGetResponseModel to mapping of accession_num -> ELinkGetResponseModel

        :return:
            dictionary in the format of
            {
                accession_num : ELinkGetResponseModel
            }
        """
        return {r.accession_num: r for r in responses}
        # return {r.accession_num: r for r in self.get_multiple(mpid_or_ostiids=mpid_or_ostiids)}

    def process_elink_post_responses(self, responses: List[ELinkPostResponseModel]) -> List[DOIRecordModel]:
        """
        find all doi record that needs to update
        will generate a doi record if response.status = sucess. otherwise, print the error and do nothing about it
        (elink will also do nothing about it)

        :param responses: list of elink post responses to process
        :return:
            list of DOI records that require update
        """
        result: List[DOIRecordModel] = []
        for response in responses:
            if response.status == ElinkResponseStatusEnum.SUCCESS:
                result.append(DOIRecordModel.from_elink_response_record(elink_response_record=response))
            else:
                # will provide more accurate prompt for known failures
                if response.status_message == ELinkAdapter.INVALID_URL_STATUS_MESSAGE:
                    self.logger.error(f"{[response.accession_num]} failed to update. "
                                      f"Error: {response.status_message}"
                                      f"Please double check whether this material actually exist "
                                      f"on the website "
                                      f"[{ELinkGetResponseModel.get_site_url(mp_id=response.accession_num)}]")
        return result


class ExplorerAdapter(Adapter):

    def post(self, data):
        pass

    def get(self, osti_id: str) -> Union[ExplorerGetJSONResponseModel, None]:
        payload = {"osti_id": osti_id}
        r = requests.get(url=self.config.endpoint, auth=(self.config.username, self.config.password), params=payload)
        if r.status_code == 200:
            if r.content == b'[]':
                return None
            else:
                content = json.loads(r.content)[0]
                return ExplorerGetJSONResponseModel.parse_obj(content)
        else:
            raise HTTPError(f"Query for OSTI ID = {osti_id} failed")

    def get_bibtex(self, osti_id: str) -> Union[str, None]:
        payload = {"osti_id": osti_id}
        header = {"Accept": "application/x-bibtex"}
        try:
            r = requests.get(url=self.config.endpoint, auth=(self.config.username, self.config.password), params=payload,
                             headers=header)
        except:
            raise HTTPError(f"Failed to request for OSTI ID = {osti_id}")
        if r.status_code == 200:
            if r.content.decode() == '':
                return None
            return r.content.decode()
        else:
            raise HTTPError(f"Query for OSTI ID = {osti_id} failed")

    def append_bibtex(self, doi_record: DOIRecordModel) -> bool:
        """
        find bibtex for a DOI record, return true if success, false otherwise
        :param doi_record: DOI record to find bibtex
        :return:
            True if sucess, false otherwise
        """
        if doi_record.get_osti_id() != '' or doi_record.get_osti_id() is not None:
            try:
                doi_record.bibtex = self.get_bibtex(doi_record.get_osti_id())
                return True
            except HTTPError as e:
                self.logger.error(f"Cannot get bibtex for {doi_record.material_id}. Error: {e}")
                return False


class ElviserAdapter(Adapter):
    def post(self, data):
        pass

    def get(self, params):
        pass


