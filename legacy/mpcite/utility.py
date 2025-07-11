from mpcite.models import (
    ConnectionModel,
    ELinkPostResponseModel,
    ELinkGetResponseModel,
    DOIRecordModel,
    ExplorerGetJSONResponseModel,
    ElinkResponseStatusEnum,
)
from abc import abstractmethod, ABCMeta
from typing import Union, List, Dict
import logging
import requests
from urllib3.exceptions import HTTPError
from xmltodict import parse
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
import json
from tqdm import tqdm
import bibtexparser
import time
from typing import Any


class Adapter(metaclass=ABCMeta):
    def __init__(self, config: ConnectionModel):
        self.config = config
        logging.getLogger("urllib3").setLevel(
            logging.ERROR
        )  # forcefully disable logging from urllib3
        logging.getLogger("dicttoxml").setLevel(
            logging.ERROR
        )  # forcefully disable logging from dicttoxml
        logging.getLogger("bibtexparser.bparser").setLevel(logging.ERROR)
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def post(self, data):
        pass

    @abstractmethod
    def get(self, params):
        pass


class ELinkAdapter(Adapter):
    INVALID_URL_STATUS_MESSAGE = "URL entered is invalid or unreachable."
    MAXIMUM_ABSTRACT_LENGTH_MESSAGE = (
        " Abstract exceeds maximum length of 12000 characters."
    )

    def post(self, data: bytes) -> List[ELinkPostResponseModel]:
        """
        Post xml. The xml is assumed to be in the format that ELINK wants. Note that this xml may contain
        multiple records, and therefore ELink may response with multiple objects

        Args:
            data: data to post

        Returns:
            Elink Response.
        """
        r = requests.post(
            self.config.endpoint,
            auth=(self.config.username, self.config.password),
            data=data,
        )
        self.logger.debug("Your data has been posted")
        if r.status_code != 200:
            self.logger.error(f"POST for {data} failed")
            raise HTTPError(f"POST for {data} failed")
        else:
            self.logger.debug("Parsing Elink Response")
            content: Dict[str, Dict[str, ELinkPostResponseModel]] = parse(r.content)
            if content["records"] is None:
                raise HTTPError(
                    f"POST for {data} failed because there's no data to post"
                )
            to_return = []
            for _, elink_responses in content["records"].items():
                if type(elink_responses) == list:
                    """
                    This is the case where you posted multiple items
                    """
                    for elink_response in elink_responses:
                        e = self.parse_obj_to_elink_post_response_model(elink_response)
                        if e is not None:
                            self.logger.debug(
                                f"Received mp-id=[{e.accession_num}] - OSTI-ID = [{e.osti_id}]"
                            )
                            to_return.append(e)
                else:
                    """
                    This is the case where you posted only one item
                    """
                    e = self.parse_obj_to_elink_post_response_model(elink_responses)
                    if e is not None:
                        to_return.append(e)
            return to_return

    def post_collection(self, data: bytes) -> requests.Response:
        """
        Post xml. The xml is assumed to be in the format that ELINK wants. Note that this xml may contain
        multiple records, and therefore ELink may response with multiple objects

        Args:
            data: data to post

        Returns:
            Elink Response.
        """
        r = requests.post(
            self.config.endpoint,
            auth=(self.config.username, self.config.password),
            data=data,
        )
        return r

    def parse_obj_to_elink_post_response_model(
        self, obj
    ) -> Union[None, ELinkPostResponseModel]:
        """
        Parse a dictionary to ELink Post Response model, catch the error and log error, don't let it error out.

        Args:
            obj: Object to be parsed into ELinkPostResponseModel

        Returns:
            None if cannot parse, instance of ElinkReponseModel if able to parse
        """
        try:
            elink_response_record = ELinkPostResponseModel.parse_obj(obj)
            return elink_response_record
        except Exception as e:
            self.logger.error(
                f"Skipping. Error:{e}.\n Cannot Parse the received Elink Response: \n{obj} "
            )
            return None

    @classmethod
    def prep_posting_data(cls, items: List[dict]) -> bytes:
        """
        using dicttoxml and customized xml configuration to generate posting data according to Elink Specification

        Args:
            items: list of dictionary of data

        Returns:
            xml data in bytes, ready to be sent via request module
        """

        def my_item_func(x):
            if x == "contributors":
                return "contributor"
            elif x == "records":
                return "record"

        xml = dicttoxml(
            items, custom_root="records", attr_type=False, item_func=my_item_func
        )
        records_xml = parseString(xml)
        items = records_xml.getElementsByTagName("item")
        for item in items:
            records_xml.renameNode(item, "", item.parentNode.nodeName[:-1])
        for item in records_xml.getElementsByTagName("contributor"):
            item.setAttribute("contributorType", "Researcher")
        return records_xml.toxml().encode("utf-8")

    def get(self, mpid_or_ostiid: str) -> Union[None, ELinkGetResponseModel]:
        """
        get a single ELinkGetResponseModel from mpid

        Args:
            mpid_or_ostiid: mpid to query

        Returns:
            ELinkGetResponseModel
        """
        key = (
            "site_unique_id"
            if "mp-" in mpid_or_ostiid or "mvc-" in mpid_or_ostiid
            else "osti_id"
        )
        payload = {key: mpid_or_ostiid}
        self.logger.debug(
            "GET from {} w/i payload = {} ...".format(self.config.endpoint, payload)
        )
        r = requests.get(
            self.config.endpoint,
            auth=(self.config.username, self.config.password),
            params=payload,
        )
        if r.status_code == 200:
            elink_response_xml = r.content
            return ELinkGetResponseModel.parse_obj(
                parse(elink_response_xml)["records"]["record"]
            )
        else:
            msg = f"Error code from GET is {r.status_code}"
            self.logger.error(msg)
            raise HTTPError(msg)

    def get_multiple(
        self, mp_ids: List[str], chunk_size=10
    ) -> List[ELinkGetResponseModel]:
        if len(mp_ids) > chunk_size:
            self.logger.info(
                f"Found and downloading [{len(mp_ids)}] Elink Data matches in chunks of {chunk_size}"
            )
            # chunck it up
            result = []
            for i in tqdm(range(0, len(mp_ids), chunk_size)):
                chunk = self.get_multiple_helper(mp_ids=mp_ids[i : i + chunk_size])
                result.extend(chunk)
                time.sleep(1)
            return result
        else:
            return self.get_multiple_helper(mp_ids=mp_ids)

    def get_multiple_helper(self, mp_ids: List[str]) -> List[ELinkGetResponseModel]:
        """
        get a list of elink responses from mpid-s
        Args:
            mp_ids: list of mpids

        Returns:
            list of ELinkGetResponseModel
        """
        if len(mp_ids) == 0:
            return []
        payload = {"accession_num": "(" + " ".join(mp_ids) + ")", "rows": len(mp_ids)}
        r = requests.get(
            self.config.endpoint,
            auth=(self.config.username, self.config.password),
            params=payload,
        )
        if r.status_code == 200:
            elink_response_xml = r.content
            result = []
            try:
                num_found = parse(elink_response_xml)["records"]["@numfound"]
                if num_found == "1":
                    ordered_dict: dict = parse(elink_response_xml)["records"]["record"]
                    if "contributors" in ordered_dict:
                        ordered_dict.pop("contributors")
                    result.append(ELinkGetResponseModel.parse_obj(ordered_dict))
                elif num_found == "0":
                    return []
                else:
                    result: List[ELinkGetResponseModel] = []
                    for record in parse(elink_response_xml)["records"]["record"]:
                        if "contributors" in record:
                            record.pop("contributors")
                        result.append(ELinkGetResponseModel.parse_obj(record))
            except Exception as e:
                self.logger.error(
                    f"Cannot parse returned xml. Error: {e} \n{elink_response_xml}"
                )
            return result
        else:
            msg = f"Error code from GET is {r.status_code}: {r.content}"
            self.logger.error(msg)
            raise HTTPError(msg)

    @classmethod
    def list_to_dict(
        cls, responses: List[ELinkGetResponseModel]
    ) -> Dict[str, ELinkGetResponseModel]:
        """
        helper method to turn a list of ELinkGetResponseModel to mapping of mpid -> ELinkGetResponseModel

        Args:
            responses: list of Elink Responses

        Returns:
            dictionary in the format of
            {
                mp_id : ELinkGetResponseModel
            }
        """

        return {r.accession_num: r for r in responses}

    def process_elink_post_responses(
        self, responses: List[ELinkPostResponseModel]
    ) -> List[DOIRecordModel]:
        """
        find all doi record that needs to update
        will generate a doi record if response.status = sucess. otherwise, print the error and do nothing about it
        (elink will also do nothing about it)

        Args:
            responses: list of elink post responses to process

        Returns:
            list of DOI records that require update
        """
        result: List[DOIRecordModel] = []
        for response in responses:
            if response.status == ElinkResponseStatusEnum.SUCCESS:
                result.append(response.generate_doi_record())
            else:
                # will provide more accurate prompt for known failures
                if response.status_message == ELinkAdapter.INVALID_URL_STATUS_MESSAGE:
                    self.logger.error(
                        f"{[response.accession_num]} failed to update. "
                        f"Error: {response.status_message}"
                        f"Please double check whether this material actually exist "
                        f"on the website "
                        f"[{ELinkGetResponseModel.get_site_url(mp_id=response.accession_num)}]"
                    )
                elif (
                    response.status_message
                    == ELinkAdapter.MAXIMUM_ABSTRACT_LENGTH_MESSAGE
                ):
                    self.logger.error("ELINK: Maximum abstract length reached")
                else:
                    self.logger.error(
                        f"ELINK Unknown error encountered. {response.status_message}"
                    )
        return result


class ExplorerAdapter(Adapter):
    def post(self, data):
        pass

    def get(self, osti_id: str) -> Union[ExplorerGetJSONResponseModel, None]:
        """
        Get Request for Explorer. Get a single item in JSON

        Args:
            osti_id: OSTI ID

        Returns:
            if an item with that OSTI ID exist, return result, otherwise, return None
        """
        payload = {"osti_id": osti_id}
        r = requests.get(
            url=self.config.endpoint,
            auth=(self.config.username, self.config.password),
            params=payload,
        )
        if r.status_code == 200:
            if r.content == b"[]":
                return None
            else:
                content = json.loads(r.content)[0]
                return ExplorerGetJSONResponseModel.parse_obj(content)
        else:
            raise HTTPError(f"Query for OSTI ID = {osti_id} failed")

    def get_bibtex(self, osti_id: str) -> Union[str, None]:
        """
        GET request for Explorer, get bibtex
        Args:
            osti_id: OSTI ID

        Returns:
            bibtex or None if entry wiht that OSTI ID do not exist
        """
        payload = {"osti_id": osti_id}
        header = {"Accept": "application/x-bibtex"}
        try:
            r = requests.get(
                url=self.config.endpoint,
                auth=(self.config.username, self.config.password),
                params=payload,
                headers=header,
            )
        except Exception:
            raise HTTPError(f"Failed to request for OSTI ID = {osti_id}")
        if r.status_code == 200:
            if r.content.decode() == "":
                return None
            return r.content.decode()
        else:
            raise HTTPError(f"Query for OSTI ID = {osti_id} failed")

    def get_multiple_bibtex(self, osti_ids: List[str], chunk_size=10) -> Dict[str, Any]:
        """
        Get multiple bibtex
        Args:
            osti_ids: List of OSTI ID to query
            chunk_size: size to query at once

        Returns:

        """
        self.logger.info(
            f"Found and downloading [{len(osti_ids)}] Bibtex records in chunk of {chunk_size}"
        )
        if len(osti_ids) == 0:
            return dict()
        elif len(osti_ids) < chunk_size:
            return self.get_multiple_bibtex_helper(osti_ids)
        else:
            result = dict()
            for i in tqdm(range(0, len(osti_ids), chunk_size)):
                try:
                    result.update(
                        self.get_multiple_bibtex_helper(osti_ids[i : i + chunk_size])
                    )
                    time.sleep(1)
                except HTTPError:
                    self.logger.error(
                        f"Failed to update osti_ids [{osti_ids[i: i + chunk_size]}], skipping"
                    )
            return result

    def get_multiple_bibtex_helper(self, osti_ids: List[str]) -> Dict[str, str]:
        """
        Get multiple bibtex, assuming that I can send all osti_ids at once
        Args:
            osti_ids: OSTI ID

        Returns:
            return OSTI -> bibtex
        """
        payload = {"rows": len(osti_ids)}
        header = {"Accept": "application/x-bibtex"}
        r = requests.get(
            url=self.config.endpoint + "?osti_id=" + "%20OR%20".join(osti_ids),
            auth=(self.config.username, self.config.password),
            params=payload,
            headers=header,
        )
        if r.status_code == 200:
            if r.content.decode() == "":
                return dict()
            result = self.parse_bibtex(r.content.decode())
            return result
        else:
            raise HTTPError(f"Query for OSTI IDs = {osti_ids} failed")

    def parse_bibtex(self, data: str) -> Dict:
        """
        String of bibtexes in the format of @article{.....}\n@article{.....}
        parse them into a dictionary of
        Args:
            data: data from GET

        Returns:
            dictionary of osti-> bibtex
        """

        new_bib = [line for line in data.splitlines() if "= ," not in line]
        new_bib = "\n".join(new_bib)
        bib_db: bibtexparser.bibdatabase.BibDatabase = bibtexparser.loads(new_bib)
        result = dict()
        for entry in bib_db.entries:
            osti_id = entry["ID"].split("_")[1]
            result[osti_id] = entry
        return result


class ElviserAdapter(Adapter):
    def post(self, data: dict):
        if data.get("doi", "") == "":
            self.logger.debug(
                f"No Elsevier POST for {data.get('identifier')} because it does not have DOI yet"
            )
        else:
            headers = {"x-api-key": self.config.password}
            url = self.config.endpoint
            r = requests.post(url=url, data=json.dumps(data), headers=headers)
            if r.status_code != 202:
                self.logger.error(
                    f"POST for {data.get('identifier')} errored. Reason: {r.content}"
                )

    def get(self, params):
        pass
