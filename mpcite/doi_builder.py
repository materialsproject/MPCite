import logging, requests, pybtex, yaml, os, time
from dicttoxml import dicttoxml
from xmltodict import parse
from maggma.core.builder import Builder
from adapter import OstiMongoAdapter
from typing import Iterable, List, Union, Tuple, Dict
from utility import OSTI, ELinkRecord, DOICollectionRecord, RoboCrys, Material, ELinkResponseRecord, ElinkResponseStatus
from pybtex.database.input import bibtex
import json
from xml.dom.minidom import parseString

class DoiBuilder(Builder):
    """
    A builder to combine information from the Materials Project database(MP) and Osti Explorer(OSTI) to produce a *dois*
    collections

    *dois* collection is similar to a "cache" where it maps the mp ids that are in MP to the DOIs that are in the OSTI

    This builder will find all materials that are in MP but are not in the  in the *dois* collection, call this list of
    uncited_materials

    It will submit the uncited_materials to OSTI and constantly check whether OSTI has updated its database

    if it finds one of the uncited_materials in OSTI, it will update the *dois* collection

    """

    def __init__(self, adapter: OstiMongoAdapter, osti: OSTI, send_size=1000, sync=True, **kwargs):
        """
         connection with materials database
            1. establish connection with materials collection (Guaranteed online)
            2. establish connection with doi collection (online or local)
            3. establish connection with robocrys (Guaranteed online)

        establish connection with ELink to submit info

        establish connection with osti explorer to get bibtex
        :param adapter: OstiMongoAdapter that keeps track of materials, doi, and other related stores
        :param osti: osti connection specification
        :param send_size: the maximum send size that E-Link allows
        :param sync: boolean representing whether to sync DOI collection with Elink and Explorer in get_items
        :param kwargs: other keywords fed into Builder(will be documented as development goes on)
        """
        self.adapter = adapter
        super().__init__(sources=[adapter.materials_store, adapter.robocrys_store],
                         targets=[adapter.doi_store],
                         **kwargs)
        self.num_bibtex_errors = 0
        self.osti = osti
        self.send_size = send_size
        self.sync = sync
        self.logger.debug("DOI Builder Succesfully instantiated")
        self.logger = logging.getLogger(__name__)

    def get_items(self) -> Iterable:
        """
        Get a list of material that requires registration with E-Link or an update.

        Step 1[OPTIONAL]: Perform syncing from Elink to DOI database.
        Step 2: Compute materials that requires update or registration
            - A new material requires registration if it is in the MP database, but not in the DOI database
            - A material requires update if its valid field is False
        Step 3: cap the number of materials sent to self.send size due to bandwidth limit

        Note, this function will by default sync with Elink and Explorer. To turn sync off, set self.sync to false

        Returns:
            generator of materials to retrieve/build DOI
        """
        self.logger.info("DoiBuilder Get Item Started")
        if self.sync:
            self.sync_doi_collection()

        new_materials_to_register = self.find_new_materials()

        to_update = self.adapter.doi_store.distinct(self.adapter.doi_store.key, criteria={"valid": False})
        self.logger.debug(f"Found {len(to_update)} materials needs to be updated")

        overall = new_materials_to_register + to_update  # limit the # of items returned
        total = len(overall)
        overall = overall[:self.send_size]
        # overall = to_update
        self.logger.info(f"There are {total} materials that will be registered or updated. Due to bandwidth limit, "
                         f"this run there will be updating the first {len(overall)} materials")
        return overall

    def process_item(self, item: str) -> dict:
        """
        Post item to E-Link
        1. Prepare meta data needed for posting by calling prep_posting_data
        2. turn the pydantic record returned by prep_posting_data to xml format that is accepted by E-Link
        3. post it
        4. do error checking???

        Args:
            item (str): taskid/mp-id of the material
        Returns:
            dict: a submitted DOI
        """
        mp_id = item
        self.logger.info("Processing document with task_id = {}".format(mp_id))
        record: ELinkRecord = self.generate_posting_data(mp_id=mp_id)
        if record.osti_id is None or record.osti_id == '':
            return record.dict(exclude={"osti_id", "doi"})
        else:
            return record.dict(exclude={"doi"})

    def update_targets(self, items: List[dict]):
        """
        send post request
        :param items:
        :return:
            None
        """
        self.logger.debug(f"Updating/registering {len(items)} items")
        xml_items_to_send: bytes = self.prep_posting_data(items)
        response:requests.Response = self.post_data_to_elink(data=xml_items_to_send)

        if response.status_code != 200:
            self.logger.error("POST request failed")
        else:
            self.update_doi_collection(parse(response.content))

        # TODO with updated info returned by E-Link, update DOI Collection


    """
    Utility functions
    """


    class MaterialNotFound(Exception):
        pass

    class MultiValueFoundOnELinkError(Exception):
        pass

    class HTTPError(Exception):
        pass

    def generate_posting_data(self, mp_id: str) -> ELinkRecord:
        """
        Prepares data to submit to E-Link
        Please note that if osti_id is set to anything other than '',
        that means that this will be an update rather than registering for new data
        :param mp_id:
        :return:
            ElinkRecord to be parsed into XML format and then submitted
        """
        material = self.adapter.materials_store.query_one(criteria={self.adapter.materials_store.key: mp_id})
        if material is None:
            msg = f"Material {mp_id} is not found in the materials store"
            self.logger.error(msg)
            raise DoiBuilder.MaterialNotFound(msg)
        else:
            material = Material.parse_obj(material)

        elink_record = ELinkRecord(osti_id=self._get_osti_id_for_update(mp_id=mp_id),
                                   title=ELinkRecord.get_title(material=material),
                                   product_nos=mp_id,
                                   accession_num=mp_id,
                                   publication_date=material.last_updated.strftime('%m/%d/%Y'),
                                   site_url=ELinkRecord.get_site_url(mp_id=mp_id),
                                   keywords=ELinkRecord.get_keywords(material=material),
                                   description=self.find_material_description(mp_id)
                                   )

        return elink_record

    def post_data_to_elink(self, data) -> requests.Response:
        """
        Send data to elink
        :param data: data to post, list of xml data
        :return:
        """
        auth = (self.osti.elink.username, self.osti.elink.password)
        r = requests.post(self.osti.elink.endpoint, auth=auth, data=data)
        return r


    def get_bibtex(self, doi):
        """get bibtex string for single material/doi"""
        time.sleep(.5)
        osti_id = doi.split('/')[-1]
        endpoint = self.explorer.endpoint + '/{}'.format(osti_id)
        auth = None
        if self.explorer.user and self.explorer.password:
            auth = (self.explorer.user, self.explorer.password)
        headers = {'Accept': 'application/x-bibtex'}
        try:
            r = requests.get(endpoint, auth=auth, headers=headers)
        except Exception as ex:
            msg = 'bibtex for {} threw exception: {}'.format(doi, ex)
            self.logger.error(msg)
            raise ValueError(msg)
        if not r.status_code == 200:
            msg = 'bibtex request for {} failed w/ code {}'.format(doi, r.status_code)
            self.logger.error(msg)
            raise ValueError(msg)
        bib_data = pybtex.database.parse_string(r.content, 'bibtex')
        if len(bib_data.entries) > 0:
            return bib_data.to_string('bibtex')
        else:
            msg = 'invalid bibtex for {}'.format(doi)
            self.logger.error(msg)
            raise ValueError(msg)

    def _get_osti_id_for_update(self, mp_id) -> str:
        """
        Used to determine if an update is necessary.

        If '' is returned, implies update is not necessary.

        Otherwise, an update is necessary
        :param mp_id:
        :return:
        """
        doi_entry = self.adapter.doi_store.query_one(criteria={self.adapter.doi_store.key: mp_id})
        if doi_entry is None:
            return ''
        else:
            return doi_entry['doi'].split('/')[-1]

    def find_new_materials(self) -> List[str]:
        """
        find new materials to add by computing the difference between the MP and DOI collection

        :return:
            list of mp_id
        """
        materials = set(self.adapter.materials_store.distinct(self.adapter.materials_store.key))
        dois = set(self.adapter.doi_store.distinct(self.adapter.doi_store.key))
        to_add = list(materials - dois)
        self.logger.debug(f"Found {len(to_add)} new Materials to register")
        return to_add

    def sync_doi_collection(self):
        """
        Sync DOI collection, set the status, and bibtext field[IN PROGRESS],
        double check whether the DOI field matches??
        If an entry is in the DOI collection but is not in E-Link, remove it

        NOTE, this function might take a while to execute
        :return:
        """
        all_keys = self.adapter.doi_store.distinct(field=self.adapter.doi_store.key)
        self.logger.info(f"Syncing {len(all_keys)} DOIs")
        for mp_id in all_keys:
            self.sync_doi_entry(mp_id)

        self.logger.info(f"DOI Collection synced. "
                         f"Removed {len(all_keys) - self.adapter.doi_store.count()} that "
                         f"are in DOI collection but not in E-Link")

    def sync_doi_entry(self, mp_id):
        """
        Given a mp_id, check if it is in the E-Link
        :param mp_id:
        :return:
        """
        self.logger.debug(f"Syncing mpid {mp_id}")
        collection_record = DOICollectionRecord.parse_obj(
            self.adapter.doi_store.query_one(criteria={self.adapter.doi_store.key: mp_id}))

        try:
            elink_response_xml = self.get_elink_response_xml_by_mp_id(mpid_or_ostiid=mp_id)
            elink_response_dict = parse(elink_response_xml)
            if int(elink_response_dict["records"]["@numfound"]) == 1:
                elink_record = ELinkRecord.parse_obj(elink_response_dict["records"]["record"])
                self.sync_doi_entry_helper(collection_record, elink_record)
            if int(elink_response_dict["records"]["@numfound"]) == 0:
                self.logger.info(f"Dirty Material [{mp_id}] Found. "
                                 f"Removing {mp_id} since it is in local DOI collection but not in E-link.")
                self.adapter.doi_store.remove_docs(criteria={self.adapter.doi_store.key: mp_id})
            else:
                msg = f"Multiple records for {mp_id} is found"
                self.logger.error(msg)
                raise DoiBuilder.MultiValueFoundOnELinkError(msg)

        except DoiBuilder.MultiValueFoundOnELinkError or DoiBuilder.HTTPError:
            self.logger.error(f"sync failed for mp_id {mp_id}. Skipping")
            return

    def get_elink_response_xml_by_mp_id(self, mpid_or_ostiid: str) -> Union[None, bytes]:
        """
        Get Elink response by MP-id

        It is gaurenteed that it should find only a single entry, if multiple found, there is a major error

        :param mpid_or_ostiid: mp id in string
        :return:
            a record object
        """
        key = 'site_unique_id' if 'mp-' in mpid_or_ostiid or 'mvc-' in mpid_or_ostiid else 'osti_id'
        payload = {key: mpid_or_ostiid}
        auth = (self.osti.elink.username, self.osti.elink.password)

        self.logger.debug('GET from {} w/i payload = {} ...'.format(self.osti.elink.endpoint, payload))

        r = requests.get(self.osti.elink.endpoint, auth=auth, params=payload)
        if r.status_code == 200:
            return r.content
        else:
            msg = f"Error code from GET is {r.status_code}"
            self.logger.error(msg)
            raise DoiBuilder.HTTPError(msg)

    def sync_doi_entry_helper(self, collection_record: DOICollectionRecord, elink_record: ELinkRecord):
        to_update = False
        if collection_record.get_osti_id() != elink_record.osti_id:
            to_update = True
            msg = f"DOI record mismatch for mp_id = {collection_record.task_id}. " \
                  f"Overwriting the one in DOI collection to match OSTI"
            self.logger.error(msg)
            collection_record.doi = elink_record.osti_id
        if collection_record.get_status() != elink_record.doi["@status"]:
            to_update = True
            self.logger.debug(f"status update for {collection_record.task_id} to {elink_record.doi['@status']}")
            collection_record.set_status(elink_record.doi["@status"])

        if to_update:
            self.adapter.doi_store.update(docs=collection_record.dict(), key=self.adapter.doi_store.key)

    def find_material_description(self, mp_id: str) -> str:
        """
        find materials description from robocrys database, if not found return the default description

        :param mp_id: mp_id to query for in the robocrys database
        :return:
            description in string
        """
        robo_result = self.adapter.robocrys_store.query_one(criteria={self.adapter.robocrys_store.key: mp_id})
        if robo_result is None:
            return ELinkRecord.get_default_description()
        else:
            robo_result = RoboCrys.parse_obj(robo_result)
            return robo_result.description

    def prep_posting_data(self, items: List[dict]) -> bytes:
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
        # dom = parseString(xml)
        # print(dom.toprettyxml())
        # return xml
        # items = xml_items_to_send.getElementsByTagName('item')
        # for item in items:
        #     xml_items_to_send.renameNode(item, '', item.parentNode.nodeName[:-1])
        # return xml_items_to_send

    def update_doi_collection(self, data: Dict[str, Dict[str, ELinkResponseRecord]]):
        """
        update DOI collection

        :param data: dictionary of "records" -> {"record", ELinkResponseRecord}
        :return:
            None
        """
        successful_elink_responses: List[ELinkResponseRecord] = []
        for _, elink_response_record in data["records"].items():
            elink_response_record = ELinkResponseRecord.parse_obj(elink_response_record)
            if elink_response_record.status == ElinkResponseStatus.FAILED:
                self.logger.debug(f"POST for {elink_response_record.accession_num} failed")
            else:
                self.logger.debug(f"POST for {elink_response_record.accession_num} succeeded")
                successful_elink_responses.append(elink_response_record)

        to_update: List[dict] = [DOICollectionRecord.from_elink_response_record(u).dict()
                                 for u in successful_elink_responses]
        for u in to_update:
            self.logger.info(f"Updating {u['task_id']} in DOI collection")
        self.adapter.doi_store.update(docs=to_update, key=self.adapter.doi_store.key)


