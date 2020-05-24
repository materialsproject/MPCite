import logging, requests, pybtex, yaml, os, time
from dicttoxml import dicttoxml
from xmltodict import parse
from maggma.core.builder import Builder
from adapter import OstiMongoAdapter
from typing import Iterable, List, Union, Dict
from models import OSTIModel, ELinkGetResponseModel, DOIRecordModel, \
    RoboCrysModel, MaterialModel, ELinkPostResponseModel
from pybtex.database.input import bibtex
from utility import *


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

    def __init__(self, adapter: OstiMongoAdapter, osti: OSTIModel, send_size=1000, sync=True, **kwargs):
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
        self.elink_adapter = ELinkAdapter(osti.elink)
        self.explorer_adapter = ExplorerAdapter(osti.explorer)
        self.send_size = send_size
        self.sync = sync
        self.logger.debug("DOI Builder Succesfully instantiated")
        self.logger = logging.getLogger("doi_builder")

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
        if self.sync:
            self.logger.info("Start Syncing with E-Link")
            self.sync_doi_collection()
        else:
            self.logger.info("Not syncing in this iteration")

        self.logger.info("Start Finding materials to register/update")
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

    def process_item(self, item: str) -> Union[None, dict]:
        """
        construct a dict of all updates necessary. ex:
        {
            "ElsevierPOSTContainerModel" : ElsevierPOSTContainerModel,
            "elink_post_record":ELinkGetResponseModel
        }

        Args:
            item (str): taskid/mp-id of the material
        Returns:
            dict: a submitted DOI
        """
        mp_id = item
        self.logger.info("Processing document with task_id = {}".format(mp_id))
        try:
            # doi_record = self.generate_doi_record_model(mp_id)
            elink_post_record = self.generate_elink_model(mp_id)
            return {"elink_post_record": elink_post_record}
        except Exception as e:
            self.logger.error(f"Skipping [{item}], Error: {e}")

    def update_targets(self, items: List[Dict[str, Union[ELinkGetResponseModel,
                                                         ElsevierPOSTContainerModel]]]):
        """
        update all items
        example items:
        [
            {
            "ElsevierPOSTContainerModel" : ElsevierPOSTContainerModel,
            "elink_post_record":ELinkGetResponseModel
            },
            {
            "elsevier_post_record" : ElsevierPOSTContainerModel,
            "elink_post_record":ELinkGetResponseModel
            }
        ]
        :param items: a list of items to update.
        :return:
            None
        """
        self.logger.info(f"Start Updating/registering {len(items)} items")
        elink_post_data: List[dict] = []
        elsevier_post_data: List[dict] = []

        # group them
        for item in items:
            if item.get("elink_post_record", None) is not None:
                elink_post_data.append(ELinkGetResponseModel.custom_to_dict(elink_record=item["elink_post_record"]))
            if item.get("elsevier_post_record", None) is not None:
                self.logger.debug("NOT IMPLEMENTED YET")

        # post it
        try:
            data: bytes = ELinkAdapter.prep_posting_data(elink_post_data)
            elink_post_responses: List[ELinkPostResponseModel] = self.elink_adapter.post(data=data)
            to_update: List[DOIRecordModel] = []
            # find all doi record that needs to update
            for response in elink_post_responses:
                if response.status == ElinkResponseStatusEnum.SUCCESS:
                    to_update.append(DOIRecordModel.from_elink_response_record(elink_response_record=response))
                else:
                    # will provide more accurate prompt for known failures
                    if response.status_message == ELinkAdapter.INVALID_URL_STATUS_MESSAGE:
                        self.logger.error(f"{[response.accession_num]} failed to update. "
                                          f"Error: {response.status_message}"
                                          f"Please double check whether this material actually exist "
                                          f"on the website "
                                          f"[{ELinkGetResponseModel.get_site_url(mp_id=response.accession_num)}]")

            # update doi collection
            self.adapter.doi_store.update(docs=[r.dict() for r in to_update], key=self.adapter.doi_store.key)
            self.logger.info(f"{len(to_update)} record(s) updated")
        except HTTPError as e:
            self.logger.error(f"Failed to POST, no updates done. Error: {e}")


    """
    Utility functions
    """

    class MaterialNotFound(Exception):
        pass

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
            None
        """
        all_keys = self.adapter.doi_store.distinct(field=self.adapter.doi_store.key)
        self.logger.info(f"Syncing {len(all_keys)} DOIs")
        elink_records_dict: Dict[str, ELinkGetResponseModel] = self.elink_adapter.get_multiple_in_dict(mpid_or_ostiids=
                                                                                                       all_keys)

        doi_records: List[DOIRecordModel] = \
            [DOIRecordModel.parse_obj(i)
             for i in self.adapter.doi_store.query(criteria={self.adapter.doi_store.key: {"$in": all_keys}})]

        to_update: List[DOIRecordModel] = []
        to_delete: List[DOIRecordModel] = []

        for doi_record in doi_records:
            if doi_record.material_id not in elink_records_dict:
                to_delete.append(doi_record)
            elif doi_record.update(elink_records_dict[doi_record.material_id], logger=self.logger):
                to_update.append(doi_record)

        self.logger.debug(f"Updating {len(to_update)} records because E-Link record does not match DOI Collection")
        self.logger.debug(f"Deleting {len(to_delete)} records because they are in DOI collection but not in E-Link")

        self.adapter.doi_store.update(docs=[r.dict() for r in to_update], key=self.adapter.doi_store.key)
        self.adapter.doi_store.remove_docs(criteria=
                                           {self.adapter.doi_store.key: {'$in': [r.material_id for r in to_delete]}})

    def check_update(self, doi_record: DOIRecordModel, elink_record: ELinkGetResponseModel) -> bool:
        to_update = False
        if doi_record.get_osti_id() != elink_record.osti_id:
            to_update = True
            msg = f"DOI record mismatch for mp_id = {doi_record.material_id}. " \
                  f"Overwriting the one in DOI collection to match OSTI"
            self.logger.error(msg)
            doi_record.doi = elink_record.osti_id
        if doi_record.get_status() != elink_record.doi["@status"]:
            to_update = True
            self.logger.debug(f"status update for {doi_record.material_id} to {elink_record.doi['@status']}")
            doi_record.set_status(elink_record.doi["@status"])
        return to_update

    def get_material_description(self, mp_id: str) -> str:
        """
        find materials description from robocrys database, if not found return the default description

        :param mp_id: mp_id to query for in the robocrys database
        :return:
            description in string
        """
        robo_result = self.adapter.robocrys_store.query_one(criteria={self.adapter.robocrys_store.key: mp_id})
        if robo_result is None:
            return ELinkGetResponseModel.get_default_description()
        else:
            robo_result = RoboCrysModel.parse_obj(robo_result)
            return robo_result.description

    def generate_elink_model(self, mp_id: str) -> ELinkGetResponseModel:
        material = self.adapter.materials_store.query_one(criteria={self.adapter.materials_store.key: mp_id})
        if material is None:
            msg = f"Material {mp_id} is not found in the materials store"
            self.logger.error(msg)
            raise DoiBuilder.MaterialNotFound(msg)
        else:
            material = MaterialModel.parse_obj(material)

        elink_record = ELinkGetResponseModel(osti_id=self._get_osti_id_for_update(mp_id=mp_id),
                                             title=ELinkGetResponseModel.get_title(material=material),
                                             product_nos=mp_id,
                                             accession_num=mp_id,
                                             publication_date=material.last_updated.strftime('%m/%d/%Y'),
                                             site_url=ELinkGetResponseModel.get_site_url(mp_id=mp_id),
                                             keywords=ELinkGetResponseModel.get_keywords(material=material),
                                             description=self.get_material_description(mp_id)
                                             )
        return elink_record

