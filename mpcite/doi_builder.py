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

    From a high level, this Builder works as follows:
    1. get_items -> get all the records that require registration / update
    2. process_item -> compute the update to one particular data from get_items
    3. update_targets -> batch update DOI collection and update remote services

    Please note that the default run() function ties the above three functions together

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
        mp_id="mp-1237770"
        self.logger.info("Processing document with task_id = {}".format(mp_id))
        try:
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
            to_update: List[DOIRecordModel] = self.elink_adapter.process_elink_post_responses(responses=
                                                                                              elink_post_responses)

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

    def find_new_materials(self) -> List[str]:
        """
        find new materials to add by computing the difference between the MP and DOI collection keys

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
        # find distinct mp_ids that are currently in the doi collection
        all_keys = self.adapter.doi_store.distinct(field=self.adapter.doi_store.key)
        self.logger.info(f"Syncing {len(all_keys)} DOIs")

        # ask elink if it has those DOI records
        elink_records_dict: Dict[str, ELinkGetResponseModel] = ELinkAdapter.list_to_dict(
            self.elink_adapter.get_multiple(mpid_or_ostiids=all_keys))

        # find all DOIs that are currently in the DOI collection
        doi_records: List[DOIRecordModel] = \
            [DOIRecordModel.parse_obj(i)
             for i in self.adapter.doi_store.query(criteria={self.adapter.doi_store.key: {"$in": all_keys}})]

        to_update: List[DOIRecordModel] = []
        to_delete: List[DOIRecordModel] = []

        # for each doi in the DOI collection, check if it exist in elink
        for doi_record in doi_records:
            if doi_record.material_id not in elink_records_dict:
                # if a DOI entry is in DOI collection, but not in Elink, add it to the delete list
                to_delete.append(doi_record)
            elif doi_record.should_update(elink_records_dict[doi_record.material_id], logger=self.logger):
                # if a DOI entry needs to updated, add it to the update list. Please note that should_update will
                # only update the DOI entry stored in memory, still need to update the actual DOI entry in database
                to_update.append(doi_record)

        self.logger.debug(f"Updating {len(to_update)} records because E-Link record does not match DOI Collection")
        self.logger.debug(f"Deleting {len(to_delete)} records because they are in DOI collection but not in E-Link")

        # send database query for actual updates/deletions
        self.adapter.doi_store.update(docs=[r.dict() for r in to_update], key=self.adapter.doi_store.key)
        self.adapter.doi_store.remove_docs(criteria=
                                           {self.adapter.doi_store.key: {'$in': [r.material_id for r in to_delete]}})

    def generate_elink_model(self, mp_id: str) -> ELinkGetResponseModel:
        """
        Generate ELink Get model by mp_id

        :param mp_id: material id of the Elink model trying to generate
        :return:
            instance of ELinkGetResponseModel
        """
        material = self.adapter.materials_store.query_one(criteria={self.adapter.materials_store.key: mp_id})
        if material is None:
            msg = f"Material {mp_id} is not found in the materials store"
            self.logger.error(msg)
            raise DoiBuilder.MaterialNotFound(msg)
        else:
            material = MaterialModel.parse_obj(material)

        elink_record = ELinkGetResponseModel(osti_id=self.adapter.get_osti_id(mp_id=mp_id),
                                             title=ELinkGetResponseModel.get_title(material=material),
                                             product_nos=mp_id,
                                             accession_num=mp_id,
                                             publication_date=material.last_updated.strftime('%m/%d/%Y'),
                                             site_url=ELinkGetResponseModel.get_site_url(mp_id=mp_id),
                                             keywords=ELinkGetResponseModel.get_keywords(material=material),
                                             description=self.adapter.get_material_description(mp_id)
                                             )
        return elink_record

