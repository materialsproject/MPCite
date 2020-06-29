from maggma.core.builder import Builder
from adapter import OstiMongoAdapter
from typing import Iterable, List, Dict, Union
from utility import ELinkAdapter, ExplorerAdapter, ElviserAdapter
from models import OSTIModel, DOIRecordModel, ELinkGetResponseModel, MaterialModel, ELinkPostResponseModel, \
    ElsevierPOSTContainerModel, LogContent
import logging
from urllib3.exceptions import HTTPError
from datetime import datetime
from pathlib import Path
import json
from models import RoboCrysModel
import os
from tqdm import tqdm

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

    def __init__(self,
                 adapter: OstiMongoAdapter,
                 osti: OSTIModel,
                 send_size=1000,
                 should_sync_from_remote_sites=True,
                 should_register_new_DOI=True,
                 should_sync_all_materials=True,
                 log_folder_path: str = os.getcwd(), **kwargs):
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
        # set connections
        self.elink_adapter = ELinkAdapter(osti.elink)
        self.explorer_adapter = ExplorerAdapter(osti.explorer)
        self.elsevier_adapter = ElviserAdapter(osti.elsevier)

        # set flags
        self.send_size = send_size
        self.should_sync_from_remote_sites = should_sync_from_remote_sites
        self.should_sync_all_materials = should_sync_all_materials
        self.should_register_new_DOI = should_register_new_DOI

        # set logging
        self.logger = logging.getLogger("doi_builder")
        self.logger.debug("DOI Builder Succesfully instantiated")
        self.log_folder_path: Path = Path(log_folder_path)
        if not self.log_folder_path.exists():
            self.log_folder_path.mkdir(parents=True, exist_ok=True)

        self.last_updated_count = 0
        self.created_at_count = 0
        self.last_validated_on_count = 0
        self.elsevier_updated_on_count = 0

    def get_items(self) -> Iterable:
        """
        Get a list of material that requires registration or update with remote servers

        Steps:
            1. Perform synchronization if should_sync_from_remote_sites is True
                - Note that this is a ONE WAY sync, it only pulls data from remote server and checks whether local data matches
                - If a mismatch happened
                    - if a mismatch is caused by there is this material from remote server, but not in our local DOI collection
                        - Something VERY wrong happend, log that material and delete from local DOI collection to maintain cleaniness
                    - If a mismatch is caused by different bibtex or changes in status
                        - update the DOI record as needed, mark the valid field to False as needed.
            2. Get all the materials that require an update
                - A material require an update iff
                    - It has updated its bibtex description
                    - its Valid flag is turned to False
            3. Get all material that require registration, if the flag should_register_new_DOI is set to True
                - A material needs to be registered iff
                    - It is not in the local DOI collection and that it is in the materials collection

        Note: This method should NOT send any PUSH request. It will only send GET request to sync, and if remote server
        needs to be notified of a change in a record, the flag of valid should be turned to False

        Returns:
            generator of materials to retrieve/build DOI
        """
        if self.should_sync_from_remote_sites:
            self.logger.info("Start Syncing with E-Link")
            try:
                self.sync_doi_collection()
            except Exception as e:
                self.logger.error(
                    "SYNC failed, abort syncing, directly continuing to finding new materials. "
                    "Please notify system administrator \n Error: {}".format(e))
        else:
            self.logger.info("Not syncing in this iteration")

        to_update = self.adapter.doi_store.distinct(self.adapter.doi_store.key, criteria={"valid": False})
        self.logger.debug(f"Found [{len(to_update)}] materials that are invalid, need to be updated")

        overall = to_update
        if self.should_register_new_DOI:
            self.logger.info("Start Finding materials to register/update")
            new_materials_to_register = self.find_new_materials()
            overall.extend(new_materials_to_register)

        total = len(overall)
        self.logger.debug(f"Capping the number of updates / register to [{self.send_size}]")
        overall = overall[:self.send_size]

        self.logger.info(f"[{total}] materials needs registered / updated. Updating the first [{len(overall)}] "
                         f"materials due to bandwidth limit")

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
            material = MaterialModel.parse_obj(
                self.adapter.materials_store.query_one(criteria={self.adapter.materials_store.key: mp_id}))
            elink_post_record = self.generate_elink_model(material=material)
            elsevier_post_record = self.generate_elsevier_model(material=material)
            return {"elink_post_record": elink_post_record,
                    "elsevier_post_record": elsevier_post_record,
                    "mp_id": mp_id}
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
        self.logger.debug("Grouping Received Items")
        for item in tqdm(items):
            if item.get("elink_post_record", None) is not None:
                elink_post_data.append(ELinkGetResponseModel.custom_to_dict(elink_record=item["elink_post_record"]))
            if item.get("elsevier_post_record", None) is not None:
                elsevier_post_data.append(item["elsevier_post_record"].dict())

        # post it
        try:
            failed_count = 0

            # Elink POST
            self.logger.info("POST-ing to Elink")
            data: bytes = ELinkAdapter.prep_posting_data(elink_post_data)
            elink_post_responses: List[ELinkPostResponseModel] = self.elink_adapter.post(data=data)
            to_update: List[DOIRecordModel] = self.elink_adapter.process_elink_post_responses(responses=
                                                                                              elink_post_responses)
            failed_count += len(elink_post_data) - len(to_update)

            # elsevier POST
            to_update_dict: Dict[str, DOIRecordModel] = dict()  # mp_id -> DOIRecord
            for doirecord in to_update:
                to_update_dict[doirecord.material_id] = doirecord
            self.logger.debug("POSTing Elsevier data")
            for post_data in elsevier_post_data:
                # if elink post was successful, then it has the doi, add it to elsevier's
                if post_data["identifier"] in to_update_dict:
                    post_data["doi"] = to_update_dict[post_data["identifier"]].doi
                    try:
                        self.elsevier_adapter.post(data=post_data)
                        to_update_dict[post_data["identifier"]].elsevier_updated_on = datetime.now()
                        self.elsevier_updated_on_count += 1
                    except Exception as e:
                        self.logger.error(msg="Unable to post because {}".format(e.__str__()))

            # update doi collection
            self.created_at_count = len(to_update)
            self.last_validated_on_count += len(to_update)
            self.last_updated_count += len(to_update)
            self.adapter.doi_store.update(docs=[r.dict() for r in to_update], key=self.adapter.doi_store.key)
            self.logger.info(f"Attempted to update / register [{len(to_update)}] record(s) "
                             f"- [{len(to_update) - failed_count}] Succeeded - [{failed_count}] Failed")
        except HTTPError as e:
            self.logger.error(f"Failed to POST, no updates done. Error: {e}")
        except Exception as e:
            self.logger.error(f"Failed to POST. No updates done. Error: \n{e}")

    def finalize(self):
        self.logger.info(f"DOI store now has {self.adapter.doi_store.count()} records")
        self.write_log()
        super(DoiBuilder, self).finalize()

    """
    Utility functions
    """

    def find_new_materials(self) -> List[str]:
        """
        find new materials to add by computing the difference between the MP and DOI collection keys

        :return:
            list of mp_id
        """
        materials = set(self.adapter.materials_store.distinct(self.adapter.materials_store.key))
        dois = set(self.adapter.doi_store.distinct(self.adapter.doi_store.key))
        to_add = list(materials - dois)
        self.logger.debug(f"Found [{len(to_add)}] new Materials to register")
        return to_add

    def sync_doi_collection(self):
        """
        Sync DOI collection, set the status, and bibtext field,
        double check whether the DOI field matches
        If an entry is in the DOI collection but is not in E-Link, remove it

        NOTE, this function might take a while to execute
        :return:
            None
        """
        # find distinct mp_ids that needs to be checked against remote servers
        if self.should_sync_all_materials:
            all_keys = self.adapter.materials_store.distinct(field=self.adapter.materials_store.key)
        else:
            all_keys = self.adapter.doi_store.distinct(field=self.adapter.doi_store.key)
        self.logger.info(f"Syncing [{len(all_keys)}] DOIs")

        # ask remote servers for those keys
        # ask elink if it has those DOI records. create a mapping of mp_id -> elink_Record
        elink_records = self.elink_adapter.get_multiple(mp_ids=all_keys, chunk_size=100)
        # osti_id -> elink_record
        elink_records_dict = ELinkAdapter.list_to_dict(elink_records)

        # osti_id -> bibtex
        try:
            bibtex_dict = self.explorer_adapter.get_multiple_bibtex(
                osti_ids=[r.osti_id for r in elink_records], chunk_size=100)
        except HTTPError:
            bibtex_dict = dict()

        # now that i have all the data, I want to check these data against the ones I have in my local DOI collection
        # find all DOIs that are currently in the DOI collection
        doi_records: List[DOIRecordModel] = \
            [DOIRecordModel.parse_obj(i)
             for i in self.adapter.doi_store.query(criteria={self.adapter.doi_store.key: {"$in": all_keys}})]

        to_update: List[DOIRecordModel] = []
        to_delete: List[DOIRecordModel] = []

        # iterate through doi records to classify them as being updated or deleted
        self.logger.debug("Syncing your local DOI collection")
        for doi_record in tqdm(doi_records):
            osti_id = doi_record.get_osti_id()
            if osti_id not in elink_records_dict:
                # if i found a record that is in my local DOI collection, but not in remote server
                # something VERY bad happened, not sure why did this record get published onto remote server
                # delete this record from local DOI collection to maintain cleanliness, but write this record out
                to_delete.append(doi_record)
            else:
                self.update_doi_record(doi_record=doi_record,
                                       elink_record=elink_records_dict.get(osti_id, None),
                                       bibtex=bibtex_dict.get(osti_id, None)
                                       )
                to_update.append(doi_record)

        # send database query for actual updates/deletions
        self.adapter.doi_store.update(docs=[r.dict() for r in to_update], key=self.adapter.doi_store.key)
        # self.adapter.doi_store.remove_docs(criteria={
        #     self.adapter.doi_store.key: {'$in': [r.material_id for r in to_delete]}})
        self.logger.info(f"Updated [{len(to_update)}] records")
        self.logger.info(f"Removed [{len(to_delete)}] records")
        if len(to_delete) > 0:
            self.write_error_log([d.json() for d in to_delete])

    def generate_elink_model(self, material: MaterialModel) -> ELinkGetResponseModel:
        """
        Generate ELink Get model by mp_id

        :param material: material of the Elink model trying to generate
        :return:
            instance of ELinkGetResponseModel
        """

        elink_record = ELinkGetResponseModel(osti_id=self.adapter.get_osti_id(mp_id=material.task_id),
                                             title=ELinkGetResponseModel.get_title(material=material),
                                             product_nos=material.task_id,
                                             accession_num=material.task_id,
                                             publication_date=material.last_updated.strftime('%m/%d/%Y'),
                                             site_url=ELinkGetResponseModel.get_site_url(mp_id=material.task_id),
                                             keywords=ELinkGetResponseModel.get_keywords(material=material),
                                             description=self.adapter.get_material_description(material.task_id)
                                             )
        return elink_record

    def generate_elsevier_model(self, material: MaterialModel) -> ElsevierPOSTContainerModel:
        doi = self.adapter.get_doi(mp_id=material.task_id)
        elsevier_model = ElsevierPOSTContainerModel(identifier=material.task_id,
                                                    title=ElsevierPOSTContainerModel.get_title(material),
                                                    doi=doi,
                                                    url=ElsevierPOSTContainerModel.get_url(material.task_id),
                                                    keywords=ElsevierPOSTContainerModel.get_keywords(material),
                                                    date=datetime.now().date().__str__(),
                                                    dateCreated=ElsevierPOSTContainerModel.get_date_created(material),
                                                    dateAvailable=ElsevierPOSTContainerModel.get_date_available(
                                                        material),
                                                    description=self.adapter.get_material_description(material.task_id)
                                                    )
        return elsevier_model

    def update_doi_record(self, doi_record, elink_record: ELinkGetResponseModel, bibtex: str):
        """
        Policies for updating a DOI record
        :param doi_record:
        :param elink_record:
        :param bibtex:
        :return:
        """
        to_update = False
        if doi_record.get_osti_id() != elink_record.osti_id:
            to_update = True
            # this condition should never be entered, but for the sake of extreme caution
            msg = f"DOI record mismatch for mp_id = {doi_record.material_id}. " \
                  f"Overwriting the one in DOI collection to match OSTI"
            self.logger.error(msg)
            doi_record.doi = elink_record.osti_id
        if doi_record.get_status() != elink_record.doi["@status"]:
            to_update = True
            self.logger.debug(f"status update for {doi_record.material_id} to {elink_record.doi['@status']}")
            doi_record.set_status(elink_record.doi["@status"])

        # update the bibtex if nessesary
        robo = self.adapter.robocrys_store.query_one(criteria={self.adapter.robocrys_store.key: doi_record.material_id})
        if robo is not None:
            doi_record.bibtex = RoboCrysModel.parse_obj(robo).description
        if doi_record.bibtex != bibtex:
            # this means that I have a new bibtex anx thus needs to update the remote server about the change
            # setting doi_record.valid = False will notify the remote server
            to_update = True
            doi_record.bibtex = bibtex
            doi_record.valid = False
            self.logger.debug(f"Need to update bibtex for mp-id = [{doi_record.material_id}]")

        doi_record.last_validated_on = datetime.now()
        self.last_validated_on_count += 1
        if to_update:
            doi_record.last_updated = datetime.now()
            self.last_updated_count += 1

    def write_log(self):
        log_file_path = self.log_folder_path / "mp_cite_log.txt"
        if not log_file_path.exists():
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            file = log_file_path.open('w+')
            file.close()
        file = log_file_path.open("a+")
        logcontent = LogContent(
            last_updated_count=self.last_updated_count,
            created_at_count=self.created_at_count,
            elsevier_updated_on_count=self.elsevier_updated_on_count,
            last_validated_count=self.last_validated_on_count,
            material_data_base_count=self.adapter.materials_store.count(),
            doi_store_count=self.adapter.doi_store.count(),
            bibtex_count=self.adapter.doi_store.count({"bibtex": {"$ne": None}}),
            doi_completed=self.adapter.doi_store.count({"status": {"$eq": "COMPLETED"}}),
            doi_pending=self.adapter.doi_store.count({"status": {"$eq": "PENDING"}}),
        )
        file.write(logcontent.json() + "\n")
        file.close()
        self.logger.debug(f"Log written to {log_file_path.as_posix()}")

    def write_error_log(self, errors: List):
        log_file_path = self.log_folder_path / "mp_cite_errors_log.txt"
        if not log_file_path.exists():
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            file = log_file_path.open('w+')
            file.close()
        file = log_file_path.open("a+")
        json.dump(errors, file, indent=4)
        file.close()
        self.logger.debug(f"Error Logs written to {log_file_path.as_posix()}")

