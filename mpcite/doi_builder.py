from maggma.core.builder import Builder
from typing import Iterable, List, Dict, Union, Set, Optional
from mpcite.utility import ELinkAdapter, ExplorerAdapter, ElviserAdapter
from mpcite.models import DOIRecordModel, ELinkGetResponseModel, MaterialModel, ELinkPostResponseModel, \
    ElsevierPOSTContainerModel, ConnectionModel, RoboCrysModel, DOIRecordStatusEnum
from urllib3.exceptions import HTTPError
from datetime import datetime
from tqdm import tqdm
from maggma.stores import Store
from monty.json import MontyDecoder
import json
import bibtexparser


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
                 materials_store: Store,
                 robocrys_store: Store,
                 doi_store: Store,
                 elink: ConnectionModel,
                 explorer: ConnectionModel,
                 elsevier: ConnectionModel,
                 max_doi_requests=1000,
                 sync=True,
                 **kwargs):
        """
         connection with materials database
            1. establish connection with materials collection (Guaranteed online)
            2. establish connection with doi collection (online or local)
            3. establish connection with robocrys (Guaranteed online)

        establish connection with ELink to submit info

        establish connection with osti explorer to get bibtex


        :param adapter: OstiMongoAdapter that keeps track of materials, doi, and other related stores
        :param osti: osti connection specification
        :param max_doi_requests: the maximum send size that E-Link allows
        :param sync: boolean representing whether to sync DOI collection with Elink and Explorer in get_items
        :param kwargs: other keywords fed into Builder(will be documented as development goes on)
        """
        super().__init__(sources=[materials_store, robocrys_store],
                         targets=[doi_store],
                         **kwargs)
        # set connections
        self.materials_store = materials_store
        self.robocrys_store = robocrys_store
        self.doi_store = doi_store
        self.elsevier = elsevier
        self.elink = elink
        self.explorer = explorer
        self.elink_adapter = ELinkAdapter(elink)
        self.explorer_adapter = ExplorerAdapter(explorer)
        self.elsevier_adapter = ElviserAdapter(self.elsevier)

        # set flags
        self.max_doi_requests = max_doi_requests
        self.sync = sync

        # set logging
        self.logger.debug("DOI Builder Succesfully instantiated")

    def get_items(self) -> Iterable:
        """
        Get a list of material that requires registration or update with remote servers

        Steps:
            1. Synchronization
                - Goal: Generate State Machine for each DOI Record that satisfy the below properties
                    a. this entry is detected in local collection, but not in remote server
                        - this means that there's something VERY wrong happended, log error for this material
                    b. If there is a mismatch for different bibtex from robocrys store
                        - set VALID to False, this will implicitly move this material to the update queue
                    c. If there is a mismatch for the STATUS when pulling data from server
                        - update local DOI collection. do NOT set VALID to False since you don't need to update
                        the server about this, server told you about this change
            2. Populate Update Queue
                - Goal: Generate a list of record from local DOI collection that requires an update
                    a. we know which record needs an update because we have constructed a State Machine with VALID=False
                    for each of the material that requires an update
                    b. The flag VALID also allow users to manually update a material -- if you set VALID to false,
                    this material will be put onto the update queue
            3. Populate New Materials Queue
                - Goal: find unregistered materials to register
                    a. only execute this if the Update Queue has not exceeded 1000 items, which is the limit
                    where OSTI can process it per 6 hours
            4. Cap the number of materials to 1000
                - This is a safety measure, above 1000, OSTI will fail, and things gets complicated

        Note: This method should NOT send any PUSH request. It will only send GET request to sync, and if remote server
        needs to be notified of a change in a record, the flag of valid should be turned to False

        Returns:
            a list of State for each record that needs an update / registration
        """
        if self.sync:
            self.logger.info("Start Syncing with E-Link")
            try:
                self.sync_doi_collection()
            except Exception as e:
                self.logger.error(
                    "SYNC failed, abort syncing, directly continuing to finding new materials. "
                    "Please notify system administrator \n Error: {}".format(e))
        else:
            self.logger.info("Not syncing in this iteration")

        update_ids = self.doi_store.distinct(self.doi_store.key, criteria={"valid": False})
        self.logger.debug(f"Found [{len(update_ids)}] materials that are invalid, need to be updated")

        overall_ids = []
        new_materials_ids: Set[str] = set()
        if len(overall_ids) < self.max_doi_requests:
            failed_ids = set(self.doi_store.distinct(self.doi_store.key, criteria={"status": "FAILURE"})) - \
                         set(overall_ids)
            overall_ids.extend(failed_ids)
        if len(overall_ids) < self.max_doi_requests:
            new_materials_ids = set(self.materials_store.distinct(field=self.materials_store.key)) - \
                                set(self.doi_store.distinct(field=self.doi_store.key))
            overall_ids.extend(new_materials_ids)
        overall_ids = overall_ids[:self.max_doi_requests]
        for ID in overall_ids:
            if ID in new_materials_ids:
                new_doi_record = DOIRecordModel(
                    material_id=ID,
                    status="INIT",
                    valid=False)
                yield new_doi_record
            else:
                yield DOIRecordModel.parse_obj(self.doi_store.query_one(criteria={self.doi_store.key: ID}))

    def process_item(self, item: DOIRecordModel) -> Union[None, dict]:
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
        try:
            material = self.materials_store.query_one(criteria={self.materials_store.key: item.material_id})
            material = MaterialModel.parse_obj(material)
            self.logger.info("Processing document with task_id = {}".format(material.task_id))
            elink_post_record = self.generate_elink_model(material=material)
            elsevier_post_record = self.generate_elsevier_model(material=material)
            return {"elink_post_record": elink_post_record,
                    "elsevier_post_record": elsevier_post_record,
                    "doi_record": item}
        except Exception as e:
            self.logger.error(f"Skipping [{item.material_id}], Error: {e}")

    def update_targets(self, items: List[Dict[str, Union[ELinkGetResponseModel,
                                                         ElsevierPOSTContainerModel,
                                                         DOIRecordModel]]]):
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
        records_dict: Dict[str, DOIRecordModel] = dict()

        # group them
        self.logger.debug("Grouping Received Items")
        for item in tqdm(items):
            doi_record: DOIRecordModel = item["doi_record"]
            records_dict[doi_record.material_id] = doi_record
            if item.get("elink_post_record", None) is not None:
                elink_post_data.append(ELinkGetResponseModel.custom_to_dict(elink_record=item["elink_post_record"]))
            if item.get("elsevier_post_record", None) is not None:
                elsevier_post_data.append(item["elsevier_post_record"].dict())

        # post it
        try:

            # Elink POST
            self.logger.info("POST-ing to Elink")
            data: bytes = ELinkAdapter.prep_posting_data(elink_post_data)
            elink_post_responses: List[ELinkPostResponseModel] = self.elink_adapter.post(data=data)
            self.logger.info("Processing Elink Response")
            for elink_post_response in tqdm(elink_post_responses):
                record: DOIRecordModel = records_dict[elink_post_response.accession_num]
                record.doi = elink_post_response.doi["#text"]
                record.status = elink_post_response.doi["@status"]
                record.valid = True
                record.last_validated_on = datetime.now()
                record.last_updated = datetime.now()
                record.error = "Unkonwn error happend when pushing to ELINK. " if record.status == "Failure" else None

            # now post to elsevier
            self.logger.info("POSTing to elsevier")
            for elsevier in tqdm(elsevier_post_data):
                mp_id = elsevier["identifier"]
                if mp_id in records_dict and records_dict[mp_id].valid:
                    elsevier["doi"] = records_dict[mp_id].doi
                    try:
                        self.elsevier_adapter.post(data=elsevier)
                        records_dict[mp_id].elsevier_updated_on = datetime.now()
                    except Exception as e:
                        self.logger.error(msg="Unable to post because {}".format(e.__str__()))
            self.logger.info("Updating local DOI collection")
            self.doi_store.update(docs=[r.dict() for r in records_dict.values()], key=self.doi_store.key)
            self.logger.info(f"Attempted to update / register [{len(records_dict)}] record(s) "
                             f"- [{len(records_dict) - len(elink_post_responses)}] Failed")

        except HTTPError as e:
            self.logger.error(f"Failed to POST, no updates done. Error: {e}")
        except Exception as e:
            self.logger.error(f"Failed to POST. No updates done. Error: \n{e}")

    def finalize(self):
        self.logger.info(f"DOI store now has {self.doi_store.count()} records")
        super(DoiBuilder, self).finalize()

    """
    Utility functions
    """

    def sync_doi_collection(self):
        """
        Goal: Synchronize DOI Collection against remote servers

        Procedure:
            1. pull data from Elink and Explorer
            2. compare data against local DOI collection
                - if there is a mismatch
                    - if there is a mimatch in Status
                        - update local DOI record's status
                    - if there is a mismatch in Bibtex
                        - set VALID = False, implicitly moving its state to False for later updates
                - update the local DOI collection
            3. return a list of mp_ids that have their VALID=False set.

        Sync DOI collection, set the status, and bibtext field,
        double check whether the DOI field matches

        NOTE, this function might take a while to execute
        :return:
            a list of mp_ids that have its state set to VALID=False
        """
        # find distinct mp_ids that needs to be checked against remote servers
        self.logger.info("Start Syncing all materials. Note that this operation will take very long, "
                         "you may terminate it at anypoint, nothing bad will happen. "
                         "You may turn off sync by setting the sync flag to False")
        all_keys = self.doi_store.distinct(field=self.doi_store.key)
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
        doi_records: List[DOIRecordModel] = []
        for i in self.doi_store.query(criteria={self.doi_store.key: {"$in": all_keys}}):
            doi_records.append(DOIRecordModel.parse_obj(i))

        to_update: List[DOIRecordModel] = []
        records_with_errors: List[DOIRecordModel] = []

        # iterate through doi records to classify them as being updated or deleted
        self.logger.debug("Syncing local DOI collection")
        for doi_record in tqdm(doi_records):
            osti_id = doi_record.get_osti_id()
            if osti_id not in elink_records_dict:
                # if i found a record that is in my local DOI collection, but not in remote server
                # something VERY bad happened, not sure why did this record get published onto remote server
                # delete this record from local DOI collection to maintain cleanliness, but write this record out
                doi_record.error = "Found in Local DOI Collection, but not found in ELink"
                records_with_errors.append(doi_record)
                self.logger.error(f"Found [{doi_record.material_id}] in Local DOI Collection, but not found in ELink")
            else:
                self.update_doi_record(doi_record=doi_record,
                                       elink_record=elink_records_dict.get(osti_id, None),
                                       bibtex_dict=bibtex_dict)
                to_update.append(doi_record)

        # send database query for actual updates
        self.doi_store.update(docs=[r.dict() for r in to_update], key=self.doi_store.key)
        self.doi_store.update(docs=[r.dict() for r in records_with_errors], key=self.doi_store.key)
        self.logger.info(f"Updated [{len(to_update)}] records")
        self.logger.error(f"[{len(records_with_errors)}] records have errors. ")

    def generate_elink_model(self, material: MaterialModel) -> ELinkGetResponseModel:
        """
        Generate ELink Get model by mp_id

        :param material: material of the Elink model trying to generate
        :return:
            instance of ELinkGetResponseModel
        """

        elink_record = ELinkGetResponseModel(osti_id=self.get_osti_id(mp_id=material.task_id),
                                             title=ELinkGetResponseModel.get_title(material=material),
                                             product_nos=material.task_id,
                                             accession_num=material.task_id,
                                             publication_date=material.last_updated.strftime('%m/%d/%Y'),
                                             site_url=ELinkGetResponseModel.get_site_url(mp_id=material.task_id),
                                             keywords=ELinkGetResponseModel.get_keywords(material=material),
                                             description=self.get_material_description(material.task_id)
                                             )
        return elink_record

    def generate_elsevier_model(self, material: MaterialModel) -> ElsevierPOSTContainerModel:
        doi = self.get_doi(mp_id=material.task_id)
        description = self.get_material_description(material.task_id)
        return ElsevierPOSTContainerModel.from_material_model(material=material, doi=doi, description=description)

    def update_doi_record(self, doi_record: DOIRecordModel, elink_record: ELinkGetResponseModel, bibtex_dict: dict):
        """
        Transitioning the state of a doi_record based on its status and bibtex values
            - if status does not equal to remote server's status
                - set to_update to True
            - if bibtex does not equal
                - set to_update to True
                - set Valid to False

        :param doi_record: doi record to update
        :param elink_record: elink record to check against
        :param bibtex_dict: dictionary of bibtex abstract note to fetch from.
        :return:
            None
        """
        to_update = False
        if doi_record.get_osti_id() != elink_record.osti_id:
            to_update = True
            # this condition should never be entered, but for the sake of extreme caution
            msg = f"DOI record mismatch for mp_id = {doi_record.material_id}. " \
                  f"Overwriting the one in DOI collection to match OSTI"
            self.logger.debug(msg)
            doi_record.doi = elink_record.osti_id
        if doi_record.status != DOIRecordStatusEnum[elink_record.doi["@status"]]:
            to_update = True
            self.logger.debug(f"status update for {doi_record.material_id} to {elink_record.doi['@status']}")
            doi_record.set_status(elink_record.doi["@status"])

        # update the bibtex if nessesary
        robo = self.robocrys_store.query_one(criteria={self.robocrys_store.key: doi_record.material_id})
        robo_description = RoboCrysModel.parse_obj(robo).description if robo is not None else None
        explorer_entry = bibtex_dict.get(doi_record.get_osti_id(), None)
        explorer_abstract_note = explorer_entry["abstractnote"] if explorer_entry is not None else None

        # sync explorer with local doi record
        if doi_record.get_bibtex_abstract() != explorer_abstract_note:
            self.logger.debug(f"[{doi_record.material_id}]: Local DOI Bibtex is different from Explorer")
            db = bibtexparser.bibdatabase.BibDatabase()
            db.entries = [explorer_entry]
            doi_record.bibtex = bibtexparser.dumps(db) if explorer_entry is not None else None
            to_update = True

        # sync local doi record with robo and mark valid = False to send to server later
        if robo_description is not None and doi_record.get_bibtex_abstract() != robo_description:
            self.logger.debug(f"[{doi_record.material_id}]: Updates from Robo collection")
            to_update = True
            doi_record.valid = False
        else:
            if doi_record.valid is False:
                # and dont forget to set it back
                self.logger.debug(f"[{doi_record.material_id}] is now valid, setting Valid to True")
                doi_record.valid = True
                to_update = True

        doi_record.last_validated_on = datetime.now()
        if to_update:
            doi_record.last_updated = datetime.now()

    def get_material_description(self, mp_id: str) -> str:
        """
        find materials description from robocrys database, if not found return the default description

        :param mp_id: mp_id to query for in the robocrys database
        :return:
            description in string
        """
        description = RoboCrysModel.get_default_description()
        robo_result = self.robocrys_store.query_one(criteria={self.robocrys_store.key: mp_id})
        if robo_result is None:
            return description
        else:
            robo_result = RoboCrysModel.parse_obj(robo_result)
            robo_description = robo_result.description
            if robo_description is None:
                return description
            return robo_description[:12000]  # 12000 is the Elink Abstract character limit

    def get_doi(self, mp_id) -> str:
        osti_id = self.get_osti_id(mp_id=mp_id)
        if osti_id == '':
            return ''
        else:
            return "10.17188/" + osti_id

    def get_osti_id(self, mp_id) -> str:
        """
        Used to determine if an update is necessary.

        If '' is returned, implies update is not necessary.

        Otherwise, an update is necessary
        :param mp_id:
        :return:
        """
        doi_entry = self.doi_store.query_one(criteria={self.doi_store.key: mp_id})
        if doi_entry is None:
            return ''
        else:
            return doi_entry['doi'].split('/')[-1]

    def as_dict(self) -> dict:
        return {
            "materials_collection": self.materials_store.as_dict(),
            "robocrys_collection": self.robocrys_store.as_dict(),
            "dois_collection": self.doi_store.as_dict(),
            "elink": self.elink.dict(),
            "explorer": self.explorer.dict(),
            "elsevier": self.elsevier.dict(),
            "max_doi_requests": self.max_doi_requests,
            "sync": self.sync,
        }

    @classmethod
    def from_dict(cls, d: dict):
        assert "elsevier" in d, "Error: elsevier config not found"
        assert "materials_collection" in d, "Error: materials_collection config not found"
        assert "dois_collection" in d, "Error: dois_collection config not found"
        assert "robocrys_collection" in d, "Error: robocrys_collection config not found"
        assert "max_doi_requests" in d, "Error: max_doi_requests config not found"
        assert "sync" in d, "Error: sync config not found"

        elink = ConnectionModel.parse_obj(d["elink"])
        explorer = ConnectionModel.parse_obj(d["explorer"])
        elsevier = ConnectionModel.parse_obj(d["elsevier"])

        materials_store = json.loads(json.dumps(d["materials_collection"]), cls=MontyDecoder)
        robocrys_store = json.loads(json.dumps(d["robocrys_collection"]), cls=MontyDecoder)
        doi_store = json.loads(json.dumps(d["dois_collection"]), cls=MontyDecoder)

        max_doi_requests = d["max_doi_requests"]
        sync = d["sync"]
        bld = DoiBuilder(materials_store=materials_store,
                         robocrys_store=robocrys_store,
                         doi_store=doi_store,
                         elink=elink,
                         explorer=explorer,
                         elsevier=elsevier,
                         max_doi_requests=max_doi_requests,
                         sync=sync)
        return bld
