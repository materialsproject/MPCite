from maggma.core.builder import Builder
from typing import Iterable, List, Dict, Union
from mpcite.utility import ELinkAdapter, ExplorerAdapter, ElviserAdapter
from mpcite.models import OSTIModel, DOIRecordModel, ELinkGetResponseModel, MaterialModel, ELinkPostResponseModel, \
    ElsevierPOSTContainerModel, ConnectionModel, RoboCrysModel, MongoConnectionModel
from urllib3.exceptions import HTTPError
from datetime import datetime
from tqdm import tqdm
from maggma.stores import Store, MongoStore


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
                 osti: OSTIModel,
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
        self.osti = osti
        self.elsevier = elsevier
        self.elink_adapter = ELinkAdapter(osti.elink)
        self.explorer_adapter = ExplorerAdapter(osti.explorer)
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
            1. Perform synchronization if sync is True
                - Note that this is a ONE WAY sync, it only pulls data from remote server and checks whether local data
                matches
                - If a mismatch happened
                    - if a mismatch is caused by there is this material from remote server, but not in our local DOI
                    collection
                        - Something VERY wrong happend, log that material and append error field
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

        to_update = self.doi_store.distinct(self.doi_store.key, criteria={"valid": False})
        self.logger.debug(f"Found [{len(to_update)}] materials that are invalid, need to be updated")

        overall = to_update

        if len(overall) < self.max_doi_requests:
            new_materials_to_register = self.find_new_materials()
            overall.extend(new_materials_to_register)

        total = len(overall)

        self.logger.debug(f"Capping the number of updates / register to [{self.max_doi_requests}]")
        overall = overall[:self.max_doi_requests]

        self.logger.info(f"[{total}] materials needs registered / updated. Updating the first [{len(overall)}] "
                         f"materials due to bandwidth limit")
        materials = self.materials_store.query(criteria={self.materials_store.key: {"$in": overall}})
        for m in materials:
            yield m

    def process_item(self, item) -> Union[None, dict]:
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
            material = MaterialModel.parse_obj(item)
            self.logger.info("Processing document with task_id = {}".format(material.task_id))
            elink_post_record = self.generate_elink_model(material=material)
            elsevier_post_record = self.generate_elsevier_model(material=material)
            return {"elink_post_record": elink_post_record,
                    "elsevier_post_record": elsevier_post_record,
                    "mp_id": material.task_id}
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
            to_update: List[DOIRecordModel] = self.elink_adapter.process_elink_post_responses(
                responses=elink_post_responses)
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
                    except Exception as e:
                        self.logger.error(msg="Unable to post because {}".format(e.__str__()))

            # update doi collection
            self.doi_store.update(docs=[r.dict() for r in to_update], key=self.doi_store.key)
            self.logger.info(f"Attempted to update / register [{len(to_update)}] record(s) "
                             f"- [{len(to_update) - failed_count}] Succeeded - [{failed_count}] Failed")
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

    def find_new_materials(self) -> List[str]:
        """
        find new materials to add by computing the difference between the MP and DOI collection keys

        :return:
            list of mp_id
        """
        materials = set(self.materials_store.distinct(self.materials_store.key))
        dois = set(self.doi_store.distinct(self.doi_store.key))
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
        self.logger.info("Start Syncing all materials. Note that this operation will take very long, "
                         "you may terminate it at anypoint, nothing bad will happen. "
                         "You may turn off sync by setting the sync flag to False")
        # all_keys = self.materials_store.distinct(field=self.materials_store.key)
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
        doi_records: List[DOIRecordModel] = \
            [DOIRecordModel.parse_obj(i)
             for i in self.doi_store.query(criteria={self.doi_store.key: {"$in": all_keys}})]

        to_update: List[DOIRecordModel] = []
        records_with_errors: List[DOIRecordModel] = []

        # iterate through doi records to classify them as being updated or deleted
        self.logger.debug("Syncing your local DOI collection")
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
        elsevier_model = ElsevierPOSTContainerModel(identifier=material.task_id,
                                                    title=ElsevierPOSTContainerModel.get_title(material),
                                                    doi=doi,
                                                    url=ElsevierPOSTContainerModel.get_url(material.task_id),
                                                    keywords=ElsevierPOSTContainerModel.get_keywords(material),
                                                    date=datetime.now().date().__str__(),
                                                    dateCreated=ElsevierPOSTContainerModel.get_date_created(material),
                                                    dateAvailable=ElsevierPOSTContainerModel.get_date_available(
                                                        material),
                                                    description=self.get_material_description(material.task_id)
                                                    )
        return elsevier_model

    def update_doi_record(self, doi_record, elink_record: ELinkGetResponseModel, bibtex_dict: dict):
        """
        Policies for updating a DOI record
        :param doi_record: doi record to update
        :param elink_record: elink record to check against
        :param bibtex_dict: dictionary of bibtex to fetch from.
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
        if doi_record.get_status() != elink_record.doi["@status"]:
            to_update = True
            self.logger.debug(f"status update for {doi_record.material_id} to {elink_record.doi['@status']}")
            doi_record.set_status(elink_record.doi["@status"])

        # update the bibtex if nessesary
        robo = self.robocrys_store.query_one(criteria={self.robocrys_store.key: doi_record.material_id})
        if robo is not None:
            doi_record.bibtex = RoboCrysModel.parse_obj(robo).description
        explorer_bibtex = bibtex_dict.get(doi_record.get_osti_id, None)
        if doi_record.bibtex != explorer_bibtex:
            # this means that I have a new bibtex anx thus needs to update the remote server about the change
            # setting doi_record.valid = False will notify the remote server
            to_update = True
            doi_record.bibtex = bibtex_dict
            doi_record.valid = False
            self.logger.debug(f"Need to update bibtex for mp-id = [{doi_record.material_id}]")

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
        robo_result = self.robocrys_store.query_one(criteria={self.robocrys_store.key: mp_id})
        if robo_result is None:
            return ELinkGetResponseModel.get_default_description()
        else:
            robo_result = RoboCrysModel.parse_obj(robo_result)
            description = robo_result.description
            if description is None:
                description = ELinkGetResponseModel.get_default_description()
            return description[:12000]  # 12000 is the Elink Abstract character limit

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
        print(self.materials_store.as_dict())
        return {
            "materials_collection": self.materials_store.as_dict(),
            "robocrys_collection": self.robocrys_store.as_dict(),
            "dois_collection": self.doi_store.as_dict(),
            "osti": self.osti.dict(),
            "elsevier": self.elsevier.dict(),
            "max_doi_requests": self.max_doi_requests,
            "sync": self.sync,
        }

    @classmethod
    def from_dict(cls, d: dict):
        assert "osti" in d, "Error: OSTI config not found"
        assert "elsevier" in d, "Error: elsevier config not found"
        assert "materials_collection" in d, "Error: materials_collection config not found"
        assert "dois_collection" in d, "Error: dois_collection config not found"
        assert "robocrys_collection" in d, "Error: robocrys_collection config not found"
        assert "max_doi_requests" in d, "Error: max_doi_requests config not found"
        assert "sync" in d, "Error: sync config not found"

        elink = ConnectionModel.parse_obj(d["osti"]["elink"])
        explorer = ConnectionModel.parse_obj(d["osti"]["explorer"])
        elsevier = ConnectionModel.parse_obj(d["elsevier"])
        osti = OSTIModel(elink=elink, explorer=explorer)
        materials_store = cls._create_mongostore(config=d, config_collection_name="materials_collection")
        robocrys_store = cls._create_mongostore(config=d, config_collection_name="robocrys_collection")
        doi_store = cls._create_mongostore(config=d, config_collection_name="dois_collection")

        max_doi_requests = d["max_doi_requests"]
        sync = d["sync"]
        bld = DoiBuilder(materials_store=materials_store,
                         robocrys_store=robocrys_store,
                         doi_store=doi_store,
                         osti=osti,
                         elsevier=elsevier,
                         max_doi_requests=max_doi_requests,
                         sync=sync)
        return bld

    @classmethod
    def _create_mongostore(cls, config: dict, config_collection_name: str) -> MongoStore:
        """
        Helper method to create a mongoStore instance
        :param config: configuration dictionary
        :param config_collection_name: collection name to build the mongo store
        :return:
            MongoStore instance based on the configuration parameters
        """
        mong_connection_model = MongoConnectionModel.parse_obj(config[config_collection_name])
        return MongoStore(database=mong_connection_model.database,
                          collection_name=mong_connection_model.collection_name,
                          host=mong_connection_model.host,
                          port=mong_connection_model.port,
                          username=mong_connection_model.username,
                          password=mong_connection_model.password,
                          key=mong_connection_model.key)
