from maggma.core.builder import Builder
from typing import Iterable, List, Dict, Union, Set
from mpcite.utility import ELinkAdapter, ExplorerAdapter
from mpcite.models import (
    DOIRecordModel,
    ELinkGetResponseModel,
    MaterialModel,
    ELinkPostResponseModel,
    ConnectionModel,
    RoboCrysModel,
    DOIRecordStatusEnum,
)
from urllib3.exceptions import HTTPError
from datetime import datetime
from tqdm import tqdm
from maggma.stores import Store
from monty.json import MontyDecoder
import json
import bibtexparser
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import PDFExporter
from pathlib import Path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


class DoiBuilder(Builder):
    """
    DOI builder that automatically pull data from materials collection and robo crystal collection
    to submit data to OSTI. It will also sync Bibtex with ELINK
    """

    def __init__(
        self,
        materials_store: Store,
        robocrys_store: Store,
        doi_store: Store,
        elink: ConnectionModel,
        explorer: ConnectionModel,
        max_doi_requests=1000,
        sync=True,
        report_emails=None,
        **kwargs,
    ):
        """
        Initialize connection with collections and prepare connection with OSTI, Elink and Elsevier

        Args:
            materials_store: Store that connects to the Materials collection
            robocrys_store: Store that connects to the Robocrystal Collection
            doi_store: Store that connects to the DOI Collection
            elink: Connection information to ELink
            explorer: Connection information to Explorer
            elsevier: Connection Information for Elsevier
            max_doi_requests: Maximum DOI POST request that this run will be sending
            sync: True to sync materials database and OSTI, False otherwise
            **kwargs:
        """
        super().__init__(
            sources=[materials_store, robocrys_store], targets=[doi_store], **kwargs
        )
        # set connections
        if report_emails is None:
            report_emails = ["wuxiaohua1011@berkeley.edu", "phuck@lbl.gov"]
        self.materials_store = materials_store
        self.robocrys_store = robocrys_store
        self.doi_store = doi_store
        # self.elsevier = elsevier
        self.elink = elink
        self.explorer = explorer
        self.elink_adapter = ELinkAdapter(elink)
        self.explorer_adapter = ExplorerAdapter(explorer)
        # self.elsevier_adapter = ElviserAdapter(self.elsevier)

        # set flags
        self.max_doi_requests = max_doi_requests
        self.sync = sync

        self.report_emails = (
            ["wuxiaohua1011@berkeley.edu", "phuck@lbl.gov"]
            if report_emails is None
            else report_emails
        )
        self.email_messages = []
        self.has_error = False
        self.config_file_path = None

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
        update_ids = self.doi_store.distinct(
            self.doi_store.key, criteria={"valid": False}
        )
        if len(update_ids) < 1000:
            if self.sync:
                self.logger.info("Start Syncing with E-Link")
                try:
                    self.sync_doi_collection()
                except Exception as e:
                    self.logger.error(
                        "SYNC failed, abort syncing, directly continuing to finding new materials. "
                        "Please notify system administrator \n Error: {}".format(e)
                    )
            else:
                self.logger.info("Not syncing in this iteration")
        else:
            self.logger.info(
                f"NOTE: you already have {len(update_ids)} records that are invalid and will be sent for "
                f"update. For efficiency purpose, im not going to sync"
            )

        update_ids = self.doi_store.distinct(
            self.doi_store.key, criteria={"valid": False}
        )
        self.logger.debug(
            f"Found [{len(update_ids)}] materials that are invalid, need to be updated"
        )

        overall_ids = update_ids
        new_materials_ids: Set[str] = set()
        if len(overall_ids) < self.max_doi_requests:
            failed_ids = set(
                self.doi_store.distinct(
                    self.doi_store.key, criteria={"status": "FAILURE"}
                )
            ) - set(overall_ids)
            overall_ids.extend(failed_ids)
        if len(overall_ids) < self.max_doi_requests:
            new_materials_ids = set(
                self.materials_store.distinct(field=self.materials_store.key)
            ) - set(self.doi_store.distinct(field=self.doi_store.key))
            overall_ids.extend(new_materials_ids)
        overall_ids = overall_ids[: self.max_doi_requests]
        for ID in overall_ids:
            if ID in new_materials_ids:
                new_doi_record = DOIRecordModel(
                    material_id=ID, status=DOIRecordStatusEnum["INIT"], valid=False
                )
                yield new_doi_record
            else:
                yield DOIRecordModel.parse_obj(
                    self.doi_store.query_one(criteria={self.doi_store.key: ID})
                )

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
            if item is None:
                return {}
            material = self.materials_store.query_one(
                criteria={self.materials_store.key: item.material_id}
            )
            material = MaterialModel.parse_obj(material)
            self.logger.info(
                "Processing document with task_id = {}".format(material.task_id)
            )
            elink_post_record = self.generate_elink_model(material=material)
            # elsevier_post_record = self.generate_elsevier_model(material=material)
            return {
                "elink_post_record": elink_post_record,
                # "elsevier_post_record": elsevier_post_record,
                "doi_record": item,
            }
        except Exception as e:
            self.logger.error(f"Skipping [{item.material_id}], Error: {e}")

    def update_targets(
        self, items: List[Dict[str, Union[ELinkGetResponseModel, DOIRecordModel]]]
    ):
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

        Args:
            items: a list of items to update.

        Returns:

        """
        if len(items) == 0:
            return
        self.logger.info(f"Start Updating/registering {len(items)} items")
        elink_post_data: List[dict] = []
        # elsevier_post_data: List[dict] = []
        records_dict: Dict[str, DOIRecordModel] = dict()

        # group them
        self.logger.debug("Grouping Received Items")
        for item in tqdm(items):
            if len(item) == 0:
                continue
            doi_record: DOIRecordModel = item["doi_record"]
            records_dict[doi_record.material_id] = doi_record
            if item.get("elink_post_record", None) is not None:
                elink_post_data.append(
                    ELinkGetResponseModel.custom_to_dict(
                        elink_record=item["elink_post_record"]
                    )
                )
        # post it
        try:

            # Elink POST
            self.logger.info(f"POST-ing {len(elink_post_data)} to Elink")
            data: bytes = ELinkAdapter.prep_posting_data(elink_post_data)
            elink_post_responses: List[
                ELinkPostResponseModel
            ] = self.elink_adapter.post(data=data)
            self.logger.info(f"Processing {len(elink_post_responses)} Elink Responses")
            for elink_post_response in tqdm(elink_post_responses):
                record: DOIRecordModel = records_dict[elink_post_response.accession_num]
                record.doi = elink_post_response.doi["#text"]
                record.status = DOIRecordStatusEnum[
                    elink_post_response.doi["@status"]
                ].value
                record.valid = (
                    True if record.status == DOIRecordStatusEnum.COMPLETED else False
                )
                record.last_validated_on = datetime.now()
                record.last_updated = datetime.now()
                record.error = (
                    f"Unkonwn error happend when pushing to ELINK. "
                    f"Material ID = {record.material_id} | DOi = {record.doi}"
                    if record.status == DOIRecordStatusEnum.FAILURE
                    else None
                )

            self.logger.info("Updating local DOI collection")
            self.doi_store.update(
                docs=[r.dict() for r in records_dict.values()], key=self.doi_store.key
            )
            self.logger.debug(
                f"Updated records with mp_ids: {[r.material_id for r in records_dict.values()]} "
            )
            message = (
                f"Attempted to update / register [{len(records_dict)}] record(s) "
                f"- [{len(records_dict) - len(elink_post_responses)}] Failed"
            )
            self.email_messages.append(message)
            self.logger.info(message)

        except Exception as e:
            self.has_error = True
            self.logger.error(f"Failed to POST. No updates done. Error: \n{e}")

    def finalize(self):
        self.logger.info(f"DOI store now has {self.doi_store.count()} records")
        self.send_email()
        super(DoiBuilder, self).finalize()

    """
    Utility functions
    """

    def sync_doi_collection(self):
        """
        First download data and sync local doi collection
        Then cross check whether local doi collection is up-to-date by cross checking with robocrys

        Returns:
            None
        """
        # find distinct mp_ids that needs to be checked against remote servers
        self.logger.info(
            "Start Syncing all materials. Note that this operation will take very long, "
            "you may terminate it at anypoint, nothing bad will happen. "
            "You may turn off sync by setting the sync flag to False"
        )
        # First I want to download all the data
        all_keys = self.materials_store.distinct(field=self.materials_store.key)
        self.logger.info(f"Downloading [{len(all_keys)}] DOIs")
        elink_records, bibtex_dict = self.download_data(all_keys)
        elink_records_dict = ELinkAdapter.list_to_dict(
            elink_records
        )  # osti_id -> elink_record

        # now i want to update my local doi collection
        try:
            self.sync_doi_collection_from_downloads(
                elink_records=elink_records, bibtex_dict=bibtex_dict
            )
        except Exception as e:
            self.has_error = True
            self.logger.error(f"Updating Local DOI failed. Error: {e}")

        # now i want to cross check against my other databases to see if doi collection is up-to-date
        # first get a list of keys in doi store
        all_keys = self.doi_store.distinct(
            criteria={self.doi_store.key: {"$in": all_keys}}, field=self.doi_store.key
        )
        # then get those keys from robo
        robos: List[RoboCrysModel] = [
            RoboCrysModel.parse_obj(robo)
            for robo in self.robocrys_store.query(
                criteria={self.robocrys_store.key: {"$in": all_keys}}
            )
        ]
        robos_dict: Dict[str, str] = {
            robo.material_id: robo.description for robo in robos
        }  # robo.material_id -> robo.description

        # then do a check if robo description == doi record description
        doi_records: List[DOIRecordModel] = [
            DOIRecordModel.parse_obj(doi_record)
            for doi_record in self.doi_store.query(
                criteria={self.doi_store.key: {"$in": all_keys}}
            )
        ]

        self.update_doi_collection(
            doi_records=doi_records,
            robos_dict=robos_dict,
            elink_records_dict=elink_records_dict,
        )
        self.logger.info("Sync Finished")

    def update_doi_collection(
        self,
        doi_records: List[DOIRecordModel],
        robos_dict: Dict[str, str],
        elink_records_dict: Dict[str, ELinkGetResponseModel],
    ) -> None:
        """
        Function that takes a list of doi records, its corresponding robos info, and the elink info,
        and make a decision of whether to mark a record as valid
        Args:
            doi_records: list of doi records to check
            robos_dict: a dictionary of corresponding new descriptions
            elink_records_dict: elink results

        Returns:
            None
        """
        self.logger.info("Updating DOI Collection")
        update_doi_record_count = 0
        for doi_record in tqdm(doi_records):
            doi_record.last_validated_on = datetime.now()
            doi_record_abstract = doi_record.get_bibtex_abstract()
            robo_description = robos_dict.get(doi_record.material_id, None)

            if robo_description is not None:
                robo_description = robo_description.replace("  ", " ")

            if (
                doi_record.status
                != DOIRecordStatusEnum[
                    elink_records_dict[doi_record.get_osti_id()].doi["@status"]
                ]
            ):
                doi_record.status = DOIRecordStatusEnum[
                    elink_records_dict[doi_record.get_osti_id()].doi["@status"]
                ]
                doi_record.last_updated = datetime.now()
                update_doi_record_count += 1

            if doi_record_abstract != robo_description:
                # mark this entry as needed to be updated
                self.logger.debug(
                    f"[{doi_record.material_id}]'s abstract needs to be updated"
                )
                doi_record.valid = False
                # doi_record.last_updated = datetime.now()
                # update_doi_record_count += 1
            elif (
                doi_record.valid is False
                and doi_record.status == DOIRecordStatusEnum.COMPLETED
            ):
                doi_record.valid = (
                    True
                )  # if the bibtex is updated, flip the valid flag back to true

        self.doi_store.update(
            docs=[doi_record.dict() for doi_record in doi_records],
            key=self.doi_store.key,
        )

        message = f"Validated [{len(doi_records)}] DOI Records"
        self.email_messages.append(message)
        self.logger.info(message)

    def download_data(self, keys: List[str]):
        elink_records = self.elink_adapter.get_multiple(mp_ids=keys, chunk_size=100)
        try:
            bibtex_dict = self.explorer_adapter.get_multiple_bibtex(
                osti_ids=[r.osti_id for r in elink_records], chunk_size=100
            )
        except HTTPError:
            bibtex_dict = dict()
        return elink_records, bibtex_dict

    def sync_doi_collection_from_downloads(
        self, elink_records: List[ELinkGetResponseModel], bibtex_dict: Dict[str, dict]
    ) -> None:
        """
        Construct DOI records and sync with local collection

        Args:
            elink_records: elink records
            bibtex_dict: bibtex records

        Returns:
            None
        """
        self.logger.info("Syncing DOI Collection from Downloads")
        curr_records: Dict[str, DOIRecordModel] = {
            DOIRecordModel.parse_obj(record).material_id: DOIRecordModel.parse_obj(
                record
            )
            for record in self.doi_store.query(
                criteria={
                    self.doi_store.key: {
                        "$in": [
                            elink_record.accession_num for elink_record in elink_records
                        ]
                    }
                }
            )
        }
        doi_records_from_download: List[DOIRecordModel] = []
        for elink_record in tqdm(elink_records):
            doi_record: DOIRecordModel = DOIRecordModel(
                material_id=elink_record.accession_num,
                doi=elink_record.doi["#text"],
                bibtex=None,
                status=elink_record.doi["@status"],
                valid=False,
                last_validated_on=datetime.now(),
                created_at=datetime.now()
                if elink_record.accession_num not in curr_records
                else curr_records[elink_record.accession_num].created_at,
                last_updated=datetime.now()
                if elink_record.accession_num not in curr_records
                else curr_records[elink_record.accession_num].last_updated,
            )
            bibtex_entry: dict = bibtex_dict.get(doi_record.get_osti_id(), None)
            doi_record.bibtex = (
                self.create_bibtex_string(bibtex_entry)
                if bibtex_entry is not None
                else None
            )
            doi_records_from_download.append(doi_record)

        self.doi_store.update(
            docs=[record.dict() for record in doi_records_from_download],
            key=self.doi_store.key,
        )
        message = f"Downloaded [{len(doi_records_from_download)}] records from Elink"
        self.email_messages.append(message)
        self.logger.info(message)

    def generate_elink_model(self, material: MaterialModel) -> ELinkGetResponseModel:
        """
        Generate ELink Get model by mp_id

        :param material: material of the Elink model trying to generate
        :return:
            instance of ELinkGetResponseModel
        """

        elink_record = ELinkGetResponseModel(
            osti_id=self.get_osti_id(mp_id=material.task_id),
            title=ELinkGetResponseModel.get_title(material=material),
            product_nos=material.task_id,
            accession_num=material.task_id,
            publication_date=material.last_updated.strftime("%m/%d/%Y"),
            site_url=ELinkGetResponseModel.get_site_url(mp_id=material.task_id),
            keywords=ELinkGetResponseModel.get_keywords(material=material),
            description=self.get_material_description(material.task_id),
        )
        return elink_record

    def create_bibtex_string(self, entry: dict):
        db = bibtexparser.bibdatabase.BibDatabase()
        db.entries = [entry]
        return bibtexparser.dumps(db) if entry is not None else None

    def get_material_description(self, mp_id: str) -> str:
        """
        find materials description from robocrys database, if not found return the default description

        :param mp_id: mp_id to query for in the robocrys database
        :return:
            description in string
        """
        description = RoboCrysModel.get_default_description()
        robo_result = self.robocrys_store.query_one(
            criteria={self.robocrys_store.key: mp_id}
        )
        if robo_result is None:
            return description
        else:
            robo_result = RoboCrysModel.parse_obj(robo_result)
            robo_description = robo_result.description
            if robo_description is None:
                return description
            return robo_description[
                :12000
            ]  # 12000 is the Elink Abstract character limit

    def get_doi(self, mp_id) -> str:
        osti_id = self.get_osti_id(mp_id=mp_id)
        if osti_id == "":
            return ""
        else:
            return "10.17188/" + osti_id

    def get_osti_id(self, mp_id) -> str:
        """
        Used to determine if an update is necessary.

        If '' is returned, implies update is not necessary.

        Otherwise, an update is necessary

        Args:
            mp_id: materials id

        Returns:
            OSTI ID in string
        """
        doi_entry = self.doi_store.query_one(criteria={self.doi_store.key: mp_id})
        if doi_entry is None:
            return ""
        else:
            return doi_entry["doi"].split("/")[-1]

    def as_dict(self) -> dict:
        return {
            "materials_collection": self.materials_store.as_dict(),
            "robocrys_collection": self.robocrys_store.as_dict(),
            "dois_collection": self.doi_store.as_dict(),
            "elink": self.elink.dict(),
            "explorer": self.explorer.dict(),
            # "elsevier": self.elsevier.dict(),
            "max_doi_requests": self.max_doi_requests,
            "sync": self.sync,
        }

    @classmethod
    def from_dict(cls, d: dict):
        assert (
            "materials_collection" in d
        ), "Error: materials_collection config not found"
        assert "dois_collection" in d, "Error: dois_collection config not found"
        assert "robocrys_collection" in d, "Error: robocrys_collection config not found"
        assert "max_doi_requests" in d, "Error: max_doi_requests config not found"
        assert "sync" in d, "Error: sync config not found"

        elink = ConnectionModel.parse_obj(d["elink"])
        explorer = ConnectionModel.parse_obj(d["explorer"])
        # elsevier = ConnectionModel.parse_obj(d["elsevier"])

        materials_store = json.loads(
            json.dumps(d["materials_collection"]), cls=MontyDecoder
        )
        robocrys_store = json.loads(
            json.dumps(d["robocrys_collection"]), cls=MontyDecoder
        )
        doi_store = json.loads(json.dumps(d["dois_collection"]), cls=MontyDecoder)
        report_emails = d["report_emails"]

        max_doi_requests = d["max_doi_requests"]
        sync = d["sync"]
        bld = DoiBuilder(
            materials_store=materials_store,
            robocrys_store=robocrys_store,
            doi_store=doi_store,
            elink=elink,
            explorer=explorer,
            max_doi_requests=max_doi_requests,
            sync=sync,
            report_emails=report_emails,
        )
        return bld

    def send_email(self):
        try:
            self.generate_report()
            self.logger.info(f"Sending Email to {self.report_emails}")
            fromaddr = "mpcite.debug@gmail.com"
            toaddr = ",".join(self.report_emails)
            msg = MIMEMultipart()  # instance of MIMEMultipart
            msg["From"] = fromaddr  # storing the senders email address
            msg["To"] = toaddr  # storing the receivers email address
            msg[
                "Subject"
            ] = f"MPCite Run data of {datetime.now()}"  # storing the subject
            body = ""  # string to store the body of the mail
            for m in self.email_messages:
                body = body + "\n" + m
            msg.attach(MIMEText(body, "plain"))  # attach the body with the msg instance
            filename = "Visualizations.pdf"  # open the file to be sent
            attachment = open("Visualizations.pdf", "rb")
            p = MIMEBase(
                "application", "octet-stream"
            )  # instance of MIMEBase and named as p
            p.set_payload(
                (attachment).read()
            )  # To change the payload into encoded form
            encoders.encode_base64(p)  # encode into base64
            p.add_header("Content-Disposition", "attachment; filename= %s" % filename)
            msg.attach(p)  # attach the instance 'p' to instance 'msg'
            s = smtplib.SMTP("smtp.gmail.com", 587)  # creates SMTP session
            s.starttls()  # start TLS for security
            s.login(fromaddr, "wuxiaohua1011")  # Authentication
            text = msg.as_string()  # Converts the Multipart msg into a string
            s.sendmail(fromaddr, toaddr, text)  # sending the mail
            s.quit()  # terminating the session
        except Exception as e:
            self.logger.error(f"Error sending email: {e}")

    def generate_report(self):
        self.logger.info("Generating Report")
        base = Path(__file__).parent
        notebook_file_path = base / "Visualizations.ipynb"
        nb = nbformat.read(notebook_file_path.open("r"), as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
        ep.preprocess(nb)
        pdf_exporter = PDFExporter()
        pdf_data, resources = pdf_exporter.from_notebook_node(nb)
        with open("Visualizations.pdf", "wb") as f:
            f.write(pdf_data)
            f.close()
