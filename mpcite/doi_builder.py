from maggma.core.builder import Builder
from typing import Iterable, List
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
import datetime
from tqdm import tqdm
from maggma.stores import Store
from monty.json import MontyDecoder
import json
import bibtexparser
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from pathlib import Path
from difflib import SequenceMatcher
from typing import Optional, Dict, Tuple, Union


class DOIBuilder(Builder):
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
        super().__init__(
            sources=[materials_store, robocrys_store], targets=[doi_store], **kwargs
        )
        # set connections
        self.materials_store = materials_store
        self.robocrys_store = robocrys_store
        self.doi_store = doi_store
        # self.elsevier = elsevier
        self.elink = elink
        self.explorer = explorer
        self.elink_adapter = ELinkAdapter(elink)
        self.explorer_adapter = ExplorerAdapter(explorer)

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
        1. download and sync from elink
        2. yield items that are needs update
            1. prioritize items with valid = False, status = COMPLETE, those are the items that have not received an
               update yet
            2. send all other item with valid = False
        3. yield new items

        note: need to cap the maximum sendable materials to self.max_doi_requests
        Returns:
            Iterable DOIRecordModel
        """
        if self.sync:
            self.download_and_sync()
            self.log_info_msg("Data Synced")
        else:
            self.log_info_msg("Not Syncing in this run")
        curr_update_ids = set(
            self.doi_store.distinct(
                self.doi_store.key,
                criteria={
                    "$and": [
                        {"valid": False},
                        {"status": {"$eq": DOIRecordStatusEnum.COMPLETED.value}},
                    ]
                },
            )
        )
        self.log_info_msg(f"[{len(curr_update_ids)}] requires priority updates")
        if len(curr_update_ids) < self.max_doi_requests:
            # send all other data with valid = False
            curr_update_ids = curr_update_ids.union(
                set(
                    self.doi_store.distinct(
                        self.doi_store.key, criteria={"valid": False}
                    )
                )
            )
            self.log_info_msg(f"[{len(curr_update_ids)}] requires normal updates")
        new_materials_ids = set(
            self.materials_store.distinct(field=self.materials_store.key)
        ) - set(self.doi_store.distinct(field=self.doi_store.key))
        if len(curr_update_ids) < self.max_doi_requests:
            curr_update_ids = curr_update_ids.union(new_materials_ids)
            self.log_info_msg(f"[{len(new_materials_ids)}] requires new registration")

        curr_update_ids = list(curr_update_ids)[: self.max_doi_requests]
        self.log_info_msg(
            msg=f"Updating/registering items with mp_id \n{curr_update_ids}"
        )
        return curr_update_ids

    def process_item(self, item: str) -> Optional[Dict]:
        """
        Construct Elink Post Record model
        Args:
            item: mp_id of a material

        Returns:

        """
        elink_post_record = self.generate_elink_model(mp_id=item)
        return {"elink_post_record": elink_post_record}

    def update_targets(self, items: List):
        """
        Post to remote connections.
        First go through the list of items, extracting the item that you want to post
        Then feed that item into a function that does the POSTing
        Lastly, sync local DOI collection if necessary

        Args:
            items: list of items to post

        Returns:
            None
        """
        try:
            self.log_info_msg(f"POSTing [{len(items)}] records to Elink")
            elink_post_data: List[dict] = []
            for item in tqdm(items):
                if len(item) == 0:
                    continue
                if item.get("elink_post_record", None) is not None:
                    elink_post_data.append(
                        ELinkGetResponseModel.custom_to_dict(
                            elink_record=item["elink_post_record"]
                        )
                    )
            self.post_to_elink(elink_post_data=elink_post_data)
        except Exception as e:
            self.has_error = True
            self.log_err_msg(msg=f"Failed to POST. No updates done. Error: \n{e}")

    def finalize(self):
        self.log_info_msg(f"DOI store now has {self.doi_store.count()} records")
        self.send_email()
        super(DOIBuilder, self).finalize()

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
        bld = DOIBuilder(
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

    def download_and_sync(self):
        try:
            self.log_info_msg("Start Syncing. This will take long")
            all_keys = self.materials_store.distinct(
                field=self.materials_store.key
            )  # this might fail in the future

            self.log_info_msg(f"[{len(all_keys)}] requires syncing")
            elink_dict, bibtex_dict = self.download_data(all_keys)
            self.sync_local_doi_collection(elink_dict, bibtex_dict)
            self.sync_robocrystal(elink_dict)
            self.log_info_msg("Sync Successfull")
        except Exception as e:
            self.log_err_msg(f"Something Failed: {e}")

    def log_info_msg(self, msg):
        self.logger.info(msg)
        self.email_messages.append(msg)

    def log_err_msg(self, msg):
        self.logger.error(msg)
        self.email_messages.append(msg)

    def download_data(
        self, keys: List[str]
    ) -> Tuple[Dict[str, ELinkGetResponseModel], Dict[str, dict]]:
        """
        Download data from elink and explorer given a set of accession numbers
        Args:
            keys: accession numbers

        Returns:
            Elink and bibtex record in mp_id -> record dictionary format
        """
        elink_records = self.elink_adapter.get_multiple(mp_ids=keys, chunk_size=100)
        elink_records_dict = ELinkAdapter.list_to_dict(
            elink_records
        )  # mp_id -> elink_record
        try:
            bibtex_dict_raw = self.explorer_adapter.get_multiple_bibtex(
                osti_ids=[r.osti_id for r in elink_records], chunk_size=100
            )
            bibtex_dict = dict()
            for elink in elink_records_dict.values():
                if elink.osti_id in bibtex_dict_raw:
                    bibtex_dict[elink.accession_num] = bibtex_dict_raw[elink.osti_id]
        except HTTPError:
            bibtex_dict = dict()

        return elink_records_dict, bibtex_dict

    def sync_local_doi_collection(
        self, elink_dict: Dict[str, ELinkGetResponseModel], bibtex_dict: Dict[str, dict]
    ):
        """
        Given Elink data and explorer, sync local DOI collection by overwriting.
        Args:
            elink_dict: data from elink
            bibtex_dict: data from explorer

        Returns:
            None
        """
        self.log_info_msg("Syncing DOI collection using data from elink")
        doi_records: Dict[str, DOIRecordModel] = {
            DOIRecordModel.parse_obj(record).material_id: DOIRecordModel.parse_obj(
                record
            )
            for record in self.doi_store.query(
                criteria={self.doi_store.key: {"$in": list(elink_dict.keys())}}
            )
        }
        for mp_id, elink in tqdm(elink_dict.items()):
            doi_record: DOIRecordModel = DOIRecordModel(
                material_id=mp_id,
                doi=elink.doi["#text"],
                bibtex=None,
                status=elink.doi["@status"],
                valid=False,
                last_validated_on=datetime.datetime.now(),
                created_at=datetime.datetime.now()
                if mp_id not in doi_records
                else doi_records[mp_id].created_at,
                last_updated=datetime.datetime.now()
                if mp_id not in doi_records
                else doi_records[mp_id].last_updated,
            )
            bibtex_entry: dict = bibtex_dict.get(doi_record.material_id, None)
            doi_record.bibtex = (
                self._create_bibtex_string(bibtex_entry)
                if bibtex_entry is not None
                else None
            )
            doi_records[mp_id] = doi_record
        self.log_info_msg("Updating Local DOI Collection. Please wait. ")
        self.doi_store.update(
            key=self.doi_store.key,
            docs=[record.dict() for record in doi_records.values()],
        )
        self.log_info_msg(
            f"Downloaded & Synced [{len(doi_records)}] records from elink"
        )

    @staticmethod
    def _create_bibtex_string(entry: dict):
        db = bibtexparser.bibdatabase.BibDatabase()
        db.entries = [entry]
        return bibtexparser.dumps(db) if entry is not None else None

    def sync_robocrystal(self, elink_dict: Dict[str, ELinkGetResponseModel]):
        """
        This function is meant to be called AFTER sync_local_doi_collection.
        It will take the robocrystal descryption and add it to the local DOI Collection.

        Furthermore, this function will flip the VALID flag iff robo = curr DOI description
        Args:
            elink_dict: elink records, this is to make things faster, since I only need to update the
            ones that elink already have.

        Returns:
            None

        """
        self.log_info_msg("Syncing Robo Crystal Description")
        all_keys = list(elink_dict.keys())
        robos: Dict[str, RoboCrysModel] = {
            RoboCrysModel.parse_obj(robo).material_id: RoboCrysModel.parse_obj(robo)
            for robo in self.robocrys_store.query(
                criteria={self.robocrys_store.key: {"$in": all_keys}}
            )
        }
        doi_records: Dict[str, DOIRecordModel] = {
            DOIRecordModel.parse_obj(record).material_id: DOIRecordModel.parse_obj(
                record
            )
            for record in self.doi_store.query(
                criteria={self.doi_store.key: {"$in": list(elink_dict.keys())}}
            )
        }

        def set_doi_status_helper(record: DOIRecordModel):
            if record.status == DOIRecordStatusEnum.COMPLETED.value:
                record.valid = True
            else:
                record.valid = False

        for mpid, doi_record in tqdm(doi_records.items()):
            try:
                doi_record_abstract = doi_record.get_bibtex_abstract()
                robo: Union[RoboCrysModel, str] = robos.get(doi_record.material_id, "")
                doi_record_abstract = (
                    "" if doi_record_abstract is None else doi_record_abstract
                )

                if type(robo) == str and robo == "":
                    set_doi_status_helper(doi_record)
                elif type(robo) == RoboCrysModel and robo.description is None:
                    set_doi_status_helper(doi_record)
                else:
                    if (
                        doi_record_abstract == ""
                        or SequenceMatcher(
                            a=robo.description[:200], b=doi_record_abstract[:200]
                        ).ratio()
                        < 0.8
                    ):
                        # mark this entry as needed to be updated
                        self.logger.debug(
                            f"[{doi_record.material_id}]'s abstract needs to be updated"
                        )
                        doi_record.valid = False
                    else:
                        set_doi_status_helper(doi_record)

            except Exception as e:
                self.log_err_msg(
                    f"Skipping {mpid}.because something bad happened: {e} "
                )
        self.log_info_msg("Updating Local DOI Collection. Please wait. ")
        self.doi_store.update(
            key=self.doi_store.key,
            docs=[doi_record.dict() for doi_record in doi_records.values()],
        )
        self.log_info_msg("Robo Crystal updated")

    def generate_elink_model(self, mp_id: str) -> ELinkGetResponseModel:
        """
        Generate ELink Get model by mp_id

        :param mp_id: material of the Elink model trying to generate
        :return:
            instance of ELinkGetResponseModel
        """
        material = MaterialModel.parse_obj(
            self.materials_store.query_one(criteria={self.materials_store.key: mp_id})
        )
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

    def post_to_elink(self, elink_post_data: List[dict]):
        data: bytes = ELinkAdapter.prep_posting_data(elink_post_data)
        elink_post_responses: List[ELinkPostResponseModel] = self.elink_adapter.post(
            data=data
        )
        self.logger.info(f"Processing {len(elink_post_responses)} Elink Responses")
        records: Dict[str, DOIRecordModel] = {
            DOIRecordModel.parse_obj(record).material_id: DOIRecordModel.parse_obj(
                record
            )
            for record in self.doi_store.query(
                criteria={
                    self.doi_store.key: {
                        "$in": [e_p.accession_num for e_p in elink_post_responses]
                    }
                }
            )
        }
        for e_p in tqdm(elink_post_responses):
            record: DOIRecordModel = records[e_p.accession_num]
            record.doi = e_p.doi["#text"]
            record.status = DOIRecordStatusEnum[e_p.doi["@status"]].value
            record.valid = (
                True if record.status == DOIRecordStatusEnum.COMPLETED else False
            )
            record.last_validated_on = datetime.datetime.now()
            record.last_updated = datetime.datetime.now()
            record.error = (
                f"Unkonwn error happend when pushing to ELINK. "
                f"Material ID = {record.material_id} | DOi = {record.doi}"
                if record.status == DOIRecordStatusEnum.FAILURE
                else None
            )
        self.log_info_msg("Updating Local DOI Collection. Please wait. ")
        self.doi_store.update(
            key=self.doi_store.key, docs=[record.dict() for record in records.values()]
        )

    def send_email(self):
        try:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText

            self.generate_report()
            self.log_info_msg(f"Sending Email to {self.report_emails}")
            fromaddr = "mpcite.debug@gmail.com"
            toaddr = ",".join(self.report_emails)
            msg = MIMEMultipart()  # instance of MIMEMultipart
            msg["From"] = fromaddr  # storing the senders email address
            msg["To"] = toaddr  # storing the receivers email address
            msg[
                "Subject"
            ] = f"MPCite Run data of {datetime.datetime.now()}"  # storing the subject
            body = (
                "" if len(self.email_messages) > 0 else "This run did not do anything"
            )
            num_valids = self.doi_store.count(criteria={"valid": True})
            self.email_messages.append(f"Number of Valid Records: [{num_valids}]")
            self.email_messages.append(
                "View Visualizations at https://dois.materialsproject.org/"
            )
            for m in self.email_messages:
                body = body + "\n" + m
            body = MIMEText(body)
            msg.attach(body)
            s = smtplib.SMTP("smtp.gmail.com", 587)  # creates SMTP session
            s.starttls()  # start TLS for security
            s.login(fromaddr, "wuxiaohua1011")  # Authentication
            text = msg.as_string()  # Converts the Multipart msg into a string
            s.sendmail(fromaddr, toaddr, text)  # sending the mail
            s.quit()  # terminating the session
        except Exception as e:
            self.log_err_msg(f"Error sending email: {e}")

    def generate_report(self):
        from nbconvert import HTMLExporter

        self.log_info_msg("Generating Report")
        base = Path(__file__).parent
        notebook_file_path = base / "Visualizations.ipynb"
        nb = nbformat.read(notebook_file_path.open("r"), as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
        ep.preprocess(nb)
        html_exporter = HTMLExporter()
        html_data, resources = html_exporter.from_notebook_node(nb)
        path = "/var/www/dois/index.html"
        try:
            with open(path, "wb") as f:
                f.write(html_data.encode("utf8"))
                f.close()
        except Exception as e:
            self.log_err_msg(f"Cannot write to [{path}]: {e}")
        with open("Visualizations.html", "wb") as f:
            f.write(html_data.encode("utf8"))
            f.close()
