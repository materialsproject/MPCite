from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from datetime import datetime
from enum import Enum
import bibtexparser


class ConnectionModel(BaseModel):
    endpoint: str = Field(..., title="URL Endpoint of the connection")
    username: str = Field(..., title="User Name")
    password: str = Field(..., title="Password")


class RoboCrysModel(BaseModel):
    material_id: str
    last_updated: datetime
    description: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def get_default_description(cls):
        return (
            "Computed materials data using density "
            "functional theory calculations. These calculations determine "
            "the electronic structure of bulk materials by solving "
            "approximations to the Schrodinger equation. For more "
            "information, see https://materialsproject.org/docs/calculations"
        )


class MaterialModel(BaseModel):
    last_updated: datetime = Field(
        None, title="timestamp for the most recent calculation"
    )
    updated_at: datetime = Field(None, title="alternative to last_updated")
    created_at: datetime = Field(
        None,
        description="creation time for this material defined by when the first structure "
        "optimization calculation was run",
    )
    task_id: str = Field(
        "", title="task id for this material. Also called the material id"
    )
    # pretty_formula: str = Field(..., title="clean representation of the formula")
    pretty_formula: str = Field(..., title="clean representation of the formula")
    chemsys: str


class ELinkGetResponseModel(BaseModel):
    osti_id: Optional[str] = Field(...)
    dataset_type: str = Field(default="SM")
    title: str = Field(...)
    creators: str = Field(default="Kristin Persson")  # replace with authors
    contributors: List[Dict[str, str]] = Field(
        default=[{"first_name": "Materials", "last_name": "Project"}],
        description="List of Dict of first name, last name mapping",
    )  # no contributor
    product_nos: str = Field(..., title="MP id")
    accession_num: str = Field(..., title="MP id")
    contract_nos: str = Field("AC02-05CH11231; EDCBEE")
    originating_research_org: str = Field(
        default="Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)"
    )
    publication_date: str = Field(...)
    language: str = Field(default="English")
    country: str = Field(default="US")
    sponsor_org: str = Field(
        default="USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)"
    )
    site_url: str = Field(...)
    contact_name: str = Field(default="Kristin Persson")
    contact_org: str = Field(default="LBNL")
    contact_email: str = Field(default="feedback@materialsproject.org")
    contact_phone: str = Field(default="+1(510)486-7218")
    related_resource: str = Field("https://materialsproject.org/citing")
    contributor_organizations: str = Field(default="MIT; UC Berkeley; Duke; U Louvain")
    subject_categories_code: str = Field(default="36 MATERIALS SCIENCE")
    keywords: str = Field(...)
    description: str = Field(default="")
    doi: dict = Field(
        {}, title="DOI info", description="Mainly used during GET request"
    )

    @classmethod
    def get_title(cls, material: MaterialModel):
        formula = material.pretty_formula
        return "Materials Data on %s by Materials Project" % formula

    @classmethod
    def get_site_url(cls, mp_id):
        return "https://materialsproject.org/materials/%s" % mp_id

    @classmethod
    def get_keywords(cls, material):
        keywords = "; ".join(
            ["crystal structure", material.pretty_formula, material.chemsys]
        )
        return keywords

    @classmethod
    def get_default_description(cls):
        return (
            "Computed materials data using density "
            "functional theory calculations. These calculations determine "
            "the electronic structure of bulk materials by solving "
            "approximations to the Schrodinger equation. For more "
            "information, see https://materialsproject.org/docs/calculations"
        )

    @classmethod
    def custom_to_dict(cls, elink_record) -> dict:
        if elink_record.osti_id is None or elink_record.osti_id == "":
            return elink_record.dict(exclude={"osti_id", "doi"})
        else:
            return elink_record.dict(exclude={"doi"})


class ElinkResponseStatusEnum(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILURE"


class ELinkPostResponseModel(BaseModel):
    osti_id: str
    accession_num: str
    product_nos: str
    title: str
    contract_nos: str
    other_identifying_nos: Optional[str]
    doi: Dict[str, str]
    status: ElinkResponseStatusEnum
    status_message: Optional[str]

    def generate_doi_record(self):
        doi_collection_record = DOIRecordModel(
            material_id=self.accession_num,
            doi=self.doi["#text"],
            status=self.doi["@status"],
            bibtex=None,
            valid=True,
            last_validated_on=datetime.now(),
        )
        doi_collection_record.set_status(status=self.doi["@status"])
        doi_collection_record.last_validated_on = datetime.now()
        return doi_collection_record


class DOIRecordStatusEnum(str, Enum):
    COMPLETED = "COMPLETED"
    PENDING = "PENDING"
    FAILURE = "FAILURE"
    INIT = "INIT"


class DOIRecordModel(BaseModel):
    material_id: str = Field(...)
    doi: str = Field(default="")
    bibtex: Optional[str] = None
    status: DOIRecordStatusEnum
    valid: bool = Field(False)
    last_updated: datetime = Field(
        default=datetime.now(),
        title="DOI last updated time.",
        description="Last updated is defined as either a Bibtex or status change.",
    )
    created_at: datetime = Field(
        default=datetime.now(),
        title="DOI Created At",
        description="creation time for this DOI record",
    )
    last_validated_on: datetime = Field(
        default=datetime.now(),
        title="Date Last Validated",
        description="Date that this data is last validated, " "not necessarily updated",
    )
    elsevier_updated_on: datetime = Field(
        default=datetime.now(),
        title="Date Elsevier is updated",
        description="If None, means never uploaded to elsevier",
    )
    error: Optional[str] = Field(
        default=None, description="None if no error, else error message"
    )

    class Config:
        use_enum_values = True

    def set_status(self, status):
        self.status = status

    def get_osti_id(self):
        if self.doi is None or self.doi == "":
            return ""
        else:
            return self.doi.split("/")[-1]

    def get_bibtex_abstract(self):
        try:
            if self.bibtex is None:
                return ""
            bib_db: bibtexparser.bibdatabase.BibDatabase = bibtexparser.loads(
                self.bibtex
            )
            if bib_db.entries:
                return bib_db.entries[0]["abstractnote"]
        except Exception as e:
            print(e)
            return ""


class OSTIDOIRecordModel(DOIRecordModel):
    material_id: str = Field(...)
    doi: str = Field(default="")
    bibtex: Optional[str] = None
    valid: bool = Field(False)
    last_updated: datetime = Field(
        default=datetime.now(),
        title="DOI last updated time.",
        description="Last updated is defined as either a Bibtex or status change.",
    )


class ElsevierPOSTContainerModel(BaseModel):
    identifier: str = Field(default="", title="mp_id")
    source: str = "MATERIALS_PROJECT"
    date: str = datetime.now().date().isoformat().__str__()
    title: str
    description: str = ""
    doi: str
    authors: List[str] = ["Kristin Persson"]
    url: str
    type: str = "dataset"
    dateAvailable: str = datetime.now().date().isoformat().__str__()
    dateCreated: str = datetime.now().date().isoformat().__str__()
    version: str = "1.0.0"
    funding: str = "USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)"
    language: str = "en"
    method: str = "Materials Project"
    accessRights: str = "Public"
    contact: str = "Kristin Persson <kapersson@lbl.gov>"
    dataStandard: str = "https://materialsproject.org/citing"
    howToCite: str = "https://materialsproject.org/citing"
    subjectAreas: List[str] = ["36 MATERIALS SCIENCE"]
    keywords: List[str]
    institutions: List[str] = ["Lawrence Berkeley National Laboratory"]
    institutionIds: List[str] = ["AC02-05CH11231; EDCBEE"]
    spatialCoverage: List[str] = []
    temporalCoverage: List[str] = []
    references: List[str] = ["https://materialsproject.org/citing"]
    relatedResources: List[str] = ["https://materialsproject.org/citing"]
    location: str = "1 Cyclotron Rd, Berkeley, CA 94720"
    childContainerIds: List[str] = []

    @classmethod
    def get_url(cls, mp_id):
        return "https://materialsproject.org/materials/%s" % mp_id

    @classmethod
    def get_keywords(cls, material: MaterialModel):
        return ["crystal structure", material.pretty_formula, material.chemsys]

    @classmethod
    def get_default_description(cls):
        return (
            "Computed materials data using density "
            "functional theory calculations. These calculations determine "
            "the electronic structure of bulk materials by solving "
            "approximations to the Schrodinger equation. For more "
            "information, see https://materialsproject.org/docs/calculations"
        )

    @classmethod
    def get_date_created(cls, material: MaterialModel) -> str:
        return material.created_at.date().__str__()

    @classmethod
    def get_date_available(cls, material: MaterialModel) -> str:
        return material.created_at.date().__str__()

    @classmethod
    def get_title(cls, material: MaterialModel) -> str:
        return material.pretty_formula

    @classmethod
    def from_material_model(cls, material: MaterialModel, doi: str, description: str):
        model = ElsevierPOSTContainerModel(
            identifier=material.task_id,
            title=material.pretty_formula,
            doi=doi,
            url="https://materialsproject.org/materials/%s" % material.task_id,
            keywords=["crystal structure", material.pretty_formula, material.chemsys],
            date=datetime.now().date().__str__(),
            dateCreated=material.created_at.date().__str__(),
            dateAvailable=ElsevierPOSTContainerModel.get_date_available(material),
            description=description,
        )
        return model


class ExplorerGetJSONResponseModel(BaseModel):
    osti_id: str
    title: str
    report_number: str
    doi: str
    product_type: str
    language: str
    country_publication: str
    description: str
    site_ownership_code: str
    publication_date: str
    entry_date: str
    contributing_organizations: str
    authors: List[str]
    subjects: List[str]
    contributing_org: str
    doe_contract_number: str
    sponsor_orgs: List[str]
    research_orgs: List[str]
    links: List[Dict[str, str]]
