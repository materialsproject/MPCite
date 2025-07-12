<<<<<<< HEAD
from pydantic import BaseModel, Field, model_validator

from datetime import datetime
from elinkapi import Record, Organization, Person

from typing import List, Any
import pytz


class DOIModel(BaseModel):
    """
    The model for a DOI document in a mongodb collection, which should better align with E-Link's record model.

    It is designed for easy transfer from E-Link record response to doi document. All fields can be mapped directly from a
    record response keywords of the same name, or, in the case of material_id, it is automatically filled in with site_unique_id
    with the model validator `set_material_id(...)`
    """

    # identifiers
    doi: str = Field(description="The DOI number as allocated by OSTI")
    title: str = Field(description="The title of the record")
    osti_id: int = Field(
        coerce_numbers_to_str=True,
        description="The OSTI ID number allocated by OSTI to make the DOI number",
    )
    material_id: str
    site_unique_id: str

    # time stamps
    date_metadata_added: datetime | None = Field(
        description="date_record_entered_onto_ELink"
    )
    date_metadata_updated: datetime | None = Field(
        description="date_record_last_updated_on_Elink"
    )

    # status
    workflow_status: str
    date_released: datetime | None = Field(description="")
    date_submitted_to_osti_first: datetime = Field(
        description="date record was first submitted to OSTI for publication, maintained internally by E-Link"
    )
    date_submitted_to_osti_last: datetime = Field(
        description="most recent date record information was submitted to OSTI. Maintained internally by E-Link"
    )
    publication_date: datetime | None = Field(description="")

    @model_validator(mode="before")
    def set_material_id(cls, values: dict[str, Any]):
        """
        set_material_id will take the values passed into the model constructor before full instantiation of the object and pydantic parcing
        and make it that the whatever is passed in for the unique_site_id will match whatever is passed in for material_id

        :cls to designate it as a class method
        :values are the values passed into the constructor (contain the "raw input")

        returns the values so that instantiation can proceed.
        """
        values["material_id"] = values["site_unique_id"]
        return values


class MinimumDARecord(Record):
    product_type: str = Field(default="DA")
    title: str  # Required
    organizations: List[Organization] = Field(
        default_factory=lambda: [
            Organization(type="RESEARCHING", name="LBNL Materials Project (LBNL-MP)"),
            Organization(
                type="SPONSOR",
                name="TEST SPONSOR ORG",
                identifiers=[{"type": "CN_DOE", "value": "AC02-05CH11231"}],
            ),  # sponsor org is necessary for submission
        ]
    )
    persons: List[Person] = Field(
        default_factory=lambda: [Person(type="AUTHOR", last_name="Persson")]
    )
    site_ownership_code: str = Field(default="LBNL-MP")
    access_limitations: List[str] = Field(default_factory=lambda: ["UNL"])
    publication_date: datetime = Field(
        default_factory=lambda: datetime.now(tz=pytz.UTC)
    )
    site_url: str = Field(default="https://next-gen.materialsproject.org/materials")
=======
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Optional
import datetime
from enum import Enum
import bibtexparser
from elinkapi import Elink, Record
from elinkapi.record import RecordResponse, AccessLimitation, JournalType
from elinkapi.geolocation import Geolocation
from elinkapi.identifier import Identifier
from elinkapi.related_identifier import RelatedIdentifier
from elinkapi.person import Person
from elinkapi.organization import Organization

class TestClass(RecordResponse):
    ...
    # stuff

class ELinkGetResponseModel(BaseModel):
    osti_id: Optional[int] = Field(...)
    dataset_type: str = Field(default="SM")
    title: str = Field(...)
    persons: List[Person]
    contributors: List[Dict[str, str]] = Field(
        default=[{"first_name": "Materials", "last_name": "Project"}],
        description="List of Dict of first name, last name mapping",
    )  # no contributor
    publication_date: datetime.date
    site_url: str = Field(...)
    doi: dict = Field(
        {}, title="DOI info", description="Mainly used during GET request"
    )
    mp_id: str | None = None
    keywords: List[str] = None

    @classmethod
    def from_elinkapi_record(cls, R):
        gotResponse = ELinkGetResponseModel(
            osti_id = R.osti_id,
            title = R.title,
            persons = R.persons,
            # assume default contributors for now, creators vs contributors?
            publication_date = R.publication_date,
            site_url = R.site_url,
            doi = {"doi": R.doi},
            mp_id = next((id.value for id in R.identifiers if id.type == 'RN'), None),
            keywords = R.keywords
        )

        return gotResponse

    def get_title(self):
        formula = self.keywords[1]
        return "Materials Data on %s by Materials Project" % formula

    def get_site_url(self):
        return "https://materialsproject.org/materials/%s" % self.mp_id

    def get_keywords(self):
        # keywords = "; ".join(
        #     ["crystal structure", material.pretty_formula, material.chemsys]
        # )
        return self.keywords

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
>>>>>>> 5fa46e4 (Merged upstream (#1))
