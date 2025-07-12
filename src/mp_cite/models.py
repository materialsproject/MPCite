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