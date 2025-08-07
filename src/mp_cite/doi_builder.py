from pydantic import BaseModel, Field, model_validator
from typing import Any
from datetime import datetime


class DOIModel(BaseModel):
    # identifiers
    doi: str = Field(
        description="The DOI number as allocated by OSTI"
    )  # can be taken from ELink API
    title: str = Field(
        description="The title of the record"
    )  # can be taken from ELink API
    osti_id: int = Field(
        coerce_numbers_to_str=True,
        description="The OSTI ID number allocated by OSTI to make the DOI number",
    )  # can be taken from ELink API
    material_id: str  # can be taken from Robocrys Collection or ELink API
    site_unique_id: str  # added for compatability with E-Link's record model but in practice will match material_id

    # time stamps
    date_metadata_added: datetime | None = Field(
        description="date_record_entered_onto_ELink"
    )  # can be taken from ELink API response
    date_metadata_updated: datetime | None = Field(
        description="date_record_last_updated_on_Elink"
    )

    # status
    workflow_status: str  # can be taken from ELink API
    date_released: datetime | None = Field(description="")
    date_submitted_to_osti_first: datetime = Field(
        description="date record was first submitted to OSTI for publication, maintained internally by E-Link"
    )
    date_submitted_to_osti_last: datetime = Field(
        description="most recent date record information was submitted to OSTI. Maintained internally by E-Link"
    )
    publication_date: datetime | None = Field(
        description=""
    )  # labelled as publication_date in RecordResponse of ELink API

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
