from pydantic import BaseModel, Field
from datetime import datetime


class DOIModel(BaseModel):
    # identifiers
    doi: str = Field(
        description="The DOI number as allocated by OSTI"
    )  # can be taken from ELink API
    title: str = Field(
        description="The title of the record"
    )  # can be taken from ELink API
    osti_id: str = Field(
        coerce_numbers_to_str=True,
        description="The OSTI ID number allocated by OSTI to make the DOI number",
    )  # can be taken from ELink API
    material_id: str  # can be taken from Robocrys Collection or ELink API

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


# hypothetically post an update or submit a new record and receive the RecordResponse
def record_response_to_DOI_model(recordresponse):
    """
    turns a recordresponse, which is returned from a save, submission, post, etc. into a doi_model object
    """
    params = {
        "doi": recordresponse.doi,
        "title": recordresponse.title,
        "osti_id": str(recordresponse.osti_id),
        "material_id": recordresponse.site_unique_id,
        "date_metadata_added": recordresponse.date_metadata_added,
        "date_metadata_updated": recordresponse.date_metadata_updated,
        "workflow_status": recordresponse.workflow_status,
        "date_released": recordresponse.date_released,
        "date_submitted_to_osti_first": recordresponse.date_submitted_to_osti_first,  # date record was first submitted to OSTI for publication, maintained internally by E-Link
        "date_submitted_to_osti_last": recordresponse.date_submitted_to_osti_last,  # most recent date record information was submitted to OSTI. Maintained internally by E-Link.
        "publication_date": recordresponse.publication_date,
    }

    return DOIModel(**params)
