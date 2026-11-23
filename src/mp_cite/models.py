from datetime import datetime, timezone
from typing import Any

from elinkapi import Identifier, Organization, Person, Record
from pydantic import BaseModel, Field, model_validator

MP_SPONSOR_NAME = "USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)"
"""Canonical E-Link ``name`` of the Materials Project core funding sponsor."""

MP_SPONSOR_IDENTIFIERS = [
    Identifier(type="CN_DOE", value="AC02-05CH11231"),
    Identifier(type="CN_NONDOE", value="EDCBEE"),
]
"""Identifiers E-Link requires on the MP core sponsor (the DOE contract number and the
accompanying non-DOE contract number)."""


class DOIModel(BaseModel):
    """Stored DOI document for a MongoDB collection, aligned with E-Link's record model.

    Designed for easy transfer from an E-Link record response to a DOI document: every
    field maps directly from a record-response keyword of the same name, except
    ``material_id``, which is filled in from ``site_unique_id`` by the
    :meth:`set_material_id` model validator.
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
        """Populate ``material_id`` from ``site_unique_id`` before validation.

        Runs in ``mode="before"`` so the raw input passed to the constructor has its
        ``material_id`` set to the given ``site_unique_id`` prior to pydantic parsing.

        Args:
            values: The raw values passed into the model constructor.

        Returns:
            The (mutated) values, so instantiation can proceed.
        """
        values["material_id"] = values["site_unique_id"]
        return values


class MinimumDARecord(Record):
    """Submission template for a core, MP-funded dataset (DA) DOI.

    Supplies the Materials Project defaults required for submission (the LBNL-MP
    researching and DOE sponsor organizations, the Persson author, site ownership, and
    the core landing page), leaving only ``title`` to be provided by the caller. Field
    defaults are documented on their respective :class:`~pydantic.Field` definitions.
    """

    product_type: str = Field(default="DA")
    title: str  # Required
    organizations: list[Organization] = Field(
        default_factory=lambda: [
            Organization(type="RESEARCHING", name="LBNL Materials Project (LBNL-MP)"),
            Organization(
                type="SPONSOR",
                name=MP_SPONSOR_NAME,
                identifiers=[i.model_copy(deep=True) for i in MP_SPONSOR_IDENTIFIERS],
            ),  # sponsor org is necessary for submission
        ]
    )
    persons: list[Person] = Field(
        default_factory=lambda: [Person(type="AUTHOR", last_name="Persson")]
    )
    site_ownership_code: str = Field(default="LBNL-MP")
    access_limitations: list[str] = Field(default_factory=lambda: ["UNL"])
    publication_date: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    site_url: str = Field(default="https://next-gen.materialsproject.org/materials")


class ContribsDoiRequest(Record):
    """Submission template for an independent (non-MP-funded) MPContribs dataset DOI.

    The attributes below are the MP-required attributes the caller must supply (in
    addition to ``title`` and ``site_url``); remaining field defaults are documented on
    their respective :class:`~pydantic.Field` definitions.

    Attributes:
        persons: AUTHORs in citation order, plus the corresponding author as
            ``Person(type="CONTACT", email=[...])``.
        organizations: Funders as ``type="SPONSOR"``, where the organization ``name``
            is the exact DOE program office (for DOE funders) and each award number is
            an ``Identifier`` of type ``CN_DOE`` (DOE) or ``CN_NONDOE`` (non-DOE).
        keywords: Keywords associated with this DOI.
        related_identifiers: DOIs of publications the data supports, recorded as
            ``type="DOI", relation="IsSupplementTo"``.
        description: The MPContribs project description.
    """

    product_type: str = Field(default="DA")
    title: str  # Required --- MPContribs project title
    site_url: str  # Required --- per-contribution landing page
    access_limitations: list[str] = Field(default_factory=lambda: ["UNL"])
    doe_funded_flag: str = Field(default="N")  # Bool-like, but ELink is a `Y`/`N` str
    publication_date: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


class ContribsDoiModel(BaseModel):
    """Stored document for a contributed-dataset DOI, in its own MongoDB collection.

    Held separately from the core :class:`DOIModel`. Fields map directly from an E-Link
    record response of the same name, and are documented on their respective
    :class:`~pydantic.Field` definitions.
    """

    # identifiers
    doi: str = Field(description="The DOI number as allocated by OSTI")
    title: str = Field(description="The title of the record")
    osti_id: int = Field(
        coerce_numbers_to_str=True,
        description="The OSTI ID number allocated by OSTI to make the DOI number",
    )
    site_unique_id: str = Field(description="The project's unique identifier")
    site_url: str | None = Field(
        default=None, description="The project's landing-page URL"
    )

    # time stamps
    date_metadata_added: datetime | None = Field(
        default=None, description="date_record_entered_onto_ELink"
    )
    date_metadata_updated: datetime | None = Field(
        default=None, description="date_record_last_updated_on_Elink"
    )

    # status
    date_released: datetime | None = Field(default=None, description="")
    date_submitted_to_osti_first: datetime = Field(
        description="date record was first submitted to OSTI for publication, maintained internally by E-Link"
    )
    date_submitted_to_osti_last: datetime = Field(
        description="most recent date record information was submitted to OSTI. Maintained internally by E-Link"
    )
    publication_date: datetime | None = Field(default=None, description="")
