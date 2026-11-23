from datetime import datetime, timezone

from elinkapi import Identifier, Organization, Person, Record
from pydantic import Field

MP_SPONSOR_NAME = "USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)"
"""Canonical E-Link ``name`` of the Materials Project core funding sponsor."""

MP_SPONSOR_IDENTIFIERS = [
    Identifier(type="CN_DOE", value="AC02-05CH11231"),
    Identifier(type="CN_NONDOE", value="EDCBEE"),
]
"""Identifiers E-Link requires on the MP core sponsor (the DOE contract number and the
accompanying non-DOE contract number)."""


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

    Attributes:
        persons: AUTHORs in citation order, plus the corresponding author as
            ``Person(type="CONTACT", email=[...])``.
        organizations: Funders as ``type="SPONSOR"``, where the organization ``name``
            is the exact DOE program office (for DOE funders) and each award number is
            an ``Identifier`` of type ``CN_DOE`` (DOE) or ``CN_NONDOE`` (non-DOE).
        description: The MPContribs project description.
        keywords: Optional. Keywords associated with this DOI.
        related_identifiers: Optional. DOIs of publications the data supports, recorded
            as ``type="DOI", relation="IsSupplementTo"``.
    """

    product_type: str = Field(default="DA")
    # --- Required ---
    title: str                                  # MPContribs project title
    site_url: str                               # per-contribution landing page
    persons: list[Person]                       # AUTHORs + CONTACT corresponding author
    organizations: list[Organization]           # funders as SPONSOR orgs
    description: str                            # MPContribs project description
    doe_funded_flag: str = Field(default="N")   # Bool-like, but ELink is a `Y`/`N` str
    access_limitations: list[str] = Field(default_factory=lambda: ["UNL"])
    publication_date: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
