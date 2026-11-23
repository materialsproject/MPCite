import re
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from elinkapi import Identifier, Organization, Person, Record
from pydantic import Field, field_validator

CORE_SUBMISSION_PREFIX = "core-sub:"
CONTRIBS_PREFIX = "contribs:"

# A bare (un-prefixed) material id, e.g. ``mp-1234`` or ``mvc-12``.
MATERIAL_ID_PATTERN = re.compile(r"^(?:mp|mvc)-\d+$")

_CONTRIBS_PROJECT_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

Namespace = Literal["core-sub", "contribs", "material"]
_ALL_NAMESPACES: frozenset[Namespace] = frozenset(("core-sub", "contribs", "material"))


# Used by outside callers for consistent formatting of id
def core_submission_site_id(submission_uuid: str | UUID) -> str:
    """Build the ``core-sub:{uuid}`` id for a core submission.

    Args:
        submission_uuid: The submission's UUID, as a :class:`~uuid.UUID` or any string a
            :class:`~uuid.UUID` can parse.

    Returns:
        The namespaced id, e.g. ``"core-sub:1e9f...-...."``.

    Raises:
        ValueError: If ``submission_uuid`` is not a valid UUID.
    """
    uuid = (
        submission_uuid
        if isinstance(submission_uuid, UUID)
        else UUID(str(submission_uuid))
    )
    return f"{CORE_SUBMISSION_PREFIX}{uuid}"


# Used by outside callers for consistent formatting of id
def contribs_site_id(project_id: str) -> str:
    """Build the ``contribs:{project_id}`` id for an MPContribs project.

    Args:
        project_id: The MPContribs project identifier (slug-like; no ``:``).

    Returns:
        The namespaced id, e.g. ``"contribs:carrier_transport"``.

    Raises:
        ValueError: If ``project_id`` is empty or contains characters outside
            ``[A-Za-z0-9_-]``.
    """
    if not _CONTRIBS_PROJECT_PATTERN.match(project_id):
        raise ValueError(
            f"invalid MPContribs project_id {project_id!r}; expected a slug of "
            "[A-Za-z0-9_-]."
        )
    return f"{CONTRIBS_PREFIX}{project_id}"


# Used by outside callers for consistent formatting of id
def material_site_id(material_id: str) -> str:
    """Return ``material_id`` if it is a valid bare material id.

    Material DOIs are un-prefixed, so this is an identity-with-validation rather than a
    prefixing builder; it exists so callers reach every id form through one place.

    Args:
        material_id: A bare material id such as ``"mp-1234"``.

    Returns:
        ``material_id`` unchanged.

    Raises:
        ValueError: If it does not match :data:`MATERIAL_ID_PATTERN`.
    """
    if not MATERIAL_ID_PATTERN.match(material_id):
        raise ValueError(
            f"invalid material id {material_id!r}; expected e.g. 'mp-1234' "
            f"(pattern {MATERIAL_ID_PATTERN.pattern})."
        )
    return material_id


def validate_site_unique_id(
    value: str, *, allowed: Iterable[Namespace] | None = None
) -> str:
    """Return ``value`` if it is a well-formed ``site_unique_id``, else raise.

    The single enforcement point for the contract. ``core-sub:`` ids must carry a valid
    UUID, ``contribs:`` ids a slug project id, and anything else must be a bare material
    id (`MATERIAL_ID_PATTERN`).

    Args:
        value: The candidate ``site_unique_id``.
        allowed: The namespaces to accept, any of ``"core-sub"``, ``"contribs"``,
            ``"material"``. Defaults to all three. Pass a subset to restrict a context
            (e.g. a contribs record only permits ``"contribs"``).

    Returns:
        ``value`` unchanged when valid.

    Raises:
        ValueError: If ``value`` is empty, malformed for its prefix, or in a namespace
            not in ``allowed``.
    """
    allowed = frozenset(allowed) if allowed is not None else _ALL_NAMESPACES

    if not value:
        raise ValueError(
            "site_unique_id must be a non-empty string; expected 'core-sub:<uuid>', 'contribs:<project_id>', or a material id like 'mp-1234'."
        )

    if value.startswith(CORE_SUBMISSION_PREFIX):
        kind: Namespace = "core-sub"
        rest = value[len(CORE_SUBMISSION_PREFIX) :]
        try:
            _ = UUID(rest)
        except ValueError:
            raise ValueError(
                f"{value!r} has a '{CORE_SUBMISSION_PREFIX}' prefix but {rest!r} is not "
                "a valid UUID."
            ) from None
    elif value.startswith(CONTRIBS_PREFIX):
        kind = "contribs"
        rest = value[len(CONTRIBS_PREFIX) :]
        if not _CONTRIBS_PROJECT_PATTERN.match(rest):
            raise ValueError(
                f"{value!r} has a '{CONTRIBS_PREFIX}' prefix but {rest!r} is not a valid "
                "project id (expected a slug of [A-Za-z0-9_-])."
            )
    elif MATERIAL_ID_PATTERN.match(value):
        kind = "material"
    else:
        raise ValueError(
            f"{value!r} is not a valid site_unique_id; expected 'core-sub:<uuid>', 'contribs:<project_id>', or a material id like 'mp-1234'."
        )

    if kind not in allowed:
        raise ValueError(
            f"{value!r} is a {kind!r} site_unique_id, which is not allowed here "
            f"(allowed: {sorted(allowed)})."
        )
    return value


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

    @field_validator("site_unique_id")
    @classmethod
    def _check_site_unique_id(cls, value: str | None) -> str | None:
        """Enforce the core namespaces (``core-sub:`` or a bare material id) when set."""
        if value is None:
            return value
        return validate_site_unique_id(value, allowed={"core-sub", "material"})


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
    title: str  # MPContribs project title
    site_url: str  # per-contribution landing page
    persons: list[Person]  # AUTHORs + CONTACT corresponding author
    organizations: list[Organization]  # funders as SPONSOR orgs
    description: str  # MPContribs project description
    doe_funded_flag: str = Field(default="N")  # Bool-like, but ELink is a `Y`/`N` str
    access_limitations: list[str] = Field(default_factory=lambda: ["UNL"])
    publication_date: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    @field_validator("site_unique_id")
    @classmethod
    def _check_site_unique_id(cls, value: str | None) -> str | None:
        """Enforce the ``contribs:`` namespace when set."""
        if value is None:
            return value
        return validate_site_unique_id(value, allowed={"contribs"})
