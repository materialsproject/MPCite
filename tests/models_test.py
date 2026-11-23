"""Offline tests for the DOI record templates and stored models (no network)."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from mp_cite import (
    Affiliation,
    ContribsDoiModel,
    ContribsDoiRequest,
    Identifier,
    MinimumDARecord,
    Organization,
    Person,
    RelatedIdentifier,
)


def test_minimum_da_record_defaults():
    record = MinimumDARecord(title="Core Materials Data")
    dumped = record.model_dump()

    assert dumped["product_type"] == "DA"
    assert dumped["access_limitations"] == ["UNL"]
    assert dumped["site_ownership_code"] == "LBNL-MP"
    # core carries the DOE sponsor identifier and the Persson author
    assert "AC02-05CH11231" in str(dumped["organizations"])
    assert "Persson" in str(dumped["persons"])


def _full_contribs_request() -> ContribsDoiRequest:
    return ContribsDoiRequest(
        title="My Contributed Dataset",
        site_url="https://contribs.materialsproject.org/projects/my_project",
        description="A great contributed dataset.",
        keywords=["perovskite", "bandgap"],
        persons=[
            Person(
                type="AUTHOR",
                first_name="Ada",
                last_name="Lovelace",
                orcid="0000-0002-1825-0097",
                affiliations=[Affiliation(name="Analytical Engine Institute")],
            ),
            Person(
                type="CONTACT",
                first_name="Ada",
                last_name="Lovelace",
                email=["ada@example.org"],
            ),
        ],
        organizations=[
            Organization(
                type="SPONSOR",
                name="USDOE Office of Science (SC), Basic Energy Sciences (BES)",
                identifiers=[Identifier(type="CN_DOE", value="DE-AC02-05CH11231")],
            ),
            Organization(
                type="SPONSOR",
                name="Some Non-DOE Foundation",
                identifiers=[Identifier(type="CN_NONDOE", value="GRANT-123")],
            ),
        ],
        related_identifiers=[
            RelatedIdentifier(
                type="DOI", relation="IsSupplementTo", value="10.1234/paper"
            )
        ],
        doe_funded_flag="Y",
    )


def test_contribs_request_scaffolding_defaults():
    request = _full_contribs_request()
    dumped = request.model_dump()

    assert dumped["product_type"] == "DA"
    assert dumped["access_limitations"] == ["UNL"]
    assert dumped["doe_funded_flag"] == "Y"
    # not set by this template -> inherited None from Record
    assert dumped["site_ownership_code"] is None


def test_contribs_request_carries_no_mp_attribution():
    dumped = _full_contribs_request().model_dump()
    blob = str(dumped)

    assert "LBNL-MP" not in blob
    assert "LBNL Materials Project" not in blob
    # no core DOE contract (the caller's own DE-AC02... award is distinct and allowed)
    assert "'value': 'AC02-05CH11231'" not in blob
    # no default Persson author
    assert not any(p.get("last_name") == "Persson" for p in dumped["persons"])


def test_contribs_request_related_dois_use_is_supplement_to():
    dumped = _full_contribs_request().model_dump()
    rel = dumped["related_identifiers"][0]
    assert rel["type"] == "DOI"
    assert rel["relation"] == "IsSupplementTo"


def test_contribs_request_requires_title_and_site_url():
    with pytest.raises(ValidationError):
        ContribsDoiRequest(site_url="https://example.org")  # missing title
    with pytest.raises(ValidationError):
        ContribsDoiRequest(title="No landing page")  # missing site_url


def test_contribs_doi_model_has_no_material_id():
    stored = ContribsDoiModel(
        doi="10.17188/abc123",
        title="My Contributed Dataset",
        osti_id=999,
        site_unique_id="my_project",
        site_url="https://contribs.materialsproject.org/projects/my_project",
        workflow_status="SV",
        date_submitted_to_osti_first=datetime(2026, 1, 1),
        date_submitted_to_osti_last=datetime(2026, 1, 2),
    )

    assert not hasattr(stored, "material_id")
    assert "material_id" not in stored.model_dump()
    assert stored.site_unique_id == "my_project"
