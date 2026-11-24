"""Tests for the site_unique_id contract (mp_cite.site_id)."""

from uuid import UUID

import pytest

from mp_cite import (
    contribs_site_id,
    core_submission_site_id,
    material_site_id,
    validate_site_unique_id,
)

_UUID = "12345678-1234-5678-1234-567812345678"


# --- builders ---------------------------------------------------------------------


def test_core_submission_site_id_from_str_and_uuid():
    assert core_submission_site_id(_UUID) == f"core-sub:{_UUID}"
    assert core_submission_site_id(UUID(_UUID)) == f"core-sub:{_UUID}"


def test_core_submission_site_id_rejects_non_uuid():
    with pytest.raises(ValueError):
        core_submission_site_id("not-a-uuid")


def test_contribs_site_id_valid_and_invalid():
    assert contribs_site_id("carrier_transport") == "contribs:carrier_transport"
    for bad in ["", "has space", "has:colon"]:
        with pytest.raises(ValueError):
            contribs_site_id(bad)


@pytest.mark.parametrize("good", ["mp-1234", "mvc-12"])
def test_material_site_id_valid(good):
    assert material_site_id(good) == good


# Only "mp-" and "mvc-" prefixes are in scope; other emmet-style prefixes are rejected.
@pytest.mark.parametrize(
    "bad", ["mp-", "foo-1", "mp-1a", "contribs:x", "MP-1", "mol-9", "mb-1"]
)
def test_material_site_id_invalid(bad):
    with pytest.raises(ValueError):
        material_site_id(bad)


# --- validator --------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [f"core-sub:{_UUID}", "contribs:my_project", "mp-1234", "mvc-7"],
)
def test_validate_accepts_each_form(value):
    assert validate_site_unique_id(value) == value


@pytest.mark.parametrize("value", ["", "no-namespace", "core-sub:nope", "contribs:bad id"])
def test_validate_rejects_malformed(value):
    with pytest.raises(ValueError):
        validate_site_unique_id(value)


def test_validate_rejects_none():
    with pytest.raises(ValueError):
        validate_site_unique_id(None)  # type: ignore[arg-type]


def test_validate_allowed_subset_rejects_other_namespace():
    # well-formed contribs id, but only material is permitted here
    with pytest.raises(ValueError):
        validate_site_unique_id("contribs:my_project", allowed={"material"})
    # the same id passes when its namespace is allowed
    assert (
        validate_site_unique_id("contribs:my_project", allowed={"contribs"})
        == "contribs:my_project"
    )
