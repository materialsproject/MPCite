import pytest
from elinkapi import Record, exceptions
from elinkapi.record import RecordResponse

import sys
import os

from dotenv import load_dotenv
from datetime import datetime


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.mp_cite.models import (
    MinimumDARecord,
)  # cannot find a good workaround for this with relative importing...
import src.mp_cite.core as core


load_dotenv()

valid_save_json = {
    "title": "Test Reserving DOI - PyTest",
    "site_ownership_code": "LBNL-MP",
    "product_type": "TR",
    "description": "Hello, from teh other side",
}
valid_save_update_json = {
    "title": "Electron microscope data for photons",
    "site_ownership_code": "LLNL",
    "product_type": "TR",
    "description": "A NEW custom description. Search on 'Allo-ballo holla olah'.",
}
invalid_save_json = {"product_type": "TD", "site_ownership_code": "LLNL"}
valid_submit_json = {
    "persons": [
        {
            "type": "AUTHOR",
            "first_name": "Required",
            "middle_name": "Optional",
            "last_name": "Required",
            "email": ["optional@optional.org"],
            "orcid": "0000000155554447",
            "phone": "Optional",
            "affiliations": [{"name": "Optional"}],
        },
        {
            "type": "RELEASE",
            "first_name": "Required",
            "middle_name": "Optional",
            "last_name": "Required",
            "email": ["required@required.org"],
            "phone": "Optional",
        },
        {
            "type": "CONTRIBUTING",
            "first_name": "Required",
            "middle_name": "Optional",
            "last_name": "Required",
            "email": ["optional@optional.org"],
            "phone": "Optional",
            "contributor_type": "Producer",
            "affiliations": [{"name": "Optional"}],
        },
    ],
    "organizations": [
        {"type": "AUTHOR", "name": "Required"},
        {"type": "CONTRIBUTING", "name": "Required", "contributor_type": "Producer"},
        {
            "type": "SPONSOR",
            "name": "Required",
            "identifiers": [
                {"type": "CN_NONDOE", "value": "Required"},
                {"type": "CN_DOE", "value": "SC0001234"},
                {"type": "AWARD_DOI", "value": "Optional"},
            ],
        },
        {"type": "RESEARCHING", "name": "Required"},
    ],
    "identifiers": [
        {"type": "CN_DOE", "value": "SC0001234"},
        {"type": "CN_NONDOE", "value": "Required"},
    ],
    "related_identifiers": [],
    "access_limitations": ["UNL"],
    "country_publication_code": "US",
    "description": "Information about a particular record, report, or other document, or executive summary or abstract of same.",
    "languages": ["English"],
    "product_type": "TR",
    "publication_date": "2018-02-21",
    "publication_date_text": "Winter 2012",
    "released_to_osti_date": "2023-03-03",
    "site_ownership_code": "LBNL-MP",
    "title": "Sample document title",
}

osti_id = "2300069"
# osti_id = 2300063
media_id = "1900082"
reason = "I wanted to"
revision_number = "2"
date = datetime.now()
state = "save"
json_responses = []
reserved_osti_id = 1


# RECORD ENDPOINTS
# Post a new Record
@pytest.fixture
def test_post_new_record(elink_review_client) -> RecordResponse:
    record_to_post = MinimumDARecord(title="Test Post Record - PyTest")

    try:
        submitted_record = elink_review_client.post_new_record(
            record_to_post, "submit"
        )  # Works - submit
        return submitted_record
    except exceptions.ForbiddenException as fe:
        pytest.fail(
            f"Forbidden: Check API key or permissions associated with provided API key. {fe}"
        )
    except exceptions.BadRequestException as ve:
        pytest.fail(f"Bad Request: Possibly incorrect parameters. {ve}")
    except Exception as e:
        pytest.fail(f"Unexpected error: {e}")


def test_get_new_single_record(test_post_new_record, elink_review_client):
    posted_record = test_post_new_record

    osti_id = test_post_new_record.osti_id

    single_record = elink_review_client.get_single_record(osti_id)

    try:
        assert osti_id is not None
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Failed Test")
        pytest.fail("Assertion failed!")

    try:
        assert single_record.title == posted_record.title
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Failed Test")
        pytest.fail("Assertion failed!")
    # assert single_record.organizations == record_to_post.organizations # this doesn't work because Elink's pydantic model defaults empty identifier to [], where as an empty identifier field is returned as None.
    # assert single_record.persons == record_to_post.persons # same issue as above^

    try:
        assert single_record.publication_date == posted_record.publication_date
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Failed Test")
        pytest.fail("Assertion failed!")

    core.delete_osti_record(elink_review_client, osti_id, "Completed Test")


def test_invalid_query(elink_production_client):
    list_of_records = elink_production_client.query_records(
        title="Allo-ballo holla olah"
    )  # works if nothing found
    assert list_of_records.total_rows == 0


# Reserve a DOI
def test_reserve_DOI(elink_review_client):
    try:
        rr = elink_review_client.reserve_doi(Record(**valid_save_json))
    except exceptions.ForbiddenException as fe:
        core.delete_osti_record(elink_review_client, rr.osti_id, "Completed Test")
        pytest.fail(
            f"Forbidden: Check API key or permissions associated with provided API key. {fe}"
        )
    except exceptions.BadRequestException as ve:
        core.delete_osti_record(elink_review_client, rr.osti_id, "Completed Test")
        pytest.fail(f"Bad Request: Possibly incorrect parameters. {ve}")
    except Exception as e:
        core.delete_osti_record(elink_review_client, rr.osti_id, "Completed Test")
        pytest.fail(f"Unexpected error: {e}")

    core.delete_osti_record(elink_review_client, rr.osti_id, "Completed Test")


def test_update_record(test_post_new_record, elink_review_client):
    posted_record = test_post_new_record
    osti_id = posted_record.osti_id

    # Update an existing Record
    try:
        elink_review_client.update_record(
            osti_id,
            MinimumDARecord(title="Test Updating Record - PyTest"),
            "submit",
        )
    except exceptions.ForbiddenException as fe:
        core.delete_osti_record(elink_review_client, osti_id, "Completed Test")
        pytest.fail(
            f"Forbidden: Check API key or permissions associated with provided API key. {fe}"
        )
    except exceptions.BadRequestException as ve:
        core.delete_osti_record(elink_review_client, osti_id, "Completed Test")
        pytest.fail(f"Bad Request: Possibly incorrect parameters. {ve}")
    except Exception as e:
        core.delete_osti_record(elink_review_client, osti_id, "Completed Test")
        pytest.fail(f"Unexpected error: {e}")

    # Get Revision based on revision number
    try:
        elink_review_client.get_revision_by_number(osti_id, revision_number)
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Completed Test")
        pytest.fail(
            f"Failed to get revision {revision_number} on record with OSTI ID: {osti_id}"
        )

    # as of 8/7/2025, elinkapi 0.5.1, these get_all_revisions() calls have stopped working)...
    # Get all RevisionHistory of a Record
    try:
        revision_history = elink_review_client.get_all_revisions(osti_id)  # works
        revision_history[0]
        revision_history[-1]
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Completed Test")
        pytest.fail(
            f"Failed to get entire revision history of record with OSTI ID: {osti_id}"
        )

    core.delete_osti_record(elink_review_client, osti_id, "Completed Test")
