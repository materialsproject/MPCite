from elinkapi import exceptions, Person, Organization, Record
from elinkapi.record import RecordResponse
import pytest
from dotenv import load_dotenv

from .conf_test import elink_production_client, elink_review_client

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import src.mp_cite.core as core

load_dotenv()


def test_get_single_record(elink_production_client):
    """
    tried to use the production client to retrieve a record.
    """
    try:
        record = elink_production_client.get_single_record(1190959)
        assert record.title == "Materials Data on Si by Materials Project"
        assert record.osti_id == 1190959
    except exceptions.ForbiddenException as fe:
        pytest.fail(
            f"Forbidden: Check API key or permissions associated with provided API key. {fe}"
        )
    except exceptions.BadRequestException as ve:
        pytest.fail(f"Bad Request: Possibly incorrect parameters. {ve}")
    except Exception as e:
        pytest.fail(f"Unexpected error: {e}")


def test_query_records(elink_production_client):
    """
    tests the query functionality of the elinkapi on the production environment
    """
    try:
        elink_production_client.query_records()
    except exceptions.ForbiddenException as fe:
        pytest.fail(
            f"Forbidden: Check API key or permissions associated with provided API key. {fe}"
        )
    except exceptions.BadRequestException as ve:
        pytest.fail(f"Bad Request: Possibly incorrect parameters. {ve}")
    except Exception as e:
        pytest.fail(f"Unexpected error: {e}")


def test_query_exists(elink_review_client):
    """
    tests to see that the query does in fact resolve entries in the form of RecordResponse objects.
    """
    assert isinstance(next(elink_review_client.query_records()), RecordResponse)


def test_switching_states(elink_review_client):
    """
    This test repeats the tests done to demonstrate unexpected behavior or passing "save" and "submit" states present in Elinkapi 0.5.2.
    """
    my_record_dict = {
        "product_type": "DA",
        "title": "My Dataset",
        "organizations": [
            Organization(type="RESEARCHING", name="LBNL Materials Project (LBNL-MP)"),
            Organization(
                type="SPONSOR",
                name="TEST SPONSOR ORG",
                identifiers=[{"type": "CN_DOE", "value": "AC02-05CH11231"}],
            ),  # sponsor org is necessary for submission
        ],
        "persons": [Person(type="AUTHOR", last_name="Persson")],
        "site_ownership_code": "LBNL-MP",
        "access_limitations": ["UNL"],
        "publication_date": "2025-8-12",
        "site_url": "https://next-gen.materialsproject.org/materials",
    }

    my_record = Record(**my_record_dict)

    # save in post then update to submit
    try:
        my_rr = elink_review_client.post_new_record(my_record, "save")
        osti_id = my_rr.osti_id
        assert my_rr.workflow_status == "SA"
        assert my_rr.revision == 1

        got_record = elink_review_client.get_single_record(osti_id)
        record_updated_state = elink_review_client.update_record(
            osti_id, got_record, "submit"
        )
        assert record_updated_state.workflow_status == "SO"
        assert record_updated_state.revision == 2
        core.delete_osti_record(elink_review_client, osti_id, "Test completed!")
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Test failed!")
        pytest.fail("Test failed!")

    # submit in post then update to save
    try:
        record_submit_first = elink_review_client.post_new_record(my_record, "submit")
        osti_id = record_submit_first.osti_id
        assert record_submit_first.workflow_status == "SO"
        assert record_submit_first.revision == 1

        got_submitted_record = elink_review_client.get_single_record(osti_id)
        record_updated_state = elink_review_client.update_record(
            osti_id, got_submitted_record, "save"
        )
        assert (
            record_updated_state.workflow_status == "SA"
        )  # record was submitted but switched to save, so should be 'SA'
        assert record_updated_state.revision == 2
        core.delete_osti_record(elink_review_client, osti_id, "Test completed!")
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Test failed!")
        pytest.fail("Test failed!")

    # update the workflow_status manually?
    try:
        record_to_manual_update = elink_review_client.post_new_record(my_record, "save")
        osti_id = record_to_manual_update.osti_id
        assert record_to_manual_update.workflow_status == "SA"
        assert record_to_manual_update.revision == 1

        got_record_to_manual_update = elink_review_client.get_single_record(osti_id)
        got_record_to_manual_update.workflow_status = "SO"
        record_after_manual_update = elink_review_client.update_record(
            osti_id, got_record_to_manual_update, "submit"
        )
        assert record_after_manual_update.workflow_status == "SO"
        assert record_after_manual_update.revision == 2
        core.delete_osti_record(elink_review_client, osti_id, "Test completed!")
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Test failed!")
        pytest.fail("Test failed!")
