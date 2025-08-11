import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import src.mp_cite.core as core

import pytest


def test_update_existing_osti_record(elink_review_client):
    record = core.submit_new_osti_record(
        elink_review_client,
        new_values={"title": "Test Update Existing OSTI Record | Pytest"},
    )
    osti_id = record.osti_id
    date_old = record.date_metadata_updated

    try:
        assert record.title == "Test Update Existing OSTI Record | Pytest"
        assert record.workflow_status == "SO"
        assert record.description is None
    except Exception:
        core.delete_osti_record(
            elink_review_client, osti_id, "Unexpected submission..."
        )
        pytest.fail("Failed submit new record as expected! Deleting test record...")

    try:
        # state = "save"
        # patch = { "description": "This is a new robocrys description" }

        # response = requests.patch(f"{elink_review_client.target}/records/{osti_id}/{state}",
        #                 headers = {
        #                     "Authorization" : f"Bearer {elink_review_client.token}",
        #                     "Content-Type": "application/json"
        #                 },
        #                 data=json.dumps(patch))
        # print("TEST TEST TEST RESPONSE: ", response, "\n", response.text)

        # if response.status_code == 400:
        #     core.delete_osti_record(elink_review_client, osti_id, "Test Failed!")
        #     pytest.fail("Failed to patch!")

        record = core.update_existing_osti_record(
            elink_review_client,
            osti_id,
            {"description": "This is a new robocrys description"},
            new_state="save",
        )

        # elink_review_client.patch_record(osti_id, { """description""": """This is a new robocrys description""" })

        assert record.workflow_status == "SA"
        assert record.description == "This is a new robocrys description"
        assert record.date_metadata_added > date_old
    except Exception:
        core.delete_osti_record(elink_review_client, osti_id, "Test Failed!")
        pytest.fail("Failed to updated existing osti record! Deleting test record...")

    core.delete_osti_record(elink_review_client, osti_id, "Test Completed.")


def test_submit_new_osti_record(elink_review_client):
    """
    Submits a record and then retrieves said submitted record. Checks that each keyword-value pair remains matching, since no updates/patches have been made.
    """

    record_submit = core.submit_new_osti_record(
        elink_review_client,
        new_values={"title": "Test Submit New OSTI Record | Pytest"},
    )
    osti_id = record_submit.osti_id

    record_got = elink_review_client.get_single_record(osti_id)

    for keyword, value in record_got:
        if keyword == "workflow_status" or getattr(record_submit, keyword) == value:
            # since the workflow_status of submitted osti records changes so quickly in the review environment, we cannot verify that one.
            pass
        else:
            core.delete_osti_record(elink_review_client, osti_id, "Test Completed.")
            pytest.fail(
                f"The submitted record's {keyword} does not match the retrieved record's {keyword}: {getattr(record_submit, keyword)} != {value}"
            )

    core.delete_osti_record(elink_review_client, osti_id, "Test Completed.")


def test_update_state_of_osti_record(elink_review_client):
    record_submit = core.submit_new_osti_record(
        elink_review_client,
        new_values={"title": "SUBMIT ONLY Test Updated State OSTI Record | Pytest"},
    )
    osti_id = record_submit.osti_id

    record_updated_save = core.update_state_of_osti_record(
        elink_review_client, osti_id, "save"
    )
    try:
        assert record_updated_save.workflow_status == "SA"
        assert record_updated_save.revision == 2
    except Exception:
        core.delete_osti_record(
            elink_review_client, osti_id, "Failed to change to saved."
        )
        pytest.fail(
            f"Failed to updated to save status: Workflow Status at Fail == {record_updated_save.workflow_status} and revision # == {record_updated_save.revision}"
        )

    record_updated_save = core.update_state_of_osti_record(
        elink_review_client, osti_id, "submit"
    )
    try:
        assert record_updated_save.workflow_status == "SO"
        assert record_updated_save.revision == 3
    except Exception:
        # core.delete_osti_record(elink_review_client, osti_id, "Failed to change to submit.")
        pytest.fail(
            f"Failed to update to submit status: Workflow Status at Fail == {record_updated_save.workflow_status} and Revision # == {record_updated_save.revision}"
        )
        # Need to ask about the desired functionality updating state to submit...

    core.delete_osti_record(elink_review_client, osti_id, "Test Completed.")
