import pytest
from elinkapi import Elink, Record, exceptions
import os
from src.mp_cite.core import make_minimum_record_to_fully_release
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


valid_save_json = {
    "title": "Electron microscope data for photons",
    "site_ownership_code": "LLNL",
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
    "site_ownership_code": "LBNL",
    "title": "Sample document title",
}


@pytest.fixture
def elink_review_client():
    """
    tests whether or not the elink review client can be properly retrieved.
    returns the elink review client
    """
    elink_review_api_key = os.getenv("elink_review_api_token")
    review_endpoint = os.getenv("ELINK_REVIEW_ENDPOINT")
    return Elink(token=elink_review_api_key, target=review_endpoint)


@pytest.fixture
def elink_production_client():
    """
    tests whether or not the elink review client can be properly retrieved.
    returns the elink review client
    """
    elink_prod_api_key = os.getenv("elink_api_PRODUCTION_key")
    return Elink(token=elink_prod_api_key)


osti_id = "2300069"
# osti_id = 2300063
media_id = "1900082"
reason = "I wanted to"
revision_number = "2"
date = datetime.now()
state = "save"
file_path = "./test_media_files/media_file.txt"
file_path2 = "./test_media_files/best_media_file.txt"
file_path3 = "./test_media_files/another_media_file.txt"
json_responses = []
reserved_osti_id = 1


# RECORD ENDPOINTS
# Post a new Record
@pytest.fixture
def test_post_new_record(elink_review_client):
    record_to_post = make_minimum_record_to_fully_release(
        title="Test Post Record - PyTest"
    )
    # try:
    #     saved_record = elink_review_client.post_new_record(record_to_post, "save") # Works - saved
    # except exceptions.ForbiddenException as fe:
    #     pytest.fail(f"Forbidden: Check API key or permissions associated with provided API key. {fe}")
    # except exceptions.BadRequestException as ve:
    #     pytest.fail(f"Bad Request: Possibly incorrect parameters. {ve}")
    # except Exception as e:
    #     pytest.fail(f"Unexpected error: {e}")

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


def test_get_new_single_record(test_post_new_record):
    # record_to_post = make_minimum_record_to_fully_release(title="Test Getting New Single Record - PyTest")
    # submitted_record = elink_review_client.post_new_record(record_to_post, "submit")

    posted_record = test_post_new_record

    elink_review_api_key = os.getenv("elink_review_api_token")
    review_endpoint = os.getenv("ELINK_REVIEW_ENDPOINT")
    elink_review_client = Elink(token=elink_review_api_key, target=review_endpoint)

    osti_id = test_post_new_record.osti_id

    single_record = elink_review_client.get_single_record(osti_id)

    assert osti_id is not None
    assert single_record.title == posted_record.title
    # assert single_record.organizations == record_to_post.organizations # this doesn't work because Elink's pydantic model defaults empty identifier to [], where as an empty identifier field is returned as None.
    # assert single_record.persons == record_to_post.persons # same issue as above^
    assert single_record.publication_date == posted_record.publication_date


def test_invalid_query(elink_production_client):
    list_of_records = elink_production_client.query_records(
        title="Allo-ballo holla olah"
    )  # works, nothing found
    assert list_of_records.total_rows == 0


# Reserve a DOI
def test_reserve_DOI(elink_review_client):
    try:
        elink_review_client.reserve_doi(Record(**valid_save_json))  # works - naved
    except Exception:
        print("failed to reserve doi on record")


def test_update_record(test_post_new_record):
    posted_record = test_post_new_record
    osti_id = posted_record.osti_id

    elink_review_api_key = os.getenv("elink_review_api_token")
    review_endpoint = os.getenv("ELINK_REVIEW_ENDPOINT")
    elink_review_client = Elink(token=elink_review_api_key, target=review_endpoint)

    # Update an existing Record
    elink_review_client.update_record(
        osti_id,
        make_minimum_record_to_fully_release("Test Updating Record - PyTest"),
        "submit",
    )  # works

    # Get Revision based on revision number
    elink_review_client.get_revision_by_number(osti_id, revision_number)  # works
    # Get Revision based on date Currently Not Working...?
    # revision_by_date = elink_review_client.get_revision_by_date(osti_id, date.strftime("%Y-%d-%m")) # works
    # Get all RevisionHistory of a Record
    revision_history = elink_review_client.get_all_revisions(osti_id)  # works
    revision_history[0]
    revision_history[-1]

    # # MEDIA ENDPOINTS
    # # Associate new Media with a Record
    # posted_media = elink_review_client.post_media(osti_id, file_path, {"title": "Title of the Media media_file.txt"})
    # posted_media3 = elink_review_client.post_media(osti_id, file_path3, {"title": "Title of the Media media_file.txt"})
    # media_id = posted_media.media_id
    # # Replace existing Media on a Record
    # replaced_media2 = elink_review_client.put_media(osti_id, media_id, file_path2, {"title": "Changed this title now"})
    # # Get Media associated with OSTI ID
    # media = elink_review_client.get_media(osti_id)
    # # Get Media content of a media resource
    # media_content = elink_review_client.get_media_content(media_id)
    # # Delete Media with media_id off of a Record
    # isSuccessDelete = elink_review_client.delete_single_media(osti_id, media_id, reason) #works
    # assert isSuccessDelete
    # # Delete all Media associated with a Record
    # isSuccessAllDelete = elink_review_client.delete_all_media(osti_id, reason)
    # assert isSuccessAllDelete

    # # Should see that all media has been deleted
    # final_media = elink_review_client.get_media(osti_id)

    # print("Finished")
