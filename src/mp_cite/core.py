from elinkapi import Elink
from elinkapi.record import RecordResponse
from pymongo import MongoClient
import requests
from elinkapi.utils import Validation
from mp_cite.models import MinimumDARecord
from typing import Literal, TypeAlias

OstiID: TypeAlias = int

def find_out_of_date_doi_entries(
    rc_client: MongoClient,
    doi_client: MongoClient,
    robocrys_db: str,
    robocrys_collection: str,
    doi_db: str,
    doi_collection: str,
) -> list[OstiID]:
    """
    find_out_of_date_doi_entries queries MP's mongo collections to find all robocrys documents that were updated less recently than the latest doi document

    :rc_client is the MongoClient used to access the robocrys collection
    :doi_client is the MongoClient used to access the doi collection (since a new doi collection is planned, they clients are passed separately, though in the future they may be the same client.)
    :robocrys_db is the name of the database the robocrys collection is in
    :robocrys_collection is the name of the robocrys collection
    :doi_db is the name of the database the doi collection is in
    :doi_collection is the name of the doi collection

    returns a list containing all OSTI IDs associated with out-of-date doi entries.
    """
    robocrys = rc_client[robocrys_db][robocrys_collection]
    dois = doi_client[doi_db][doi_collection]

    latest_doi = next(
        dois.aggregate(
            [
                {"$project": {"_id": 0, "date_metadata_updated": 1}},
                {"$sort": {"date_metadata_updated": -1}},
                {"$limit": 1},
            ]
        )
    )["date_metadata_updated"]

    material_ids_to_update = list(
        map(
            lambda x: x["material_id"],
            robocrys.find(
                {"last_updated": {"$gt": latest_doi}}, {"_id": 0, "material_id": 1}
            ),
        )
    )

    return list(
        map(
            lambda x: x["osti_id"],
            dois.find(
                {"material_id": {"$in": material_ids_to_update}},
                {"_id": 0, "osti_id": 1},
            ),
        ),
    )


def update_existing_osti_record(
    elinkapi: Elink, osti_id: OstiID, new_values: dict
) -> RecordResponse:
    """
    update_existing_osti_record allows users to provide a dictionary of keywords and new values, which will replace the old values under the same keywords in the record with the given osti id

    :elinkapi is the instance of the elinkapi associated with the environment in which the record is held (e.g. either production or review environment)
    :osti_id is the osti id of the record which ought to be updated
    :new_values is a dictionary of keywords (which should exist in ELink's record model) and new value pairs.

    N.B., it is currently assumed that the user will handle the "sponsor identifier bug"
    --- in which the retreived record responses of validated records from the E-Link production environment seemingly
    lack the required Sponsor Organization identifiers which were necessary for their submission (due to rearrangement of metadata
    on E-Link's side) --- before calling this function.

    Otherwise, the following code excerpt would need to be added to retroactively fix the issue with the sponsor organization's identifiers
    for entry in record.organizations:
        if entry.type == "SPONSOR":
            entry.identifiers = [{"type": 'CN_DOE', "value": 'AC02-05CH11231'}]
            break

    Instead, we leave this for the user.
    """

    record_on_elink = elinkapi.get_single_record(osti_id)

    for keyword in new_values:
        setattr(record_on_elink, keyword, new_values[keyword])

    return elinkapi.update_record(
        osti_id, record_on_elink, state="save"
    )  # user should use update_state_of_osti_record to submit instead


def submit_new_osti_record(
    elinkapi: Elink,
    new_values: dict,
    state="submit",
) -> RecordResponse:
    """
    submit_new_osti_record generates a new record based on the provided keyword-value pairs in the new_values dict and the default minimum DA Record metadata necessary for submission

    :elinkapi is the elinkapi (see previous)
    :new_values is the dictionary of keywords and values which want to be included in the submitted record (besides or in lieu of default values). The title MUST be provided.
    :state defaults to "submit" but the user can simply "save" if desired. This is done given our assumption that there is
    no need to both with saving, rather, just only send new record to osti when it's ready for submission.

    returns the record response after submission
    """

    # template for all repeated stuff
    # only submit
    new_record = MinimumDARecord(
        **new_values
    )  # record is an instance of the MinimumDARecord model which gives default values to all necessary fields (EXCEPT Title)
    record_response = elinkapi.post_new_record(new_record, state)

    return record_response


def update_state_of_osti_record(
    elinkapi: Elink, osti_id: OstiID, new_state: Literal["save", "submit"]
) -> RecordResponse:
    """
    update_state_of_osti_record allows a user to update the state of a record with provided osti_id to either "save" or "submit" (the two valid states)

    :elinkapi is the elinkapi (see previous)
    :osti_id is the OSTI ID associated with the record of which to update state.
    :new_state is a Literal object, in this case a subtype of strings (either "save" or "submit").

    returns the record response after updating the state.
    """
    record = elinkapi.get_single_record(osti_id)
    return elinkapi.update_record(osti_id, record, new_state)


def delete_osti_record(elinkapi: Elink, osti_id: OstiID, reason: str) -> bool:
    """
    Delete a record by its OSTI ID.

    :elinkapi is the elinkapi
    :osti_id is the osti_id associated with the record which ought to be deleted
    :reason is a str object which explains in words why the record is to be deleted (necessary for the http request)

    returns true if deleted successfully, else false, which indicates a bad status_code
    """
    response = requests.delete(
        f"{elinkapi.target}records/{osti_id}?reason={reason}",
        headers={"Authorization": f"Bearer {elinkapi.token}"},
    )
    Validation.handle_response(response)
    return response.status_code == 204  # True if deleted successfully