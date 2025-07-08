from typing import TypeAlias

from elinkapi import Elink
from elinkapi.record import RecordResponse, Record
from pymongo import MongoClient

import requests
from elinkapi.utils import Validation

OstiID: TypeAlias = int


def find_out_of_date_doi_entries(
    client: MongoClient,
    robocrys_db: str,
    robocrys_collection: str,
    doi_db: str,
    doi_collection: str,
) -> list[OstiID]:
    robocrys = client[robocrys_db][robocrys_collection]
    dois = client[doi_db][doi_collection]

    latest_doi = next(
        dois.aggregate(
            [
                {"$project": {"_id": 0, "date_record_last_updated_on_Elink": 1}},
                {"$sort": {"date_record_last_updated_on_Elink": -1}},
                {"$limit": 1},
            ]
        )
    )["date_record_last_updated_on_Elink"]

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
    elinkapi: Elink,
    osti_id: OstiID,
    new_values: dict
) -> RecordResponse:
    record_on_elink = elinkapi.get_single_record(osti_id)

    for keyword in new_values.keys():
        try:
            setattr(record_on_elink, keyword, new_values[keyword])
        except ValueError:
            print("Extraneous keywords found in the dictionary that do not correspond to attributes in the ELink API's record class.")

    # assume the use with fix the sponsor identifier bug before calling the update function
    # # fix the issue with the sponsor organization's identifiers
    # for entry in record_on_elink.organizations:
    #     if entry.type == "SPONSOR":
    #         entry.identifiers = [{"type": 'CN_DOE', "value": 'AC02-05CH11231'}]
    #         break

    return elinkapi.update_record(osti_id, record_on_elink, state="save") # user should use update_state_of_osti_record to submit instead


def submit_new_osti_record(
    elinkapi: Elink,
    new_record: Record,
    state = "submit", # assuming there is no need to both with saving. just send new record to osti when its ready for submission. also assume bug with DOE contract number identifier in sponsor organization is accounted for
) -> RecordResponse:
    # template for all repeated stuff
    # only submit
    record_response = elinkapi.post_new_record(new_record, state)

    return record_response


def update_state_of_osti_record(
    elinkapi: Elink,
    osti_id: OstiID,
    new_state = "submit"
) -> RecordResponse:
    record = elinkapi.get_single_record(osti_id)

    # assuming that the user will handle the sponsor identifier bug before calling this function
    # # fix the issue with the sponsor organization's identifiers
    # for entry in record.organizations:
    #     if entry.type == "SPONSOR":
    #         entry.identifiers = [{"type": 'CN_DOE', "value": 'AC02-05CH11231'}]
    #         break

    return elinkapi.update_record(osti_id, record, new_state)


def delete_osti_record(
    elinkapi: Elink,
    osti_id: OstiID,
    reason: str
) -> RecordResponse:
    """Delete a record by its OSTI ID."""
    response = requests.delete(f"{elinkapi.target}records/{osti_id}?reason={reason}", headers={"Authorization": f"Bearer {api.token}"})
    Validation.handle_response(response)
    return response.status_code == 204  # True if deleted successfully

# TODO: make the github error thing for the weird issue with identifiers not being allocated to the sponsoring organization