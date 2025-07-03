from typing import TypeAlias

from elinkapi import Elink
from elinkapi.record import RecordResponse
from pymongo import MongoClient

OstiID: TypeAlias = int


def find_out_of_date_records(
    client: MongoClient,
    robocrys_db: str,
    robocrys_collection: str,
    doi_db: str,
    doi_collection,
) -> list[OstiID]:
    robocrys = client.robocrys_db.robocrys_collection
    doi = client.doi_db.doi_collection

    out_of_data_osti_ids = []

    # robocrys docs newer than in doi

    return out_of_data_osti_ids


def update_existing_osti_record(*args, **kwargs) -> RecordResponse: ...


def submit_new_osti_record(*args, **kwargs) -> RecordResponse: ...


def update_state_of_osti_record(*args, **kwargs) -> RecordResponse: ...


def delete_osti_record(*args, **kwargs) -> RecordResponse: ...
