from typing import TypeAlias

from elinkapi import Elink
from elinkapi.record import RecordResponse
from pymongo import MongoClient

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


def update_existing_osti_record(*args, **kwargs) -> RecordResponse: ...


def submit_new_osti_record(*args, **kwargs) -> RecordResponse: ...


def update_state_of_osti_record(*args, **kwargs) -> RecordResponse: ...


def delete_osti_record(*args, **kwargs) -> RecordResponse: ...
