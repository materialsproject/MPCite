from typing import Any, Literal, TypeAlias

import requests
from elinkapi import Elink, Record
from elinkapi.record import RecordResponse
from elinkapi.utils import Validation
from pymongo import MongoClient

OstiID: TypeAlias = int


def find_out_of_date_doi_entries(
    client: MongoClient,
    db: str,
    source_collection: str,
    doi_collection: str,
    *,
    source_key: str = "material_id",
    source_timestamp: str = "last_updated",
    doi_match_key: str = "material_id",
) -> list[OstiID]:
    """Find source documents updated more recently than the latest DOI document.

    By default this reproduces the core behaviour (a robocrys source keyed on
    ``material_id`` against the core DOI collection).

    Args:
        client: ``MongoClient`` used to access both collections.
        db: Name of the database holding both collections.
        source_collection: Name of the source collection (e.g. robocrys for core).
        doi_collection: Name of the DOI collection.
        source_key: Field on a source document that joins to the DOI collection.
            Defaults to ``"material_id"``.
        source_timestamp: The "last updated" field on a source document. Defaults to
            ``"last_updated"``.
        doi_match_key: Field on a DOI document to match ``source_key`` against.
            Defaults to ``"material_id"``.

    Returns:
        All OSTI IDs associated with out-of-date DOI entries.
    """
    source = client[db][source_collection]
    dois = client[db][doi_collection]

    latest_doi = next(
        dois.aggregate(
            [
                {"$project": {"_id": 0, "date_metadata_updated": 1}},
                {"$sort": {"date_metadata_updated": -1}},
                {"$limit": 1},
            ]
        )
    )["date_metadata_updated"]

    keys_to_update = list(
        map(
            lambda x: x[source_key],
            source.find(
                {source_timestamp: {"$gt": latest_doi}}, {"_id": 0, source_key: 1}
            ),
        )
    )

    return list(
        map(
            lambda x: x["osti_id"],
            dois.find(
                {doi_match_key: {"$in": keys_to_update}},
                {"_id": 0, "osti_id": 1},
            ),
        ),
    )


def update_existing_osti_record(
    elinkapi: Elink,
    osti_id: OstiID,
    new_values: dict[str, Any],
    *,
    state: Literal["save", "submit"] = "save",
) -> RecordResponse:
    """Partially update an existing record, identified by its OSTI ID.

    Args:
        elinkapi: The :class:`elinkapi.Elink` client for the environment in which the
            record is held (production or review).
        osti_id: OSTI ID of the record that ought to be updated.
        new_values: Mapping of E-Link record field names to their new (JSON-serializable)
            values. Only these fields are changed.
        state: Workflow state for the resulting revision — ``"save"`` (the default) to
            draft it, or ``"submit"`` to send it on for release.

    Returns:
        The record response for the new revision.
    """
    patch_operations = [
        {"op": "add", "path": f"/{field}", "value": value}
        for field, value in new_values.items()
    ]

    return elinkapi.patch_json(osti_id, patch_operations, state=state)


def submit_new_osti_record(
    elinkapi: Elink,
    record: Record,
    state: str = "submit",
) -> RecordResponse:
    """Submit an already-templated record to E-Link.

    Args:
        elinkapi: The :class:`elinkapi.Elink` client for the target environment.
        record: A fully constructed :class:`elinkapi.Record` (or subclass) ready for
            submission.
        state: The state to submit the record in. Defaults to ``"submit"``; pass
            ``"save"`` to only draft it. The default reflects the assumption that a new
            record is sent to OSTI only once it is ready for submission.

    Returns:
        The record response after submission.
    """
    return elinkapi.post_new_record(record, state)


def update_state_of_osti_record(
    elinkapi: Elink, osti_id: OstiID, new_state: Literal["save", "submit"]
) -> RecordResponse:
    """Update the state of a record to either ``"save"`` or ``"submit"``.

    Args:
        elinkapi: The :class:`elinkapi.Elink` client for the target environment.
        osti_id: OSTI ID of the record whose state should be updated.
        new_state: The new state, one of the two valid values ``"save"`` or
            ``"submit"``.

    Returns:
        The record response after updating the state.
    """
    record = elinkapi.get_single_record(osti_id)
    return elinkapi.update_record(osti_id, record, new_state)


def delete_osti_record(elinkapi: Elink, osti_id: OstiID, reason: str) -> bool:
    """Delete a record by its OSTI ID.

    Args:
        elinkapi: The :class:`elinkapi.Elink` client for the target environment.
        osti_id: OSTI ID of the record that ought to be deleted.
        reason: Human-readable explanation of why the record is being deleted
            (required by the HTTP request).

    Returns:
        ``True`` if the record was deleted successfully; ``False`` otherwise, which
        indicates a non-204 status code.
    """
    response = requests.delete(
        f"{elinkapi.target}records/{osti_id}?reason={reason}",
        headers={"Authorization": f"Bearer {elinkapi.token}"},
    )
    Validation.handle_response(response)
    return response.status_code == 204  # True if deleted successfully
