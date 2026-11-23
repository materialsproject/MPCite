from typing import Any, Literal, TypeAlias

import requests
from elinkapi import Elink, Record
from elinkapi.record import RecordResponse
from elinkapi.utils import Validation
from pymongo import MongoClient

OstiID: TypeAlias = int


class OstiClient:
    """Single client for the Materials Project DOI workflow.

    Attributes:
        _elink: The wrapped E-Link client, or ``None`` if not configured.
        _mongo: The wrapped Mongo client for the DOI store, or ``None`` if not configured.
        _db: Name of the database holding the source and DOI collections.
    """

    def __init__(
        self,
        elink: Elink | None = None,
        mongo: MongoClient[Any] | None = None,
        *,
        db: str | None = None,
    ) -> None:
        """Wrap already-constructed backend clients.

        Args:
            elink: The :class:`elinkapi.Elink` client to operate through. Prefer
                :meth:`for_review` / :meth:`for_production` unless you need to supply a
                custom client (e.g. in tests).
            mongo: The :class:`pymongo.MongoClient` for the DOI store. Required only for
                :meth:`find_out_of_date_doi_entries`.
            db: Name of the database holding the source and DOI collections. Required
                alongside ``mongo``.
        """
        self._elink: Elink | None = elink
        self._mongo: MongoClient[Any] | None = mongo
        self._db: str | None = db

    @classmethod
    def for_review(
        cls,
        settings: ElinkSettings | None = None,
        mongo_settings: MongoSettings | None = None,
    ) -> "OstiClient":
        """Build a client whose E-Link side targets the review environment.

        Args:
            settings: Pre-loaded :class:`ElinkSettings` to read the review token and
                endpoint from. Defaults to a fresh :class:`ElinkSettings` loaded from
                the environment.
            mongo_settings: When given, also attach the DOI store built from these
                settings (its ``database`` becomes the client's ``db``). Pass
                ``MongoSettings()`` to load the store config from the environment; omit
                to build an E-Link-only client.

        Returns:
            An :class:`OstiClient` whose E-Link side is pointed at the review environment.
        """
        return cls._build(review=True, settings=settings, mongo_settings=mongo_settings)

    @classmethod
    def for_production(
        cls,
        settings: ElinkSettings | None = None,
        mongo_settings: MongoSettings | None = None,
    ) -> "OstiClient":
        """Build a client whose E-Link side targets the production environment.

        Args:
            settings: Pre-loaded :class:`ElinkSettings` to read the production token and
                endpoint from. Defaults to a fresh :class:`ElinkSettings` loaded from
                the environment.
            mongo_settings: When given, also attach the DOI store built from these
                settings (its ``database`` becomes the client's ``db``). Pass
                ``MongoSettings()`` to load the store config from the environment; omit
                to build an E-Link-only client.

        Returns:
            An :class:`OstiClient` whose E-Link side is pointed at production.
        """
        return cls._build(
            review=False, settings=settings, mongo_settings=mongo_settings
        )

    @classmethod
    def _build(
        cls,
        *,
        review: bool,
        settings: ElinkSettings | None,
        mongo_settings: MongoSettings | None,
    ) -> "OstiClient":
        """Assemble a client for the given environment, with an optional DOI store.

        Shared by :meth:`for_review` and :meth:`for_production`.

        Args:
            review: Target the review environment when ``True``, else production.
            settings: E-Link settings, or ``None`` to load from the environment.
            mongo_settings: DOI store settings, or ``None`` for an E-Link-only client.

        Returns:
            The assembled :class:`OstiClient`.
        """
        elink = get_elink_client(review=review, settings=settings)
        if mongo_settings is None:
            return cls(elink)
        return cls(elink, get_mongo_client(mongo_settings), db=mongo_settings.database)

    @property
    def elink(self) -> Elink:
        """The wrapped E-Link client, as an escape hatch for raw API access.

        Raises:
            RuntimeError: If no E-Link client was configured.
        """
        if self._elink is None:
            raise RuntimeError(
                "No E-Link client configured; construct with an `elink` "
                "(e.g. OstiClient.for_review())."
            )
        return self._elink

    def _require_store(self) -> tuple[MongoClient[Any], str]:
        """Return the Mongo client and db name, or raise if the store is unconfigured.

        Raises:
            RuntimeError: If either ``mongo`` or ``db`` was not supplied.
        """
        if self._mongo is None or self._db is None:
            raise RuntimeError(
                "No MongoDB store configured; construct with `mongo=` and `db=` "
                "to use the DOI store."
            )
        return self._mongo, self._db

    def query_records(self, **params: Any) -> Query:
        """Query E-Link for records matching the given search parameters.

        Args:
            **params: E-Link query parameters (e.g. ``title=...``, ``product_type=...``).
                See the E-Link ``getRecords`` operation for the allowed set. With none,
                iterates all accessible records.

        Returns:
            An iterable :class:`elinkapi.query.Query` yielding a page of
            :class:`~elinkapi.record.RecordResponse` values at a time.
        """
        return self.elink.query_records(**params)

    def get_record(self, osti_id: OstiID) -> Record:
        """Fetch a single record by its OSTI ID.

        Args:
            osti_id: OSTI ID of the record to fetch.

        Returns:
            The record as currently held by E-Link.
        """
        return self.elink.get_single_record(osti_id)

    def submit_new_record(
        self, record: Record, state: str = "submit"
    ) -> RecordResponse:
        """Submit an already-templated record to E-Link.

        Args:
            record: A fully constructed :class:`elinkapi.Record` (or subclass) ready for
                submission.
            state: The state to submit the record in. Defaults to ``"submit"``; pass
                ``"save"`` to only draft it. The default reflects the assumption that a
                new record is sent to OSTI only once it is ready for submission.

        Returns:
            The record response after submission.
        """
        return self.elink.post_new_record(record, state)

    def update_record_fields(
        self,
        osti_id: OstiID,
        new_values: dict[str, Any],
        *,
        state: Literal["save", "submit"] = "save",
    ) -> RecordResponse:
        """Partially update an existing record, identified by its OSTI ID.

        Args:
            osti_id: OSTI ID of the record that ought to be updated.
            new_values: Mapping of E-Link record field names to their new
                (JSON-serializable) values. Only these fields are changed.
            state: Workflow state for the resulting revision — ``"save"`` (the default)
                to draft it, or ``"submit"`` to send it on for release.

        Returns:
            The record response for the new revision.
        """
        patch_operations = [
            {"op": "add", "path": f"/{field}", "value": value}
            for field, value in new_values.items()
        ]

        return self.elink.patch_json(osti_id, patch_operations, state=state)

    def update_state(
        self, osti_id: OstiID, new_state: Literal["save", "submit"]
    ) -> RecordResponse:
        """Update the state of a record to either ``"save"`` or ``"submit"``.

        Args:
            osti_id: OSTI ID of the record whose state should be updated.
            new_state: The new state, one of the two valid values ``"save"`` or
                ``"submit"``.

        Returns:
            The record response after updating the state.
        """
        record = self.get_record(osti_id)
        return self.elink.update_record(osti_id, record, new_state)

    def delete_record(self, osti_id: OstiID, reason: str) -> bool:
        """Delete a record by its OSTI ID.

        Issues a raw HTTP ``DELETE`` because the wrapped client exposes no delete; this
        keeps that workaround in one place.

        Args:
            osti_id: OSTI ID of the record that ought to be deleted.
            reason: Human-readable explanation of why the record is being deleted
                (required by the HTTP request).

        Returns:
            ``True`` if the record was deleted successfully; ``False`` otherwise, which
            indicates a non-204 status code.
        """
        import requests
        from elinkapi.utils import Validation

        elink = self.elink
        response = requests.delete(
            f"{elink.target}records/{osti_id}?reason={reason}",
            headers={"Authorization": f"Bearer {elink.token}"},
        )
        Validation.handle_response(response)
        return response.status_code == 204  # True if deleted successfully

    def find_out_of_date_doi_entries(
        self,
        source_collection: str,
        doi_collection: str,
        *,
        source_key: str = "material_id",
        source_timestamp: str = "last_updated",
        doi_match_key: str = "material_id",
    ) -> list[OstiID]:
        """Find DOIs whose source material was updated after the DOI was last pushed.

        The discovery step of the update loop: it takes the newest
        ``date_metadata_updated`` across all minted DOIs, finds source documents updated
        after that, and returns the OSTI IDs of the DOI documents behind them. Those IDs
        are what you then feed to :meth:`update_record_fields` / :meth:`update_state`.

        By default this reproduces the core behaviour (a robocrys source keyed on
        ``material_id`` against the core DOI collection). Requires a configured Mongo
        store (``mongo`` and ``db`` at construction).

        Args:
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
        mongo, db = self._require_store()
        source = mongo[db][source_collection]
        dois = mongo[db][doi_collection]

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
