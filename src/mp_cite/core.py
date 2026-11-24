from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from elinkapi.record import WorkflowStatus

from mp_cite.models import validate_site_unique_id
from mp_cite.settings import get_elink_client

if TYPE_CHECKING:
    from elinkapi import Elink, Record
    from elinkapi.query import Query
    from elinkapi.record import RecordResponse

    from mp_cite.settings import ElinkSettings

OstiID: TypeAlias = int

# Workflow states in which OSTI is actively processing a release, so starting a new
# revision would race that processing.
_TRANSIENT_STATUSES: frozenset[str] = frozenset(
    {
        WorkflowStatus.SubmitReleasing.value,  # "SR"
        WorkflowStatus.SubmitOSTI.value,  # "SO"
    }
)


class OstiClient:
    """Single client for the Materials Project DOI workflow.

    A thin wrapper over :class:`elinkapi.Elink` that holds the Materials Project
    conventions. It owns no state store: where minted DOIs are persisted is left to each pipeline.

    Attributes:
        _elink: The wrapped E-Link client, or ``None`` if not configured.
    """

    def __init__(self, elink: Elink | None = None) -> None:
        """Wrap an already-constructed E-Link client.

        Args:
            elink: The :class:`elinkapi.Elink` client to operate through. Prefer
                :meth:`for_review` / :meth:`for_production` unless you need to supply a
                custom client (e.g. in tests).
        """
        self._elink: Elink | None = elink

    @classmethod
    def for_review(cls, settings: ElinkSettings | None = None) -> "OstiClient":
        """Build a client whose E-Link side targets the review environment.

        Args:
            settings: Pre-loaded :class:`ElinkSettings` to read the review token and
                endpoint from. Defaults to a fresh :class:`ElinkSettings` loaded from
                the environment.

        Returns:
            An :class:`OstiClient` whose E-Link side is pointed at the review environment.
        """
        return cls._build(review=True, settings=settings)

    @classmethod
    def for_production(cls, settings: ElinkSettings | None = None) -> "OstiClient":
        """Build a client whose E-Link side targets the production environment.

        Args:
            settings: Pre-loaded :class:`ElinkSettings` to read the production token and
                endpoint from. Defaults to a fresh :class:`ElinkSettings` loaded from
                the environment.

        Returns:
            An :class:`OstiClient` whose E-Link side is pointed at production.
        """
        return cls._build(review=False, settings=settings)

    @classmethod
    def _build(cls, *, review: bool, settings: ElinkSettings | None) -> "OstiClient":
        """Assemble a client for the given environment.

        Shared by :meth:`for_review` and :meth:`for_production`.

        Args:
            review: Target the review environment when ``True``, else production.
            settings: E-Link settings, or ``None`` to load from the environment.

        Returns:
            The assembled :class:`OstiClient`.
        """
        return cls(get_elink_client(review=review, settings=settings))

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

    def find_or_create(self, record: Record, state: str = "submit") -> RecordResponse:
        """Return the existing record for ``record``'s ``site_unique_id``, or create it.

        Looks E-Link up by the record's ``site_unique_id`` and, only if nothing is there,
        submits the record. ``site_unique_id`` is expected to be namespaced per system
        (e.g. ``core-sub:{submission_uuid}``, ``contribs:{project_id}``, or a plain ``mp-id``)
        so lookups cannot collide across the systems sharing the OSTI account.

        Args:
            record: A fully constructed `elinkapi.Record` whose ``site_unique_id``
                is set. Submitted verbatim when no existing record is found.
            state: State to submit a newly created record in, forwarded to
                `submit_new_record`. Defaults to ``"submit"``; ignored when an
                existing record is found.

        Returns:
            The existing record if exactly one matches ``site_unique_id``, otherwise the
            response for the record just created.

        Raises:
            ValueError: If ``record.site_unique_id`` is unset, empty, or not a
                well-formed namespaced id (see :func:`mp_cite.site_id.validate_site_unique_id`).
            RuntimeError: If more than one record already matches ``site_unique_id`` (the
                namespacing scheme is meant to make this impossible, so it signals a data
                problem rather than a normal outcome).
        """
        site_unique_id = validate_site_unique_id(record.site_unique_id)

        query = self.query_records(site_unique_id=site_unique_id)
        if query.total_rows == 0:
            return self.submit_new_record(record, state=state)
        if query.total_rows == 1:
            return cast("list[RecordResponse]", query.data)[0]
        raise RuntimeError(
            f"{query.total_rows} records already match site_unique_id "
            f"{site_unique_id!r}; expected at most one. Resolve the duplicates in E-Link "
            "before minting."
        )

    def _raise_if_transient(self, osti_id: OstiID, status: str) -> None:
        """Reject an update when the record is mid-release.

        The pre-check shared by `update_record_fields` and `update_state`.
        A record may be revised from any settled state (draft, released, or a failed
        state), but not while OSTI is actively releasing it.

        Args:
            osti_id: OSTI ID of the record, for the error message.
            status: The record's current ``workflow_status``.

        Raises:
            RuntimeError: If ``status`` is a transient release state (``SR``/``SO``).
        """
        if status in _TRANSIENT_STATUSES:
            raise RuntimeError(
                f"Record {osti_id} is mid-release (workflow_status {status!r}); "
                "wait for it to settle (e.g. with wait_for_release) before revising."
            )

    def update_record_fields(
        self,
        osti_id: OstiID,
        new_values: dict[str, Any],
        *,
        state: Literal["save", "submit"] = "save",
        precheck: bool = True,
    ) -> RecordResponse:
        """Partially update an existing record, identified by its OSTI ID.

        With ``state="save"`` the revision is left as a draft; with ``state="submit"``
        it is sent on for release (required to make a revision of a released record go
        live again).

        Args:
            osti_id: OSTI ID of the record that ought to be updated.
            new_values: Mapping of E-Link record field names to their new
                (JSON-serializable) values. Only these fields are changed.
            state: Workflow state for the resulting revision — ``"save"`` (the default)
                to draft it, or ``"submit"`` to send it on for release.
            precheck: When ``True`` (the default), fetch the record first and refuse to
                patch it while it is mid-release (``workflow_status`` ``SR``/``SO``),
                where a revision would race OSTI's processing. Pass ``False`` to skip the
                extra fetch when the record's state is already known to be settled.

        Returns:
            The record response for the new revision.

        Raises:
            RuntimeError: If ``precheck`` is set and the record is mid-release.
        """
        if precheck:
            self._raise_if_transient(osti_id, self.get_record(osti_id).workflow_status)

        patch_operations = [
            {"op": "add", "path": f"/{field}", "value": value}
            for field, value in new_values.items()
        ]

        return self.elink.patch_json(osti_id, patch_operations, state=state)

    def update_state(
        self, osti_id: OstiID, new_state: Literal["save", "submit"]
    ) -> RecordResponse:
        """Update the state of a record to either ``"save"`` or ``"submit"``.

        Re-``submit`` of a released record produces a new revision, like
        `update_record_fields`. The same mid-release pre-check applies: the record
        it fetches is rejected when caught in a transient release state (``SR``/``SO``).

        Args:
            osti_id: OSTI ID of the record whose state should be updated.
            new_state: The new state, one of the two valid values ``"save"`` or
                ``"submit"``.

        Returns:
            The record response after updating the state.

        Raises:
            RuntimeError: If the record is mid-release when fetched.
        """
        record = self.get_record(osti_id)
        self._raise_if_transient(osti_id, record.workflow_status)
        return self.elink.update_record(osti_id, record, new_state)

    def wait_for_release(
        self, osti_id: OstiID, *, timeout: float = 300.0, interval: float = 10.0
    ) -> Record:
        """Poll a record until OSTI releases it, returning the released record.

        Polls `get_record` until the record's ``workflow_status`` reaches
        ``WorkflowStatus.Released``. A caller submits a record and then waits here before
        treating the DOI as live.

        Args:
            osti_id: OSTI ID of the record to wait on.
            timeout: Maximum number of seconds to wait before giving up. Defaults to
                ``300``.
            interval: Seconds to sleep between polls. Defaults to ``10``.

        Returns:
            The record once its ``workflow_status`` is ``WorkflowStatus.Released``.

        Raises:
            RuntimeError: If the record enters a terminal failure state
                 and so will never be released.
            TimeoutError: If the record is not released within ``timeout`` seconds.
        """
        import time

        # WorkflowStatus is a plain Enum (not str-based) while ``workflow_status`` is a
        # str, so compare against ``.value``, not the members themselves.
        deadline = time.monotonic() + timeout
        while True:
            record = self.get_record(osti_id)
            status = record.workflow_status
            if status == WorkflowStatus.Released.value:
                return record
            if status in {
                WorkflowStatus.FailedValidation.value,
                WorkflowStatus.FailedRelease.value,
            }:
                raise RuntimeError(
                    f"Record {osti_id} is in terminal failure state {status!r}; "
                    "it will not be released."
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Record {osti_id} was not released within {timeout}s "
                    f"(last workflow_status {status!r})."
                )
            time.sleep(interval)

    def delete_record(self, osti_id: OstiID, reason: str) -> None:
        """Delete a record by its OSTI ID.

        Args:
            osti_id: OSTI ID of the record that ought to be deleted.
            reason: Human-readable explanation of why the record is being deleted
                (required by E-Link and recorded in the record's history).
        """
        self.elink.delete_record(osti_id, reason)
