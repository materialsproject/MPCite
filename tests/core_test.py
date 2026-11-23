"""Offline tests for core.py logic, using tiny in-memory fakes (no network dep)."""

from types import SimpleNamespace

import pytest

from mp_cite import OstiClient


class _FakeQuery:
    """Stand-in for elinkapi.query.Query: carries a row count and the current page."""

    def __init__(self, data):
        self.data = list(data)
        self.total_rows = len(self.data)


class _FakeElink:
    """Minimal fake of the elinkapi.Elink surface OstiClient calls."""

    def __init__(self, *, statuses=None, query_result=None, created=None):
        # wait_for_release: a queue of workflow_status values to hand back, in order.
        self._statuses = list(statuses or [])
        # find_or_create: the Query to return, and what post_new_record should return.
        self._query_result = query_result
        self._created = created
        self.post_calls = []
        self.query_calls = []

    def get_single_record(self, osti_id):
        status = self._statuses.pop(0)
        return SimpleNamespace(osti_id=osti_id, workflow_status=status)

    def query_records(self, **kwargs):
        self.query_calls.append(kwargs)
        return self._query_result

    def post_new_record(self, record, state):
        self.post_calls.append((record, state))
        return self._created


def test_wait_for_release_returns_when_released(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *_: None)
    elink = _FakeElink(statuses=["SV", "SR", "R"])
    osti = OstiClient(elink)

    record = osti.wait_for_release(999, interval=0.0)

    assert record.workflow_status == "R"
    assert record.osti_id == 999


@pytest.mark.parametrize("failure", ["SF", "SX"])
def test_wait_for_release_raises_on_terminal_failure(monkeypatch, failure):
    monkeypatch.setattr("time.sleep", lambda *_: None)
    elink = _FakeElink(statuses=["SV", failure])
    osti = OstiClient(elink)

    with pytest.raises(RuntimeError):
        osti.wait_for_release(999, interval=0.0)


def test_wait_for_release_times_out(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *_: None)
    # never reaches "R"; timeout=0 trips the deadline on the first poll
    elink = _FakeElink(statuses=["SV", "SV", "SV"])
    osti = OstiClient(elink)

    with pytest.raises(TimeoutError):
        osti.wait_for_release(999, timeout=0.0, interval=0.0)


def test_find_or_create_creates_when_absent():
    created = SimpleNamespace(osti_id=1, site_unique_id="contribs:proj_a")
    elink = _FakeElink(query_result=_FakeQuery([]), created=created)
    osti = OstiClient(elink)
    record = SimpleNamespace(site_unique_id="contribs:proj_a")

    result = osti.find_or_create(record, state="save")

    assert result is created
    assert elink.post_calls == [(record, "save")]
    assert elink.query_calls == [{"site_unique_id": "contribs:proj_a"}]


def test_find_or_create_returns_existing_without_posting():
    existing = SimpleNamespace(osti_id=42, site_unique_id="contribs:proj_a")
    elink = _FakeElink(query_result=_FakeQuery([existing]))
    osti = OstiClient(elink)
    record = SimpleNamespace(site_unique_id="contribs:proj_a")

    result = osti.find_or_create(record)

    assert result is existing
    assert elink.post_calls == []  # no write when a record already exists


def test_find_or_create_raises_on_duplicate_matches():
    dupes = [SimpleNamespace(osti_id=1), SimpleNamespace(osti_id=2)]
    elink = _FakeElink(query_result=_FakeQuery(dupes))
    osti = OstiClient(elink)
    record = SimpleNamespace(site_unique_id="contribs:proj_a")

    with pytest.raises(RuntimeError):
        osti.find_or_create(record)
    assert elink.post_calls == []


def test_find_or_create_requires_site_unique_id():
    elink = _FakeElink(query_result=_FakeQuery([]))
    osti = OstiClient(elink)
    record = SimpleNamespace(site_unique_id=None)

    with pytest.raises(ValueError):
        osti.find_or_create(record)
    assert elink.query_calls == []  # fails before any lookup
