"""Offline tests for core.py mongo logic, using a tiny in-memory fake (no mongomock dep)."""

from datetime import datetime

from mp_cite import OstiClient


class _FakeCollection:
    def __init__(self, docs):
        self._docs = docs

    @staticmethod
    def _matches(doc, filt):
        for key, cond in filt.items():
            val = doc.get(key)
            if isinstance(cond, dict):
                if "$gt" in cond and not (val is not None and val > cond["$gt"]):
                    return False
                if "$in" in cond and val not in cond["$in"]:
                    return False
            elif val != cond:
                return False
        return True

    def find(self, filt=None, projection=None):
        filt = filt or {}
        return (doc for doc in self._docs if self._matches(doc, filt))

    def aggregate(self, pipeline):
        docs = list(self._docs)
        for stage in pipeline:
            if "$project" in stage:
                fields = [k for k, keep in stage["$project"].items() if keep == 1]
                docs = [{k: d.get(k) for k in fields} for d in docs]
            elif "$sort" in stage:
                ((field, direction),) = stage["$sort"].items()
                docs.sort(key=lambda d: d.get(field), reverse=(direction == -1))
            elif "$limit" in stage:
                docs = docs[: stage["$limit"]]
        return iter(docs)


class _FakeDB:
    def __init__(self, collections):
        self._collections = collections

    def __getitem__(self, name):
        return _FakeCollection(self._collections[name])


class _FakeClient:
    def __init__(self, databases):
        self._databases = databases

    def __getitem__(self, name):
        return _FakeDB(self._databases[name])


def test_find_out_of_date_doi_entries_core_defaults():
    client = _FakeClient(
        {
            "mp_db": {
                "robocrys": [
                    {"material_id": "mp-1", "last_updated": datetime(2026, 2, 1)},
                    {"material_id": "mp-2", "last_updated": datetime(2026, 1, 1)},
                ],
                "dois": [
                    {
                        "material_id": "mp-1",
                        "osti_id": 101,
                        "date_metadata_updated": datetime(2026, 1, 10),
                    },
                    {
                        "material_id": "mp-2",
                        "osti_id": 102,
                        "date_metadata_updated": datetime(2026, 1, 5),
                    },
                ],
            }
        }
    )

    osti = OstiClient(mongo=client, db="mp_db")
    result = osti.find_out_of_date_doi_entries("robocrys", "dois")

    # only mp-1 was updated after the latest DOI metadata date (2026-01-10)
    assert result == [101]


def test_find_out_of_date_doi_entries_contribs_kwargs():
    client = _FakeClient(
        {
            "contribs_db": {
                "projects": [
                    {"identifier": "proj_a", "updated_at": datetime(2026, 4, 1)},
                    {"identifier": "proj_b", "updated_at": datetime(2026, 2, 1)},
                ],
                "contribs_dois": [
                    {
                        "identifier": "proj_a",
                        "osti_id": 201,
                        "date_metadata_updated": datetime(2026, 3, 1),
                    },
                    {
                        "identifier": "proj_b",
                        "osti_id": 202,
                        "date_metadata_updated": datetime(2026, 3, 2),
                    },
                ],
            }
        }
    )

    osti = OstiClient(mongo=client, db="contribs_db")
    result = osti.find_out_of_date_doi_entries(
        "projects",
        "contribs_dois",
        source_key="identifier",
        source_timestamp="updated_at",
        doi_match_key="identifier",
    )

    # latest contribs DOI metadata date is 2026-03-02; only proj_a updated after it
    assert result == [201]
