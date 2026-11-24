# MPCite

Materials Project wrapper around OSTI's [E-Link v2](https://www.osti.gov/elink2api/) API for minting and managing DOIs.

`OstiClient` is the single entry point. It handles both **Core** and **MPContribs** DOIs, wrapping [`elinkapi`](https://pypi.org/project/elinkapi/) with MP conventions and validation. It holds no state - persisting minted DOIs is each pipeline's job.

## Install

```bash
uv sync      # or: pip install -e .
```

## Configure

Credentials are read from the environment (or a `.env`), prefixed `ELINK_`:

| Variable | Purpose |
|---|---|
| `ELINK_API_TOKEN` | Production bearer token |
| `ELINK_REVIEW_API_TOKEN` | Review bearer token |
| `ELINK_ENDPOINT` / `ELINK_REVIEW_ENDPOINT` | Override endpoints (defaults point at OSTI) |

Build a client with `OstiClient.for_review()` or `OstiClient.for_production()`.

## Record templates

Both are `elinkapi.Record` subclasses with MP defaults baked in.

- **`MinimumDARecord`** - Core DOI. Only `title` is required; LBNL-MP org, DOE sponsor, Persson author, and the core landing page are defaulted.
- **`ContribsDoiRequest`** - MPContribs DOI. Caller supplies authors, funders, landing page, and description.

### `site_unique_id`

The idempotency key used by `find_or_create` to avoid duplicate DOIs. Namespaced so systems sharing the OSTI account can't collide. Build/validate via helpers:

| Helper | Format |
|---|---|
| `core_submission_site_id(uuid)` | `core-sub:<uuid>` |
| `contribs_site_id(project_id)` | `contribs:<slug>` |
| `material_site_id("mp-149")` | `mp-149` (bare, validated) |

Templates validate this field on assignment (Core accepts `core-sub:`/material, Contribs accepts `contribs:`).

## Usage

### Mint a Core DOI

```python
from mp_cite import OstiClient, MinimumDARecord

record = MinimumDARecord(title="Foo", site_unique_id="mp-149")
client = OstiClient.for_production()

client.submit_new_record(record, state="submit")  # live; use "save" to draft
```

### Mint an MPContribs DOI

```python
from mp_cite import OstiClient, ContribsDoiRequest, Person, Organization

record = ContribsDoiRequest(
    title="Foo",
    site_url="https://next-gen.materialsproject.org/contribs/projects/foo",
    persons=[Person(type="CONTACT", last_name="Foley")],
    organizations=[Organization(type="SPONSOR", name="LBNL")],
    description="FooBar",
)
client.submit_new_record(record, state="submit")
```

### Idempotent mint

Returns the existing record if one already matches `site_unique_id`, else submits. Raises if more than one matches.

```python
resp = client.find_or_create(record, state="submit")
```

### Update a DOI

Re-submitting a released record creates a new revision. `precheck` refuses to patch a record mid-release (`SR`/`SO`).

```python
record = client.query_records(site_unique_id="mp-149").data[0]
client.update_record_fields(
    record.osti_id,
    {"description": "Updated description"},
    state="submit",
    precheck=True,
)
```

### Other operations

| Method | Does |
|---|---|
| `get_record(osti_id)` | Fetch one record |
| `query_records(**params)` | Search E-Link (paginated iterable) |
| `update_state(osti_id, state)` | Flip `save`/`submit` without changing fields |
| `wait_for_release(osti_id)` | Poll until released; raises on terminal failure/timeout |
| `delete_record(osti_id, reason)` | Delete (reason required) |
| `.elink` | Escape hatch to the raw `elinkapi.Elink` client |

## Tests

```bash
uv run pytest
```
