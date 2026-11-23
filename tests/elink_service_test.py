from elinkapi.record import RecordResponse

from mp_cite import (
    ContribsDoiRequest,
    Organization,
    Person,
)

# TODO: Write tests that verify our usage of Elink is correct,
#       and make sure any upstream breaking changes get caught
#       here when version upgrades happen

# 1. general query logic + params that we use regularly?
# 2. make sure we can submit a correctly templated dataset submission
# 3. make sure record updates work
# 4. deleting records?
# 5+. test any other surfaces of the Elink api that we interact with


def test_elink_query(osti_review_client):
    # placeholder, just to verify gh actions until full test suite is done
    assert isinstance(next(osti_review_client.query_records()), RecordResponse)


def test_contribs_record_roundtrip(osti_review_client):
    """Save -> fetch -> re-save -> delete a contributed-dataset record in the review env.

    Uses state="save" (a draft) throughout so the test never actually releases a DOI.
    Skipped automatically when ELINK_REVIEW_API_TOKEN is unset (see conftest fixture).
    """
    request = ContribsDoiRequest(
        title="mp_cite integration test — contributed dataset",
        site_url="https://contribs.materialsproject.org/projects/mp_cite_test",
        description="Temporary record created by the mp_cite test suite.",
        persons=[Person(type="AUTHOR", first_name="Test", last_name="Author")],
        organizations=[Organization(type="SPONSOR", name="Test Non-DOE Sponsor")],
    )

    created = osti_review_client.submit_new_record(request, state="save")
    osti_id = created.osti_id
    try:
        fetched = osti_review_client.get_record(osti_id)
        assert fetched.osti_id == osti_id
        assert fetched.title == request.title

        resaved = osti_review_client.update_state(osti_id, "save")
        assert isinstance(resaved, RecordResponse)
    finally:
        # delete_record raises on failure; a plain return means the record is gone.
        osti_review_client.delete_record(osti_id, reason="mp_cite test cleanup")
