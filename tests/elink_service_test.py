from elinkapi.record import RecordResponse

# TODO: Write tests that verify our usage of Elink is correct,
#       and make sure any upstream breaking changes get caught
#       here when version upgrades happen

# 1. general query logic + params that we use regularly?
# 2. make sure we can submit a correctly templated dataset submission
# 3. make sure record updates work
# 4. deleting records?
# 5+. test any other surfaces of the Elink api that we interact with


def test_elink_query(elink_review_client):
    # placeholder, just to verify gh actions until full test suite is done
    assert isinstance(next(elink_review_client.query_records()), RecordResponse)
