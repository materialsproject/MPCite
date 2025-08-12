from elinkapi import exceptions
from elinkapi.record import RecordResponse
import pytest
from dotenv import load_dotenv

from tests.conf_test import elink_review_client

load_dotenv()

# TODO: Write tests that verify our usage of Elink is correct,
#       and make sure any upstream breaking changes get caught
#       here when version upgrades happen


# 1. general query logic + params that we use regularly?
# 2. make sure we can submit a correctly templated dataset submission
# 3. make sure record updates work
# 4. deleting records?
# 5+. test any other surfaces of the Elink api that we interact with


def test_get_single_record(elink_production_client):
    """
    tried to use the production client to retrieve a record.
    """
    try:
        record = elink_production_client.get_single_record(1190959)
        assert record.title == "Materials Data on Si by Materials Project"
        assert record.osti_id == 1190959
    except exceptions.ForbiddenException as fe:
        pytest.fail(
            f"Forbidden: Check API key or permissions associated with provided API key. {fe}"
        )
    except exceptions.BadRequestException as ve:
        pytest.fail(f"Bad Request: Possibly incorrect parameters. {ve}")
    except Exception as e:
        pytest.fail(f"Unexpected error: {e}")


def test_query_records(elink_production_client):
    """
    tests the query functionality of the elinkapi on the production environment
    """
    try:
        elink_production_client.query_records()
    except exceptions.ForbiddenException as fe:
        pytest.fail(
            f"Forbidden: Check API key or permissions associated with provided API key. {fe}"
        )
    except exceptions.BadRequestException as ve:
        pytest.fail(f"Bad Request: Possibly incorrect parameters. {ve}")
    except Exception as e:
        pytest.fail(f"Unexpected error: {e}")


def test_query_exists(elink_review_client):
    """
    tests to see that the query does in fact resolve entries in the form of RecordResponse objects.
    """
    assert isinstance(next(elink_review_client.query_records()), RecordResponse)
