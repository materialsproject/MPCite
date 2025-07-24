import os
import pytest
from elinkapi import Elink, exceptions
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def elink_review_client():
    """
    tests whether or not the elink review client can be properly retrieved.
    returns the elink review client
    """
    elink_review_api_key = os.getenv("elink_review_api_token")
    review_endpoint = os.getenv("ELINK_REVIEW_ENDPOINT")
    return Elink(token=elink_review_api_key, target=review_endpoint)


@pytest.fixture
def elink_production_client():
    """
    tests whether or not the elink review client can be properly retrieved.
    returns the elink review client
    """
    elink_prod_api_key = os.getenv("elink_api_PRODUCTION_key")
    return Elink(token=elink_prod_api_key)


def test_get_single_record(elink_production_client):
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
