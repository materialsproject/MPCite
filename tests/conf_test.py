import os
import pytest
from elinkapi import Elink
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
