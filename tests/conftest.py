import os

import pytest
from elinkapi import Elink


@pytest.fixture
def elink_review_client():
    review_endpoint = os.getenv("ELINK_REVIEW_ENDPOINT")
    elink_review_api_key = os.getenv("ELINK_REVIEW_API_TOKEN")
    return Elink(token=elink_review_api_key, target=review_endpoint)
