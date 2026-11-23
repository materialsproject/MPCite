import pytest

from mp_cite.settings import ElinkSettings, get_elink_client


@pytest.fixture
def elink_review_client():
    """Review-environment Elink client.

    Skips the dependent test when ``ELINK_REVIEW_API_TOKEN`` is unset so the offline
    suite (model + mongo tests) still runs without credentials.

    Returns:
        A :class:`elinkapi.Elink` client pointed at the review environment.
    """
    settings = ElinkSettings()
    if not settings.review_api_token:
        pytest.skip("ELINK_REVIEW_API_TOKEN not set; skipping live review-env test")
    return get_elink_client(review=True, settings=settings)
