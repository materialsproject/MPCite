import pytest

from mp_cite import OstiClient
from mp_cite.settings import ElinkSettings, get_elink_client


@pytest.fixture
def elink_review_client():
    """Raw review-environment Elink client.

    Skips the dependent test when ``ELINK_REVIEW_API_TOKEN`` is unset so the offline
    suite (model + core-logic tests) still runs without credentials.

    Returns:
        A :class:`elinkapi.Elink` client pointed at the review environment.
    """
    settings = ElinkSettings()
    if not settings.review_api_token:
        pytest.skip("ELINK_REVIEW_API_TOKEN not set; skipping live review-env test")
    return get_elink_client(review=True, settings=settings)


@pytest.fixture
def osti_review_client(elink_review_client):
    """Review-environment :class:`OstiClient` wrapping the raw review client.

    Shares the raw ``elink_review_client`` instance (and its skip-when-unconfigured
    behaviour), so live tests exercise the same client the wrapper would build.

    Returns:
        An :class:`OstiClient` pointed at the review environment.
    """
    return OstiClient(elink_review_client)
