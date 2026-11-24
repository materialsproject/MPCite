from typing import ClassVar

from elinkapi import Elink
from pydantic_settings import BaseSettings, SettingsConfigDict

PROD_ENDPOINT = "https://www.osti.gov/elink2api/"
REVIEW_ENDPOINT = "https://review.osti.gov/elink2api/"


class ElinkSettings(BaseSettings):
    """E-Link connection settings sourced from the environment.

    Values are read from environment variables prefixed with ``ELINK_``.

    Attributes:
        review_api_token: Bearer token for the E-Link review environment, from
            ``ELINK_REVIEW_API_TOKEN``. ``None`` when unset.
        review_endpoint: Base URL of the review environment, from
            ``ELINK_REVIEW_ENDPOINT``. Defaults to :data:`REVIEW_ENDPOINT`.
        api_token: Bearer token for the E-Link production environment, from
            ``ELINK_API_TOKEN``. ``None`` when unset.
        endpoint: Base URL of the production environment, from ``ELINK_ENDPOINT``.
            Defaults to :data:`PROD_ENDPOINT`.
    """

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="ELINK_",
        env_file=".env",
        extra="ignore",
    )

    review_api_token: str | None = None
    review_endpoint: str = REVIEW_ENDPOINT
    api_token: str | None = None
    endpoint: str = PROD_ENDPOINT


def get_elink_client(
    review: bool = True, settings: ElinkSettings | None = None
) -> Elink:
    """Build an :class:`elinkapi.Elink` client for the review or production environment.

    Args:
        review: When ``True`` (the default, to avoid accidental writes against
            production), target the review environment; when ``False``, target
            production.
        settings: Pre-loaded :class:`ElinkSettings` to read the token and endpoint
            from. Defaults to a fresh :class:`ElinkSettings` loaded from the
            environment.

    Returns:
        A configured :class:`elinkapi.Elink` client. The token may be ``None`` when
        the corresponding environment variable is unset; E-Link calls will then fail
        with an auth error, which surfaces the missing-configuration problem clearly.
    """
    settings = settings or ElinkSettings()

    if review:
        return Elink(token=settings.review_api_token, target=settings.review_endpoint)

    return Elink(token=settings.api_token, target=settings.endpoint)
