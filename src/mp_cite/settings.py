from typing import TYPE_CHECKING, ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from elinkapi import Elink
    from pymongo import MongoClient

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
) -> "Elink":
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
    from elinkapi import Elink

    settings = settings or ElinkSettings()

    if review:
        return Elink(token=settings.review_api_token, target=settings.review_endpoint)

    return Elink(token=settings.api_token, target=settings.endpoint)


class MongoSettings(BaseSettings):
    """MongoDB connection settings for the DOI store, sourced from the environment.

    Values are read from environment variables prefixed with ``MONGO_``.

    Attributes:
        uri: Connection string for the MongoDB deployment, from ``MONGO_URI`` (any
            credentials are embedded here). Defaults to a local server.
        database: Name of the database holding the source and DOI collections, from
            ``MONGO_DATABASE``. ``None`` when unset, which surfaces as a clear error
            when a store operation is attempted without it.
    """

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="MONGO_",
        env_file=".env",
        extra="ignore",
    )

    uri: str = "mongodb://localhost:27017"
    database: str | None = None


def get_mongo_client(settings: MongoSettings | None = None) -> "MongoClient":
    """Build a :class:`pymongo.MongoClient` for the DOI store.

    Args:
        settings: Pre-loaded :class:`MongoSettings` to read the connection URI from.
            Defaults to a fresh :class:`MongoSettings` loaded from the environment.

    Returns:
        A configured :class:`pymongo.MongoClient`. The database name is carried on the
        settings (``database``), not the client, and is supplied to
        :class:`~mp_cite.core.OstiClient` separately.
    """
    from pymongo import MongoClient

    settings = settings or MongoSettings()
    return MongoClient(settings.uri)
