from typing import TYPE_CHECKING

# Map each public name to the submodule that defines it. Kept in sync with
# ``__all__``; ``_SUBMODULE_FOR`` is derived from it.
_NAMES_BY_SUBMODULE: dict[str, tuple[str, ...]] = {
    "mp_cite.core": ("OstiClient",),
    "mp_cite.models": (
        "MP_SPONSOR_NAME",
        "MP_SPONSOR_IDENTIFIERS",
        "MinimumDARecord",
        "ContribsDoiRequest",
        "validate_site_unique_id",
        "core_submission_site_id",
        "contribs_site_id",
        "material_site_id",
    ),
    "mp_cite.settings": (
        "get_elink_client",
        "ElinkSettings",
    ),
    "elinkapi": (
        "Person",
        "Organization",
        "Affiliation",
        "Identifier",
        "RelatedIdentifier",
    ),
}
_SUBMODULE_FOR: dict[str, str] = {
    name: module for module, names in _NAMES_BY_SUBMODULE.items() for name in names
}

__all__ = [
    "OstiClient",
    "MP_SPONSOR_NAME",
    "MP_SPONSOR_IDENTIFIERS",
    "MinimumDARecord",
    "ContribsDoiRequest",
    "get_elink_client",
    "ElinkSettings",
    "validate_site_unique_id",
    "core_submission_site_id",
    "contribs_site_id",
    "material_site_id",
    "Person",
    "Organization",
    "Affiliation",
    "Identifier",
    "RelatedIdentifier",
]
assert set(__all__) == set(_SUBMODULE_FOR), "__all__ out of sync with _SUBMODULE_FOR"


def __getattr__(name: str) -> object:
    """Import and cache a public name on first access (PEP 562)."""
    module_name = _SUBMODULE_FOR.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value  # cache so __getattr__ is not called again for `name`
    return value


def __dir__() -> list[str]:
    return sorted(__all__)


if TYPE_CHECKING:
    # Give type checkers and IDEs the real symbols; never executed at runtime.
    from elinkapi import (
        Affiliation,
        Identifier,
        Organization,
        Person,
        RelatedIdentifier,
    )

    from mp_cite.core import OstiClient
    from mp_cite.models import (
        MP_SPONSOR_IDENTIFIERS,
        MP_SPONSOR_NAME,
        ContribsDoiRequest,
        MinimumDARecord,
        contribs_site_id,
        core_submission_site_id,
        material_site_id,
        validate_site_unique_id,
    )
    from mp_cite.settings import (
        ElinkSettings,
        get_elink_client,
    )
