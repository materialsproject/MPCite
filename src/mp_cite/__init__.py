from elinkapi import (
    Affiliation,
    Identifier,
    Organization,
    Person,
    RelatedIdentifier,
)

from mp_cite.core import (
    delete_osti_record,
    find_out_of_date_doi_entries,
    submit_new_osti_record,
    update_existing_osti_record,
    update_state_of_osti_record,
)
from mp_cite.models import (
    MP_SPONSOR_IDENTIFIERS,
    MP_SPONSOR_NAME,
    ContribsDoiModel,
    ContribsDoiRequest,
    DOIModel,
    MinimumDARecord,
)
from mp_cite.settings import ElinkSettings, get_elink_client

__all__ = [
    # flow
    "submit_new_osti_record",
    "update_existing_osti_record",
    "update_state_of_osti_record",
    "delete_osti_record",
    "find_out_of_date_doi_entries",
    # core sponsor constants
    "MP_SPONSOR_NAME",
    "MP_SPONSOR_IDENTIFIERS",
    # templates / stored models
    "MinimumDARecord",
    "ContribsDoiRequest",
    "DOIModel",
    "ContribsDoiModel",
    # config
    "get_elink_client",
    "ElinkSettings",
    # re-exported elinkapi sub-objects
    "Person",
    "Organization",
    "Affiliation",
    "Identifier",
    "RelatedIdentifier",
]
