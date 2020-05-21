import pydantic
from pydantic import BaseModel, Field
from io import StringIO
from pybtex.database.input import bibtex
from materials_model import Material
from datetime import datetime

class DictAsMember(dict):
    # http://stackoverflow.com/questions/10761779/when-to-use-getattr/10761899#10761899
    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsMember(value)
        return value


class Connection(BaseModel):
    endpoint: str = Field(..., title="URL Endpoint of the connection")
    username: str = Field(..., title="User Name")
    password: str = Field(..., title="Password")


class OSTI(BaseModel):
    elink: Connection = Field(..., title="Elink endpoint")
    explorer: Connection = Field(..., title="Explorer endpoint")


class DOICollectionRecord(BaseModel):
    task_id: str = Field(...)
    doi: str = Field(default='')
    bibtex: str = Field(...)
    _status: str = Field(...)
    valid: bool = Field(False)

    def get_status(self):
        return self._status

    def set_status(self, status):
        self._status = status

    def get_osti_id(self):
        if self.doi is None or self.doi == '':
            return ''
        else:
            return self.doi.split('/')[-1]


class ELinkRecord(BaseModel):
    osti_id: str = Field(...)
    dataset_type: str = Field(default='SM')
    title: str = Field(...)
    creators: str = Field(default='Kristin Persson')
    product_nos: str = Field(..., title="MP id")
    accession_num: str = Field(..., title="MP id")
    contract_nos: str = Field('AC02-05CH11231; EDCBEE')
    originating_research_org: str = Field(
        default='Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)')
    publication_date: str = Field(...)
    language: str = Field(default='English')
    country: str = Field(default='USA')
    sponsor_org: str = Field(default='USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)')
    site_url: str = Field(...)
    contact_name: str = Field(default='Kristin Persson')
    contact_org: str = Field(default="LBNL")
    contact_email: str = Field(default='kapersson@lbl.gov')
    contact_phone: str = Field(default='+1(510)486-7218')
    related_resource: str = Field('https://materialsproject.org/citing')
    contributor_organizations: str = Field(default='MIT; UC Berkeley; Duke; U Louvain')
    subject_categories_code: str = Field(default='36 MATERIALS SCIENCE')
    keywords: str = Field(...)
    description: str = Field(default="")
    doi: dict = Field({}, title="DOI info", description="Mainly used during GET request")

    @classmethod
    def get_title(cls, material: Material):
        formula = material.formula_pretty
        return 'Materials Data on %s by Materials Project' % formula

    @classmethod
    def get_site_url(cls, mp_id):
        return 'https://materialsproject.org/materials/%s' % mp_id

    @classmethod
    def get_keywords(cls, material):
        # keywords = '; '.join(['crystal structure',
        #                       material.formula_pretty,
        #                       material.chemsys,
        #                       '; '.join(['-'.join(['ICSD', str(iid)]) for iid in material['icsd_ids']]),
        #                       ])
        keywords = '; '.join(['crystal structure', material.formula_pretty, material.chemsys])
        keywords += '; electronic bandstructure' if material.bandstructure is not None else ''
        return keywords

    @classmethod
    def get_default_description(cls):
        return 'Computed materials data using density ' \
               'functional theory calculations. These calculations determine '\
               'the electronic structure of bulk materials by solving '\
               'approximations to the Schrodinger equation. For more '\
               'information, see https://materialsproject.org/docs/calculations'

class RoboCrys(BaseModel):
    material_id: str
    last_updated: datetime
    # condensed_structure
    description: str
