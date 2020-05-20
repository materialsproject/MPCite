import pydantic
from pydantic import BaseModel, Field
from io import StringIO
from pybtex.database.input import bibtex


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


class Record(BaseModel):
    osti_id: str = Field(...)
    dataset_type: str = Field(default='SM')
    title: str = Field(...)
    creators: str = Field(default='Kristin Persson')
    product_nos: str = Field(..., title="MP id")
    accession_num: str = Field(...)
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
    description: str = Field(default='Computed materials data using density '
                                     'functional theory calculations. These calculations determine '
                                     'the electronic structure of bulk materials by solving '
                                     'approximations to the Schrodinger equation. For more '
                                     'information, see https://materialsproject.org/docs/calculations')

    def _get_title(self, material):
        formula = material['pretty_formula']
        sg_num = material['spacegroup']['number']
        return 'Materials Data on %s (SG:%d) by Materials Project' % (
            formula, sg_num
        )

    def _get_creators(self, material):
        creators = []
        for author in material['snl_final']['about']['authors']:
            names = author['name'].split()
            last_name = names[-1]
            first_name = ' '.join(names[:-1])
            creators.append(', '.join([last_name, first_name]))
        return '; '.join(creators)

    def _get_site_url(self, mp_id):
        return 'https://materialsproject.org/materials/%s' % mp_id

    def _get_related_resource(self, material):
        bibtext_parser = bibtex.Parser()
        bib_data = bibtext_parser.parse_stream(StringIO(material['snl_final']['about']['references']))
        related_resource = []
        for entry in bib_data.entries.values():
            related_resource.append(entry.fields.get('url'))
        return ', '.join(filter(None, related_resource))

    def _get_keywords(self, material):
        keywords = '; '.join(['crystal structure',
                              material['snl_final']['reduced_cell_formula_abc'],
                              material['snl_final']['chemsystem'],
                              '; '.join(['-'.join(['ICSD', str(iid)]) for iid in material['icsd_ids']]),
                              ])
        keywords += '; electronic bandstructure' if material['has_bandstructure'] else ''
        return keywords
