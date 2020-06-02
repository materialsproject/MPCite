from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Union
from enum import Enum
from datetime import datetime
from enum import Enum


class ConnectionModel(BaseModel):
    endpoint: str = Field(..., title="URL Endpoint of the connection")
    username: str = Field(..., title="User Name")
    password: str = Field(..., title="Password")


class OSTIModel(BaseModel):
    elink: ConnectionModel = Field(..., title="Elink endpoint")
    explorer: ConnectionModel = Field(..., title="Explorer endpoint")


class RoboCrysModel(BaseModel):
    material_id: str
    last_updated: datetime
    # condensed_structure
    description: str


class Lattice(BaseModel):
    a: float = Field(..., title="*a* lattice parameter")
    alpha: int = Field(..., title="Angle between a and b lattice vectors")
    b: float = Field(..., title="b lattice parameter")
    beta: int = Field(..., title="angle between a and c lattice vectors")
    c: float = Field(..., title="c lattice parameter")
    gamma: int = Field(..., title="angle between b and c lattice vectors")
    volume: float = Field(..., title="lattice volume")
    matrix: List[List[int]] = Field(..., title="matrix representation of this lattice")


class Specie(BaseModel):
    element: str = Field(..., title="element")
    occu: float = Field(..., title="site occupancy")


class Site(BaseModel):
    abc: List[float] = Field(..., title="fractional coordinates")
    label: str = None
    species: List[Specie] = Field(..., title="species occupying this site")
    xyz: List[float] = Field(..., title="cartesian coordinates")
    properties: Dict[str, int] = Field(..., title="arbitrary property list")


class Structure(BaseModel):
    charge: Optional[float] = Field(None, title="site wide charge")
    lattice: Lattice
    sites: List[Site]


class CrystalSystem(str, Enum):
    tetragonal = "tetragonal"
    triclinic = "triclinic"
    orthorhombic = "orthorhombic"
    monoclinic = "monoclinic"
    hexagonal = "hexagonal"
    cubic = "cubic"
    trigonal = "trigonal"


class Symmetry(BaseModel):
    source: str
    symbol: str
    number: int
    point_group: str
    crystal_system: CrystalSystem
    hall: str


class Origin(BaseModel):
    materials_key: str
    task_type: str
    task_id: str
    last_updated: datetime


class BandStructure(BaseModel):
    band_gap: float
    bs_task: Union[str, None]
    cbm: Union[str, None]
    dos_task: Union[str, None]
    efermi: float
    is_gap_direct: bool
    is_metal: bool
    uniform_task: Union[str, None]
    vbm: Union[str, None]


class MaterialModel(BaseModel):
    last_updated: datetime = Field(None, title="timestamp for the most recent calculation")
    created_at: datetime = Field(None,
                                 title="creation time for this material defined by when the first structure optimization calculation was run", )
    task_ids: List[str] = Field([], title="List of task ids that created this material")
    task_id: str = Field('', title="task id for this material. Also called the material id")
    origins: List[Origin]
    task_types: Dict[str, str] = Field(dict())
    bandstructure: Union[BandStructure, None] = Field(None)
    energy: float
    energy_per_atom: float
    # entries
    # initial_structures
    # inputs
    # magnetism
    structure: Structure = Field(..., title="the structure object")
    nsites: int
    elements: List[str] = Field(..., title="list of elements")
    nelements: int = Field(..., title="number of elements")
    composition: Dict[str, int] = Field(
        dict(), title="composition as a dictionary of elements and their amount"
    )
    composition_reduced: Dict[str, int] = Field(
        dict(), title="reduced composition as a dictionary of elements and their amount"
    )
    formula_pretty: str = Field(..., title="clean representation of the formula")
    formula_anonymous: str = Field(..., title="formula using anonymized elements")
    chemsys: str = Field('',
                         title="chemical system as a string of elements in alphabetical order delineated by dashes", )
    volume: float = Field(..., title="")
    density: float = Field(..., title="mass density")
    symmetry: Symmetry = Field(..., title="symmetry data for this")


class ELinkGetResponseModel(BaseModel):
    osti_id: Union[str, None] = Field(...)
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
    country: str = Field(default='US')
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
    def get_title(cls, material: MaterialModel):
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
               'functional theory calculations. These calculations determine ' \
               'the electronic structure of bulk materials by solving ' \
               'approximations to the Schrodinger equation. For more ' \
               'information, see https://materialsproject.org/docs/calculations'

    @classmethod
    def custom_to_dict(cls, elink_record) -> dict:
        if elink_record.osti_id is None or elink_record.osti_id == '':
            return elink_record.dict(exclude={"osti_id", "doi"})
        else:
            return elink_record.dict(exclude={"doi"})


class ElinkResponseStatusEnum(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILURE"


class ELinkPostResponseModel(BaseModel):
    osti_id: str
    accession_num: str
    product_nos: str
    title: str
    contract_nos: str
    other_identifying_nos: Union[str, None]
    doi: Dict[str, str]
    status: ElinkResponseStatusEnum
    status_message: Union[str, None]


class DOIRecordModel(BaseModel):
    material_id: str = Field(...)
    doi: str = Field(default='')
    bibtex: Union[str, None] = Field(...)
    status: str = Field(...)
    valid: bool = Field(False)
    last_updated: datetime = Field(default=datetime.now())

    def get_status(self):
        return self.status

    def set_status(self, status):
        self.status = status

    def get_osti_id(self):
        if self.doi is None or self.doi == '':
            return ''
        else:
            return self.doi.split('/')[-1]

    @classmethod
    def from_elink_response_record(cls, elink_response_record: ELinkPostResponseModel):
        doi_collection_record = DOIRecordModel(material_id=elink_response_record.accession_num,
                                               doi=elink_response_record.doi["#text"],
                                               status=elink_response_record.doi["@status"],
                                               bibtex=None,
                                               valid=True)
        doi_collection_record.set_status(status=elink_response_record.doi["@status"])
        return doi_collection_record


class ElsevierPOSTContainerModel(BaseModel):
    identifier: str = Field(default="", title="mp_id")
    source: str = Field(default="https://materialsproject.org")
    date: str = Field(default=datetime.now().date().isoformat().__str__())
    title: str = Field(...)
    description: str = Field(default="")
    doi: str = Field(...)
    authors: List[str] = Field(default=['Kristin Persson'])
    url: str = Field(...)
    type: str = Field(default='dataset')
    dateAvailable: str = Field(default= datetime.now().date().isoformat().__str__())
    dateCreated: str = Field(default=datetime.now().date().isoformat().__str__())
    version: str = Field(default="0.0.1")
    funding: str = Field(default='USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)')
    language: str = Field(default="en")
    method: str = Field(default="Materials Project")
    accessRights: str = Field(default="Public")
    contact: str = Field('Kristin Persson <kapersson@lbl.gov>')
    dataStandard: str = Field(default="https://materialsproject.org/citing")
    howToCite: str = Field(default="https://materialsproject.org/citing")
    subjectAreas: List[str] = Field(default=["36 MATERIALS SCIENCE"])
    keywords: List[str] = Field(...)
    institutions: List[str] = Field(default=["Lawrence Berkeley National Laboratory"])
    institutionIds: List[str] = Field(default=["AC02-05CH11231; EDCBEE"])
    spatialCoverage: List[str] = Field(default=[])
    temporalCoverage: List[str] = Field(default=[])
    references: List[str] = Field(default=["https://materialsproject.org/citing"])
    relatedResources: List[str] = Field(default=["https://materialsproject.org/citing"])
    location: str = Field("1 Cyclotron Rd, Berkeley, CA 94720")
    childContainerIds: List[str] = Field(default=[])

    @classmethod
    def get_url(cls, mp_id):
        return 'https://materialsproject.org/materials/%s' % mp_id

    @classmethod
    def get_keywords(cls, material:MaterialModel):
        if material.bandstructure is not None:
            return ['crystal structure', material.formula_pretty, material.chemsys, 'electronic bandstructure']
        else:
            return ['crystal structure', material.formula_pretty, material.chemsys]

    @classmethod
    def get_default_description(cls):
        return 'Computed materials data using density ' \
               'functional theory calculations. These calculations determine ' \
               'the electronic structure of bulk materials by solving ' \
               'approximations to the Schrodinger equation. For more ' \
               'information, see https://materialsproject.org/docs/calculations'

    @classmethod
    def get_date_created(cls, material: MaterialModel) -> str:
        return material.created_at.__str__()

    @classmethod
    def get_date_available(cls, material: MaterialModel) -> str:
        return material.created_at.__str__()

    @classmethod
    def get_title(cls, material: MaterialModel) -> str:
        return material.formula_pretty


class ExplorerGetJSONResponseModel(BaseModel):
    osti_id: str
    title: str
    report_number: str
    doi: str
    product_type: str
    language: str
    country_publication: str
    description: str
    site_ownership_code: str
    publication_date: str
    entry_date: str
    contributing_organizations: str
    authors: List[str]
    subjects: List[str]
    contributing_org: str
    doe_contract_number: str
    sponsor_orgs: List[str]
    research_orgs: List[str]
    links: List[Dict[str, str]]



