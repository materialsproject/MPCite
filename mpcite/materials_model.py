from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Union
from enum import Enum
from datetime import datetime


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
    bs_task: str
    cbm: Union[str, None]
    dos_task: str
    efermi: float
    is_gap_direct: bool
    is_metal: bool
    uniform_task: str
    vbm: Union[str, None]


class Material(BaseModel):
    last_updated: datetime = Field(None, title="timestamp for the most recent calculation")
    created_at: datetime = Field(None, title="creation time for this material defined by when the first structure optimization calculation was run", )
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
