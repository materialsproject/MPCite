import requests
import json
from datetime import datetime

from models import ElsevierPOSTContainerModel

elvisier = ElsevierPOSTContainerModel(identifier="mp-10231",
                                      title="LiMnSe2",
                                      doi="10.17188/1186837",
                                      url="https://materialsproject.org/materials/mp-10074",
                                      keywords=['crystal structure'],
                                      date="2011-05-28",
                                      dateCreated="2011-05-28",
                                      dateAvailable="2020-05-04")

# print(datetime.now().isoformat().__str__())
print("********** data being posted ***************")

data = {'identifier': 'mp-1218145', 'source': 'MATERIALS_PROJECT', 'date': '2020-06-05', 'title': 'SrLaTaNiO6', 'description': 'SrLaTaNiO6 is (Cubic) Perovskite-derived structured and crystallizes in the cubic F-43m space group. The structure is three-dimensional. Sr2+ is bonded to twelve equivalent O2- atoms to form SrO12 cuboctahedra that share corners with twelve equivalent SrO12 cuboctahedra, faces with six equivalent LaO12 cuboctahedra, faces with four equivalent TaO6 octahedra, and faces with four equivalent NiO6 octahedra. All Sr–O bond lengths are 2.83 Å. La3+ is bonded to twelve equivalent O2- atoms to form LaO12 cuboctahedra that share corners with twelve equivalent LaO12 cuboctahedra, faces with six equivalent SrO12 cuboctahedra, faces with four equivalent TaO6 octahedra, and faces with four equivalent NiO6 octahedra. All La–O bond lengths are 2.83 Å. Ta5+ is bonded to six equivalent O2- atoms to form TaO6 octahedra that share corners with six equivalent NiO6 octahedra, faces with four equivalent SrO12 cuboctahedra, and faces with four equivalent LaO12 cuboctahedra. The corner-sharing octahedral tilt angles are 0°. All Ta–O bond lengths are 1.98 Å. Ni2+ is bonded to six equivalent O2- atoms to form NiO6 octahedra that share corners with six equivalent TaO6 octahedra, faces with four equivalent SrO12 cuboctahedra, and faces with four equivalent LaO12 cuboctahedra. The corner-sharing octahedral tilt angles are 0°. All Ni–O bond lengths are 2.03 Å. O2- is bonded in a distorted linear geometry to two equivalent Sr2+, two equivalent La3+, one Ta5+, and one Ni2+ atom.', 'doi': '10.80460/1480077', 'authors': ['Kristin Persson'], 'url': 'https://materialsproject.org/materials/mp-1218145', 'type': 'dataset', 'dateAvailable': '2019-01-12', 'dateCreated': '2019-01-12', 'version': '1.0.0', 'funding': 'USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)', 'language': 'en', 'method': 'Materials Project', 'accessRights': 'Public', 'contact': 'Kristin Persson <kapersson@lbl.gov>', 'dataStandard': 'https://materialsproject.org/citing', 'howToCite': 'https://materialsproject.org/citing', 'subjectAreas': ['36 MATERIALS SCIENCE'], 'keywords': ['crystal structure', 'SrLaTaNiO6', 'La-Ni-O-Sr-Ta', 'electronic bandstructure'], 'institutions': ['Lawrence Berkeley National Laboratory'], 'institutionIds': ['AC02-05CH11231; EDCBEE'], 'spatialCoverage': [], 'temporalCoverage': [], 'references': ['https://materialsproject.org/citing'], 'relatedResources': ['https://materialsproject.org/citing'], 'location': '1 Cyclotron Rd, Berkeley, CA 94720', 'childContainerIds': []}

print(json.dumps(data, indent=2))
headers = {"x-api-key": "gopvu7IDTs7zhBs5w5Ss11V7WiWyYOm44YfdggP4"}
url = "https://push-feature.datasearch.elsevier.com/container"
r = requests.post(url=url, data=data, headers=headers)

print("********** response returned posted ***************")
print(r)
print(r.content)
