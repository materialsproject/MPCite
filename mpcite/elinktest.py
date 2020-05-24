import requests
import json
from xmltodict import parse
from xml.dom.minidom import parseString
import logging

elink_endpoint = "https://www.osti.gov/elinktest/2416api"
username = "materials2416websvs"
password = "Sti!2416sub"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


data = b'<?xml version="1.0" ?><records><record><dataset_type>SM</dataset_type><title>Materials Data on Mg(SbO2)2 by Materials Project</title><creators>Kristin Persson</creators><product_nos>mp-1388665</product_nos><accession_num>mp-1388665</accession_num><contract_nos>AC02-05CH11231; EDCBEE</contract_nos><originating_research_org>Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)</originating_research_org><publication_date>05/02/2020</publication_date><language>English</language><country>US</country><sponsor_org>USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)</sponsor_org><site_url>https://materialsproject.org/materials/mp-1388665</site_url><contact_name>Kristin Persson</contact_name><contact_org>LBNL</contact_org><contact_email>kapersson@lbl.gov</contact_email><contact_phone>+1(510)486-7218</contact_phone><related_resource>https://materialsproject.org/citing</related_resource><contributor_organizations>MIT; UC Berkeley; Duke; U Louvain</contributor_organizations><subject_categories_code>36 MATERIALS SCIENCE</subject_categories_code><keywords>crystal structure; Mg(SbO2)2; Mg-O-Sb; electronic bandstructure</keywords><description>Computed materials data using density functional theory calculations. These calculations determine the electronic structure of bulk materials by solving approximations to the Schrodinger equation. For more information, see https://materialsproject.org/docs/calculations</description></record></records>'
data = b'<?xml version="1.0" ?><records><dataset_type>SM</dataset_type><title>Materials Data on RbLi2V2(BO3)3 by Materials Project</title><creators>Kristin Persson</creators><product_nos>mp-772424</product_nos><accession_num>mp-772424</accession_num><contract_nos>AC02-05CH11231; EDCBEE</contract_nos><originating_research_org>Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)</originating_research_org><publication_date>05/02/2020</publication_date><language>English</language><country>US</country><sponsor_org>USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)</sponsor_org><site_url>https://materialsproject.org/materials/mp-772424</site_url><contact_name>Kristin Persson</contact_name><contact_org>LBNL</contact_org><contact_email>kapersson@lbl.gov</contact_email><contact_phone>+1(510)486-7218</contact_phone><related_resource>https://materialsproject.org/citing</related_resource><contributor_organizations>MIT; UC Berkeley; Duke; U Louvain</contributor_organizations><subject_categories_code>36 MATERIALS SCIENCE</subject_categories_code><keywords>crystal structure; RbLi2V2(BO3)3; B-Li-O-Rb-V; electronic bandstructure</keywords><description>RbLi2V2(BO3)3 crystallizes in the monoclinic P2/c space group. The structure is three-dimensional. Rb1+ is bonded to twelve O2- atoms to form distorted RbO12 cuboctahedra that share corners with two equivalent RbO12 cuboctahedra, corners with two equivalent VO4 tetrahedra, edges with two equivalent RbO12 cuboctahedra, edges with two equivalent LiO4 tetrahedra, edges with four equivalent VO4 tetrahedra, and faces with two equivalent LiO4 tetrahedra. There are a spread of Rb\xe2\x80\x93O bond distances ranging from 3.03\xe2\x80\x933.57 \xc3\x85. Li1+ is bonded to four O2- atoms to form LiO4 tetrahedra that share  a cornercorner with one LiO4 tetrahedra, corners with three equivalent VO4 tetrahedra,  an edgeedge with one RbO12 cuboctahedra, and  a faceface with one RbO12 cuboctahedra. There are a spread of Li\xe2\x80\x93O bond distances ranging from 1.92\xe2\x80\x932.03 \xc3\x85. V3+ is bonded to four O2- atoms to form VO4 tetrahedra that share  a cornercorner with one RbO12 cuboctahedra, corners with three equivalent LiO4 tetrahedra, and edges with two equivalent RbO12 cuboctahedra. There are a spread of V\xe2\x80\x93O bond distances ranging from 1.92\xe2\x80\x931.96 \xc3\x85. There are two inequivalent B3+ sites. In the first B3+ site, B3+ is bonded in a trigonal planar geometry to three O2- atoms. All B\xe2\x80\x93O bond lengths are 1.39 \xc3\x85. In the second B3+ site, B3+ is bonded in a trigonal planar geometry to three O2- atoms. There is one shorter (1.35 \xc3\x85) and two longer (1.41 \xc3\x85) B\xe2\x80\x93O bond length. There are five inequivalent O2- sites. In the first O2- site, O2- is bonded in a distorted bent 120 degrees geometry to two equivalent Rb1+, one V3+, and one B3+ atom. In the second O2- site, O2- is bonded in a 3-coordinate geometry to one Rb1+, one Li1+, one V3+, and one B3+ atom. In the third O2- site, O2- is bonded in a trigonal planar geometry to one Rb1+, one Li1+, one V3+, and one B3+ atom. In the fourth O2- site, O2- is bonded in a distorted trigonal planar geometry to one Rb1+, one Li1+, one V3+, and one B3+ atom. In the fifth O2- site, O2- is bonded in a distorted trigonal planar geometry to two equivalent Rb1+, two equivalent Li1+, and one B3+ atom.</description></records>'
data = b'<?xml version="1.0" ?><records><record><dataset_type>SM</dataset_type><title>Materials Data on CaY5(MoO6)2 by Materials Project</title><creators>Kristin Persson</creators><product_nos>mp-1234100</product_nos><accession_num>mp-1234100</accession_num><contract_nos>AC02-05CH11231; EDCBEE</contract_nos><originating_research_org>Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)</originating_research_org><publication_date>03/27/2019</publication_date><language>English</language><country>US</country><sponsor_org>USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)</sponsor_org><site_url>https://materialsproject.org/materials/mp-1234100</site_url><contact_name>Kristin Persson</contact_name><contact_org>LBNL</contact_org><contact_email>kapersson@lbl.gov</contact_email><contact_phone>+1(510)486-7218</contact_phone><related_resource>https://materialsproject.org/citing</related_resource><contributor_organizations>MIT; UC Berkeley; Duke; U Louvain</contributor_organizations><subject_categories_code>36 MATERIALS SCIENCE</subject_categories_code><keywords>crystal structure; CaY5(MoO6)2; Ca-Mo-O-Y; electronic bandstructure</keywords><description>Computed materials data using density functional theory calculations. These calculations determine the electronic structure of bulk materials by solving approximations to the Schrodinger equation. For more information, see https://materialsproject.org/docs/calculations</description></record></records>'


def post():
    logging.debug("POSTING")
    r = requests.post(elink_endpoint, auth=(username, password), data=data)
    return r


if __name__ == "__main__":
    dom = parseString(data)

    # print("*************")
    # print("I'm posting the data below")
    # print(dom.toprettyxml())
    # print("*************")

    r = post()

    print("**********************")
    print("Results from posting: ")
    print("status_code = ", r.status_code)
    import json

    print(f"content = {json.dumps(parse(r.content), indent=2)}")
    print("**********************")