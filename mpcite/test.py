import requests
import json
from xmltodict import parse
from xml.dom.minidom import parseString
import logging
elink_endpoint = "http://www.osti.gov/elinktest/2416api"
username = "materials2416websvs"
password = "Sti!2416sub"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


data = b'<?xml version="1.0" ?><records><record><dataset_type>SM</dataset_type><title>Materials Data on Mn3Sn by Materials Project</title><creators>Kristin Persson</creators><product_nos>mp-22389</product_nos><accession_num>mp-22389</accession_num><contract_nos>AC02-05CH11231; EDCBEE</contract_nos><originating_research_org>Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)</originating_research_org><publication_date>05/03/2020</publication_date><language>English</language><country>USA</country><sponsor_org>USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)</sponsor_org><site_url>https://materialsproject.org/materials/mp-22389</site_url><contact_name>Kristin Persson</contact_name><contact_org>LBNL</contact_org><contact_email>kapersson@lbl.gov</contact_email><contact_phone>+1(510)486-7218</contact_phone><related_resource>https://materialsproject.org/citing</related_resource><contributor_organizations>MIT; UC Berkeley; Duke; U Louvain</contributor_organizations><subject_categories_code>36 MATERIALS SCIENCE</subject_categories_code><keywords>crystal structure; Mn3Sn; Mn-Sn; electronic bandstructure</keywords><description>Mn3Sn is beta Cu3Ti-like structured and crystallizes in the hexagonal P6_3/mmc space group. The structure is three-dimensional. there are two inequivalent Mn sites. In the first Mn site, Mn is bonded to eight Mn and four equivalent Sn atoms to form distorted MnMn8Sn4 cuboctahedra that share corners with four equivalent SnMn12 cuboctahedra, corners with fourteen equivalent MnMn8Sn4 cuboctahedra, edges with six equivalent SnMn12 cuboctahedra, edges with twelve MnMn8Sn4 cuboctahedra, faces with four equivalent SnMn12 cuboctahedra, and faces with sixteen MnMn8Sn4 cuboctahedra. There are a spread of Mn\xe2\x80\x93Mn bond distances ranging from 2.72\xe2\x80\x932.87 \xc3\x85. All Mn\xe2\x80\x93Sn bond lengths are 2.80 \xc3\x85. In the second Mn site, Mn is bonded to eight equivalent Mn and four equivalent Sn atoms to form distorted MnMn8Sn4 cuboctahedra that share corners with four equivalent SnMn12 cuboctahedra, corners with fourteen MnMn8Sn4 cuboctahedra, edges with six equivalent SnMn12 cuboctahedra, edges with twelve equivalent MnMn8Sn4 cuboctahedra, faces with four equivalent SnMn12 cuboctahedra, and faces with sixteen MnMn8Sn4 cuboctahedra. All Mn\xe2\x80\x93Sn bond lengths are 2.80 \xc3\x85. Sn is bonded to twelve Mn atoms to form SnMn12 cuboctahedra that share corners with six equivalent SnMn12 cuboctahedra, corners with twelve MnMn8Sn4 cuboctahedra, edges with eighteen MnMn8Sn4 cuboctahedra, faces with eight equivalent SnMn12 cuboctahedra, and faces with twelve MnMn8Sn4 cuboctahedra.</description></record></records>'


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

    # print("**********************")
    # print("Results from posting: ")
    # print("status_code = ", r.status_code)
    # import json
    #
    # print(f"content = {json.dumps(parse(r.content), indent=2)}")
    # print("**********************")
