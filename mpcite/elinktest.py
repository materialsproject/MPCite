import requests
import json
from xmltodict import parse
from xml.dom.minidom import parseString
import logging

elink_endpoint = "https://www.osti.gov/elinktest/2416api"
username = ""
password = ""

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

data = b'<?xml version="1.0" ?><records><record><osti_id>1479827</osti_id><dataset_type>SM</dataset_type><title>Materials Data on Mo12P by Materials Project</title><creators>Kristin Persson</creators><product_nos>mp-1237770</product_nos><accession_num>mp-1237770</accession_num><contract_nos>AC02-05CH11231; EDCBEE</contract_nos><originating_research_org>Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)</originating_research_org><publication_date>04/01/2019</publication_date><language>English</language><country>US</country><sponsor_org>USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)</sponsor_org><site_url>https://materialsproject.org/materials/mp-1237770</site_url><contact_name>Kristin Persson</contact_name><contact_org>LBNL</contact_org><contact_email>kapersson@lbl.gov</contact_email><contact_phone>+1(510)486-7218</contact_phone><related_resource>https://materialsproject.org/citing</related_resource><contributor_organizations>MIT; UC Berkeley; Duke; U Louvain</contributor_organizations><subject_categories_code>36 MATERIALS SCIENCE</subject_categories_code><keywords>crystal structure; Mo12P; Mo-P; electronic bandstructure</keywords><description>Computed materials data using density functional theory calculations. These calculations determine the electronic structure of bulk materials by solving approximations to the Schrodinger equation. For more information, see https://materialsproject.org/docs/calculations</description></record></records>'

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

    print(f"content = {r.content}")
    print("**********************")