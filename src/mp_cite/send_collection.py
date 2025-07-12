from pathlib import Path
from xml.dom.minidom import parseString
from dicttoxml import dicttoxml
from mpcite.doi_builder import DOIBuilder
import json
from monty.json import MontyDecoder
from pydantic import BaseModel, Field
from typing import List

default_description = (
    "Computed materials data using density functional theory calculations. These "
    "calculations determine the electronic structure of bulk materials by solving "
    "approximations to the Schrodinger equation. For more information, "
    "see https://materialsproject.org/docs/calculations"
)


class CollectionsModel(BaseModel):
    title: str = Field(default="Sample Title")
    product_type: str = Field(default="DC")
    relidentifiersblock: List[List[str]] = Field()
    contributors: List[dict]
    description: str = Field(default=default_description)
    site_url: str = Field(default="https://materialsproject.org/")


config_file = Path("/Users/michaelwu/Desktop/projects/MPCite/files/config_prod.json")

bld: DOIBuilder = json.load(config_file.open("r"), cls=MontyDecoder)
bld.config_file_path = config_file.as_posix()

records = [
    CollectionsModel(
        relidentifiersblock=[["mp-1", "mp-2", "mp-1"]],
        contributors=[
            {
                "first_name": "Michael",
                "last_name": "Wu",
                "email": "wuxiaohua1011@berkeley.edu",
            }
        ],
    ).dict(),
    CollectionsModel(
        relidentifiersblock=[["mp-21"], ["mp-22"]],
        contributors=[
            {
                "first_name": "Michael",
                "last_name": "Wu",
                "email": "wuxiaohua1011@berkeley.edu",
            }
        ],
    ).dict(),
]


def my_item_func(x):
    if x == "records":
        return "record"
    elif x == "contributors":
        return "contributor"
    elif x == "relidentifier_detail":
        return "related_identifier"
    elif x == "relidentifiersblock":
        return "relidentifier_detail"
    else:
        return "item"


records_xml = parseString(
    dicttoxml(records, custom_root="records", attr_type=False, item_func=my_item_func)
)

for item in records_xml.getElementsByTagName("relidentifier_detail"):
    item.setAttribute("type", "accession_num")
    item.setAttribute("relationType", "Compiles")

print(records_xml.toprettyxml())
# response = bld.elink_adapter.post_collection(data=records_xml.toxml())
# print(response)
