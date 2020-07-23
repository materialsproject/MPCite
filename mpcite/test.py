import ostiapi
from pathlib import Path
from monty.json import MontyDecoder
from mpcite.doi_builder import DoiBuilder
import json
from mpcite.utility import ELinkGetResponseModel, DOIRecordModel
from mpcite.models import MaterialModel

ostiapi.testmode()

config_file = Path("/Users/michaelwu/Desktop/projects/MPCite/files/config_test.json")
bld: DoiBuilder = json.load(config_file.open("r"), cls=MontyDecoder)

bld.doi_store.connect()
bld.materials_store.connect()

mp_id = "mp-839"

doi_record: DOIRecordModel = DOIRecordModel.parse_obj(
    bld.doi_store.query_one(criteria={bld.doi_store.key: mp_id})
)
material: MaterialModel = MaterialModel.parse_obj(
    bld.materials_store.query_one(criteria={bld.materials_store.key: mp_id})
)

elink_response_model = ELinkGetResponseModel(
    osti_id=doi_record.get_osti_id(),
    title=ELinkGetResponseModel.get_title(material=material),
    product_nos=material.task_id,
    accession_num=material.task_id,
    publication_date=material.last_updated.strftime("%m/%d/%Y"),
    site_url=ELinkGetResponseModel.get_site_url(mp_id=material.task_id),
    keywords=ELinkGetResponseModel.get_keywords(material=material),
    description=bld.get_material_description(material.task_id),
)

print(elink_response_model)
