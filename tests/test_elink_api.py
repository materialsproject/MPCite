import os
from dotenv import load_dotenv

from elinkapi import Elink, Record, exceptions
import pytest
from mpcite.models import ELinkGetResponseModel, TestClass

from pymongo import MongoClient


load_dotenv()

atlas_user = os.environ.get("atlas_user")
atlas_password = os.environ.get("atlas_password")
atlas_host = os.environ.get("atlas_host")
mongo_uri = f"mongodb+srv://{atlas_user}:{atlas_password}@{atlas_host}/"

api = Elink(token=os.environ.get("elink_api_key")) # target default is production E-link service.

record = api.get_single_record(1190959)
type(record)

ELinkGotRecordModel = ELinkGetResponseModel.from_elinkapi_record(record)

print(ELinkGotRecordModel.get_title())
print(ELinkGotRecordModel.get_site_url())
print(ELinkGotRecordModel.get_keywords())
print(ELinkGotRecordModel.get_default_description())



ELinkTestGetRecordModel = TestClass(**record.model_dump())

with MongoClient(mongo_uri) as client:
    #get all material_ids and dois from doi collection
    doi_collection = client["mp_core"]["dois"]
    materials_to_update = list(doi_collection.find({}, {"_id": 0, "material_id": 1, "doi": 1}, limit=10))
    material_ids = [entry["material_id"] for entry in materials_to_update]

    # check # of material_ids from DOI collection vs amount in robocrys

    # get description for material_ids from robocrys collection
    coll = client["mp_core_blue"]["robocrys"]
    res = list(coll.find({"material_id": {"$in": material_ids}}, {"_id": 0, "material_id": 1, "description": 1}))

    # join on material_id
    for doc in res:
        mat = next(filter(lambda x: x["material_id"] == doc["material_id"], materials_to_update))
        doc["doi"] = mat["doi"]


# {"material_id": ..., "doi": ..., "description": ...} ->
# Record(
#     template_fields ...,
#     doi: ...,
#     description: ...,
#     fields_where_material_id_makes_sense: ...,
# )

