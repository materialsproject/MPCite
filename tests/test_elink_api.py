import os
from dotenv import load_dotenv

from elinkapi import Elink, Record, exceptions
import pytest
from mpcite.models import ELinkGetResponseModel, TestClass

from pymongo import MongoClient
import pymongo

load_dotenv()

atlas_user = os.environ.get("atlas_user")
atlas_password = os.environ.get("atlas_password")
atlas_host = os.environ.get("atlas_host")
mongo_uri = f"mongodb+srv://{atlas_user}:{atlas_password}@{atlas_host}/"

api = Elink(token=os.environ.get("elink_api_PRODUCTION_key")) # target default is production E-link service.

### Grabbing an existing record

# record = api.get_single_record(mp-id) # test for silicon

# type(record)

# ELinkGotRecordModel = ELinkGetResponseModel.from_elinkapi_record(record)

# print(ELinkGotRecordModel.get_title())
# print(ELinkGotRecordModel.get_site_url())
# print(ELinkGotRecordModel.get_keywords())
# print(ELinkGotRecordModel.get_default_description())

# ELinkTestGetRecordModel = TestClass(**record.model_dump())

### Making a new record

# with MongoClient(mongo_uri) as client:
#     #get all material_ids and dois from doi collection
#     doi_collection = client["mp_core"]["dois"]
#     materials_to_update = list(doi_collection.find({}, {"_id": 0, "material_id": 1, "doi": 1}, limit=10))
#     material_ids = [entry["material_id"] for entry in materials_to_update]

#     # check # of material_ids from DOI collection vs amount in robocrys

#     # get description for material_ids from robocrys collection
#     coll = client["mp_core_blue"]["robocrys"]
#     res = list(coll.find({"material_id": {"$in": material_ids}}, {"_id": 0, "material_id": 1, "description": 1}))

#     # join on material_id
#     for doc in res:
#         mat = next(filter(lambda x: x["material_id"] == doc["material_id"], materials_to_update))
#         doc["doi"] = mat["doi"]


# {"material_id": ..., "doi": ..., "description": ...} ->
# Record(
#     template_fields ...,
#     doi: ...,
#     description: ...,
#     fields_where_material_id_makes_sense: ...,
# )

# with the client open
with MongoClient(mongo_uri) as client:
    # get all dois from the collection
    doi_collection = client["mp_core"]["dois"]
    materials_to_update = list(doi_collection.find({}, {"_id": 0, "doi": 1, "material_id": 1}, limit=2))

    # from the doi collection, grab the material_id and doi of each material
    material_ids = [entry["material_id"] for entry in materials_to_update]

    # additionally, gain the osti id from the doi
    osti_ids = [entry["doi"].split("10.17188/")[1] for entry in materials_to_update]

    # additionally, grab the description of each material from the robocrys
    coll = client["mp_core_blue"]["robocrys"] # grabs robocrys collection from active database
    res = list(coll.find({"material_id": {"$in": material_ids}}, {"_id": 0, "material_id": 1, "description": 1})) # grabs the material id and description of entries in the collection
    descriptions = [entry["description"] for entry in res]

    # for each material (and its material_id, doi, and osti_id)
    for i in range(len(materials_to_update)):    
        internal_material_id = material_ids[i]
        internal_osti_id = osti_ids[i]
        internal_description = descriptions[i]

        # get_single_record(osti_id)
        record = api.get_single_record(internal_osti_id)

        print(f"\n \n \nPrinting what is currently on ELINK for {internal_material_id}*****************************************")
        print(record)

        if internal_material_id == record.site_unique_id:
            # update description
            record.description = "testTESTtestTESTtest"

        print(f"\n \n \nPrinting record for {internal_material_id}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print(record)

        # # post updated record
        # try:
        #     saved_record = api.post_new_record(record, "save")
        # except exceptions.BadRequestException as ve:
        #     ...
        #     # ve.message = "Site Code AAAA is not valid."
        #     # ve.errors provides more details:
        #     # [{"status":"400", "detail":"Site Code AAAA is not valid.", "source":{"pointer":"site_ownership_code"}}]



