from mpcite.core import *
from mpcite.doi_builder import RecordResponse_to_doi_model, upload_doi_document_model_to_collection
import os
import json
from dotenv import load_dotenv

load_dotenv() # depends on the root directory from which you run your python scripts.

review_endpoint = "https://review.osti.gov/elink2api/"

prod_api  = Elink(token = os.environ.get("elink_api_PRODUCTION_key"))
review_api = Elink(token = os.environ.get("elink_review_api_token"), target=review_endpoint)

cwd = os.getcwd()
path = "/json_pages/page_number_1000.0" # IT'S ONLY DOING ONE FILE RIGHT NOW
file = open(cwd + path, "r")

atlas_user = os.environ.get("atlas_user")
atlas_password = os.environ.get("atlas_password")
atlas_host = os.environ.get("atlas_host")
mongo_uri = f"mongodb+srv://{atlas_user}:{atlas_password}@{atlas_host}/"

with MongoClient(mongo_uri) as real_client:
    with MongoClient() as doi_client: # open the mongoclient outside of the for loop, is more efficient than opening and closing it repeatedly
        dois = doi_client["dois_test"]["dois"]

        # for line in file:
        #     js = json.loads(line.strip())

        #     # temporarily fix the sponsor organization bug
        #     for entry in js["organizations"]:
        #         if entry["type"] == "SPONSOR":
        #             entry["identifiers"] = [{"type": 'CN_DOE', "value": 'AC02-05CH11231'}]

        #     my_record = Record(**js)

        #     # make a post to the elink review environment    
        #     saved_record = review_api.post_new_record(my_record, state="submit")

        #     # make a doi document with saved_record
        #     doi_model = RecordResponse_to_doi_model(saved_record)

        #     # now, add that doi to the local doi collection
        #     upload_doi_document_model_to_collection(doi_model, dois)

        # all_material_ids = [doc["material_id"] for doc in dois.find({}, {"_id": 0, "material_id": 1})]

        # for material_id in all_material_ids:
            
        #     # query prod env for record with materials_id == site_unique_id
        #     record_from_prod = prod_api.query_records(site_unique_id=material_id)

        #     if record_from_prod.total_rows != 1:
        #         print(f"ERROR: not unique Material_ID! {material_id}")
        #         raise

        #     # make a doi_model from that data
        #     recordresponse_from_prod = RecordResponse_to_doi_model(record_from_prod.data[0])

        #     query_filter = {"material_id": material_id}

        #     # Find existing document to preserve the osti_id
        #     existing_doc = dois.find_one(query_filter, {"osti_id": 1})  # only retrieve osti_id

        #     if not existing_doc:
        #         print(f"ERROR: document with material_id {material_id} not found in `dois` collection.")
        #         raise

        #     replacement_doc = recordresponse_from_prod.model_dump()
        #     replacement_doc["osti_id"] = existing_doc["osti_id"] 

        #     dois.replace_one(query_filter, replacement_doc)

        osti_OOD_list = find_out_of_date_doi_entries(real_client, doi_client, "mp_core_blue", "robocrys", "dois_test", "dois")
        print(osti_OOD_list)

    for osti_id in osti_OOD_list:
        material_id_to_update = review_api.get_single_record(osti_id).site_unique_id

        new_values = {
            "description": "UPDATED ROBOCRYS DESCRIPTION: " + next(real_client["mp_core_blue"]["robocrys"].find({"material_id": material_id_to_update}, {"_id": 0, "description": 1}))["description"]
        }

        update_existing_osti_record(review_api, osti_id, new_values)