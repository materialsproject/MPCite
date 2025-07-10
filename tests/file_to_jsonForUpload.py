import os
import json
from elinkapi import Elink, Record
from dotenv import load_dotenv

import requests
from elinkapi.utils import Validation

from pymongo import MongoClient
import pymongo

from timeit import default_timer as timer

load_dotenv() # depends on the root directory from which you run your python scripts.

review_endpoint = "https://review.osti.gov/elink2api/"

prod_api  = Elink(token = os.environ.get("elink_api_PRODUCTION_key"))
review_api = Elink(token = os.environ.get("elink_review_api_token"), target=review_endpoint)


atlas_user = os.environ.get("atlas_user")
atlas_password = os.environ.get("atlas_password")
atlas_host = os.environ.get("atlas_host")
mongo_uri = f"mongodb+srv://{atlas_user}:{atlas_password}@{atlas_host}/"

cwd = os.getcwd()
path = "/json_pages/page_number_4.0" # IT'S ONLY DOING ONE FILE RIGHT NOW
file = open(cwd + path, "r")

update_counter = 0
records_checked = 0

def delete_record(api, osti_id, reason):
    """Delete a record by its OSTI ID."""
    response = requests.delete(f"{api.target}records/{osti_id}?reason={reason}", headers={"Authorization": f"Bearer {api.token}"})
    Validation.handle_response(response)
    return response.status_code == 204  # True if deleted successfully

def emptyReviewAPI(reason):
    allDeleted = True
    for record in review_api.query_records():
        delete_record(review_api, record.osti_id, reason) 

raise

start = timer()

# Post an updated json

postUnedited = False

for line in file:
    js = json.loads(line.strip())

    for entry in js["organizations"]:
        if entry["type"] == "SPONSOR":
            entry["identifiers"] = [{"type": 'CN_DOE', "value": 'AC02-05CH11231'}]

    material_id = js["site_unique_id"]

    robocrys_description = js["description"]

    with MongoClient(mongo_uri) as client:
        coll = client["mp_core_blue"]["robocrys"]
        res = coll.find_one({"material_id" : material_id})
        records_checked += 1
        
        if res != None:
           robocrys_description = res["description"]

    # see if an update to the description is necessary, if it is, then update the description and post a new record.
    if postUnedited or (robocrys_description != None and js["description"] != robocrys_description): #if a robocrys_description was found internally and it doesn't match what ELink has record...
        js["description"] = "OLD WAS UPDATED, THEN IT WAS POSTED: " + robocrys_description
        my_record = Record(**js)

        saved_record = None
        try:
            # The API will now return an error code on this call
            # because "AAAA" is not a valid site_ownership_code

            saved_record = review_api.post_new_record(my_record, state="submit")
            update_counter += 1

            print(f"NEW RECORD POSTED: {saved_record.osti_id}")
            raise
        except:
            print(f"Record failed to post!: {my_record.doi}. Robocrys Collection Had Description {robocrys_description[0:50]}... Prod_Env ELink Had {my_record.description[37:87]}...")
            raise

    if update_counter >= 10000:
        break

end = timer()
print(f"Records Updated and/or Posted: {update_counter} \nRecords Checked in Total: {records_checked}. \nIt took {end - start} seconds")

#######################################################
# JUST POST JSON, Then update posted json Later
# post_counter = 0
# records_checked = 0

# for line in file:
#     js = json.loads(line.strip())

#     material_id = js["site_unique_id"]

#     # always post, no update
#     my_record = Record(**js)

#     saved_record = None
#     try:
#         # The API will now return an error code on this call
#         # because "AAAA" is not a valid site_ownership_code

#         # posts an unupdated record
#         saved_record = review_api.post_new_record(my_record, "save")
#         post_counter += 1

#         print("\n\n NEW RECORD POSTED")
#         print(saved_record)

#         robocrys_description = js["description"]

#         with MongoClient(mongo_uri) as client:
#             coll = client["mp_core_blue"]["robocrys"]
#             res = coll.find_one({"material_id" : material_id})
#             records_checked += 1
            
#             if res != None:
#                 robocrys_description = res["description"]

#         if robocrys_description != None and js["description"] != robocrys_description: # if an update is needed
#             # update the js["description"]
#             js["description"] = "OLD WAS POSTED, THEN RECORD WITH NEW DESCRIPTION UPDATED IT: " + robocrys_description

#             # turn it into a new record
#             new_updated_record = Record(**js)

#             # use that new record to update what was just posted
#             review_api.update_record(saved_record.osti_id, new_updated_record, "save")

#     except:
#         print("Record failed to post!")

#     if post_counter >= 10000:
#         break

# end = timer()
# print(f"Records Updated and/or Posted: {update_counter} \n Records Checked in Total: {records_checked}. It took {end - start} seconds")

######################################################