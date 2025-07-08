import os
import json
from elinkapi import Elink, Record
from elinkapi.record import RecordResponse
from dotenv import load_dotenv

import requests
from elinkapi.utils import Validation

from pymongo import MongoClient
import pymongo

from timeit import default_timer as timer
import logging
import datetime
from doi_builder import *

load_dotenv() # depends on the root directory from which you run your python scripts.

review_endpoint = "https://review.osti.gov/elink2api/"

prod_api  = Elink(token = os.environ.get("elink_api_PRODUCTION_key"))
review_api = Elink(token = os.environ.get("elink_review_api_token"), target=review_endpoint)

atlas_user = os.environ.get("atlas_user")
atlas_password = os.environ.get("atlas_password")
atlas_host = os.environ.get("atlas_host")
mongo_uri = f"mongodb+srv://{atlas_user}:{atlas_password}@{atlas_host}/"

failed_osti_ids = []

cwd = os.getcwd()
path = "/json_pages/"

for filename in os.listdir(cwd+path):
    logging.debug(f"Now extracting {filename}")
    file = open(cwd + path + filename, "r")
    for line in file:
        record = RecordResponse(**json.loads(line.strip()))
        record.osti_id = record.doi.split('/')[1]
        # for every record in the OSTI production environment:
        # flag for update performance
        update_success = False

        material_id = record.site_unique_id

        with MongoClient(mongo_uri) as client: # should I open this in or outside of the for loop?
            coll = client["mp_core_blue"]["robocrys"]
            res = coll.find_one({"material_id" : material_id})
        
            if res != None:
                robocrys_description = res["description"]
                
            # what if there is no document in robocrys found?
            else:
                logging.warning(f"No robocrys document was found to match the OSTI record: {record.osti_id}!")

        # if the description of the record on Elink doesnt match what is in the robocrys collection:
        if res != None and record.description != robocrys_description:
            # directly update the description of the record via the record response
            record.description = robocrys_description
            
            # and directly update the identifier for sponsoring org
            for entry in record.organizations:
                if entry.type == "SPONSOR":
                    entry.identifiers = [{"type": 'CN_DOE', "value": 'AC02-05CH11231'}]
                    break

            try:
                # send update to the record with the record response # update_record(osti_id, record, state="save")
                record_response = prod_api.update_record(record.osti_id, record, state="save")
                update_success = True

            except:
                logging.debug("The update failed to save!")
                # add the osti_id of the failed update to failed_osti_ids
                failed_osti_ids.append(record.osti_id)

            # if the update worked...
            if update_success == True:
                # save the record response returned with sending the update, done above
                # convert that record response into a doi_model
                doi_model = RecordResponse_to_doi_model(record) #change later to record response

                # upload that doi_model as a document to the new doi collection in mp_core
                # what is the collection
                with MongoClient() as local_client:
                    collection = local_client["dois_test"]["dois"]
                    x = collection.insert_one(doi_model.dict(by_alias=True)).inserted_id

        # else if the description on Elink matches what is in the robocrys collection:
        elif record.description == robocrys_description:
            # convert that record into a doi_model
            doi_model = RecordResponse_to_doi_model(record)

            # upload that doi_model as a document to the new doi collection in mp_core, no updated needed!
            with MongoClient() as local_client:
                collection = local_client["dois_test"]["dois"]
                x = collection.insert_one(doi_model).inserted_id

cwd = os.getcwd()
path = f"/files/failed_osti_ids_{str(datetime.datetime.now())}.txt"
with open(cwd+path, 'w') as output: # change filepath as needed
    for id in failed_osti_ids:
        output.write(str(id) + '\n') # i'm pretty sure it's a string already though...