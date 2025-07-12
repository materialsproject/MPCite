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

# emptyReviewAPI("Testing", review_api)

with MongoClient() as client:
    client.dois_test.dois.delete_many({}, comment="Testing")