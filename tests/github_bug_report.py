from elinkapi import Elink, Organization, Person, exceptions, Record
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

prod_api = Elink(token = os.environ.get("elink_api_PRODUCTION_key"))
review_endpoint = "https://review.osti.gov/elink2api/"
review_api = Elink(token = os.environ.get("elink_review_api_token"), target=review_endpoint)

# record_response = prod_api.get_single_record(1190959) # returns OSTI record response with OSTI ID = 1190959, which has a DOE Contract Number saved (AC02-05CH11231; EDCBEE)
# record_response_dict = record_response.model_dump(exclude_none=True)
# record_response_dict.pop("osti_id") # remove osti_id to allow post function

# new_record = Record(**record_response_dict) # identical record with removed OSTI_ID
# for org in new_record.organizations:
#     if org.type == "SPONSOR":
#         print(org)
#         org.identifiers = [{"type": 'CN_DOE', "value": 'AC02-05CH11231'}]

# # attempt to submit exact same record to review environment
# record_response_after_post = review_api.post_new_record(new_record, "save") # works after re-providing the DOE contract number

# # next, attempt updating this record
# record_to_update = review_api.get_single_record(record_response_after_post.osti_id)
# record_to_update.title = "Updated Title For Materials Data"
# review_api.update_record(record_response_after_post.osti_id, record_to_update, "submit")

required_fields = {
    "product_type": "DA",
    "title": "Testing if CN_DOE can be random",
    "organizations": [Organization(type='RESEARCHING', name='LBNL Materials Project (LBNL-MP)'), 
                      Organization(type='SPONSOR', name='TEST SPONSOR ORG', identifiers=[{"type": 'CN_DOE', "value": 'oiajdiwjdiwj'}])],
    "persons": [Person(type='AUTHOR', last_name='Schmoe')],
    "site_ownership_code": "LBNL-MP",
    "access_limitations": ['UNL'],
    "publication_date": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    "site_url": "https://next-gen.materialsproject.org/materials"
}

empty_record = Record(**required_fields)
print(f"SUBMITTED TO OSTI, FULLY VALIDATED:\n{review_api.get_single_record(2525614)}\n\n\nTRYING TO SUBMIT:\n{empty_record}")

try:
    saved_record = review_api.post_new_record(empty_record, "submit")
except exceptions.BadRequestException as ve:
    print(ve.message)
    print(ve.errors)