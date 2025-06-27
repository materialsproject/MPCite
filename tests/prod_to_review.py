from elinkapi import Elink, Query, Record

import os
from dotenv import load_dotenv

import json

load_dotenv() # depends on the root directory from which you run your python scripts.

review_endpoint = "https://review.osti.gov/elink2api/"

prod_api  = Elink(token = os.environ.get("elink_api_PRODUCTION_key"))
review_api = Elink(token = os.environ.get("elink_review_api_token"), target=review_endpoint)

print(prod_api.query_records())

rows_per_page = 100

# query production
query = prod_api.query_records(rows=rows_per_page)
print(f"Query retrieved {query.total_rows} record(s)")

count_materials_data = 0
count_MaterialsDataOn = 0
cwd = os.getcwd()
page_number = 0
page_json_list = []

for record in query:
    # increment counter
    count_materials_data = count_materials_data + 1 
    print(f"On record #{count_materials_data}, next url is {query.next_url}, previous url is {query.previous_url}")

        # see if the record is a Materials Data on record
    if record.title.startswith("Materials Data on"):
            # increment the MaterialsDataOn counter
            count_MaterialsDataOn = count_MaterialsDataOn + 1

            # prepare the new record for the review environment, remove the OSTI ID, and add its model_dump to the list of json objects for the page.
            new_record = record
            new_record_dict = new_record.model_dump(exclude_none=True)

            new_record_osti_id = new_record_dict.pop("osti_id") # now new_record_dict does not have the osti_id key.
            js = json.dumps(new_record_dict, default=str) # datetime objects are not JSON serializable, so we use default=str to convert them to strings.

            page_json_list.append(js)
            
            # TODO: take the new_record_dict and make it into a new post to the review environment and save the RecordResponse.

    else:
        print(f"Found edge case: {record.title}")

    if count_materials_data % rows_per_page == 0:
        # create/open, write, and close new json file 
        page_number = count_materials_data / rows_per_page
        path = f'/json_pages/page_number_{page_number}'
        fp = open(cwd+path, 'a')

        for js in page_json_list:
            fp.write(js)
            fp.write("\n")
        
        fp.close()
        page_json_list = []

        print(f"Page {page_number} finished. Now at {count_materials_data} data entries. {count_materials_data - count_MaterialsDataOn} edge cases found.")

# print remainder of records if not a full page after for loop exits
page_number = page_number + 1
path = f'/json_pages/page_number_{page_number}'
fp = open(cwd+path, 'a')
for js in page_json_list:
    fp.write(js)
    fp.write("\n")
fp.close()

# # if contains materials data on, then add to batch
# for count_materials_data < query.total_rows:

#     # print(f"The length of the query is now {len(query.data)}")
#     record = next(query)
#     count_materials_data = count_materials_data + 1

#     if record.title.startswith("Materials Data on"):
#         count_MaterialsDataOn = count_MaterialsDataOn + 1

#         new_record = record
#         new_record_dict = new_record.model_dump(exclude_none=True)

#         new_record_osti_id = new_record_dict.pop("osti_id")

#         page_dict[f"Entry OSTI_ID {new_record_osti_id}"] = new_record_dict

#         # TODO: take the new_record_dict and make it into a new post to the review environment and save the RecordResponse.



#     if count_materials_data % rows_per_page == 0:
#         # if a page has been fully consummed, then print the new batched dictionary to a json file.

#         js = json.dumps(page_dict, default=str)

#         # open new json file if not exist it will create
#         cwd = os.getcwd()
#         path = f'/json_pages/page_number_{count_materials_data/rows_per_page}'
#         fp = open(cwd+path, 'a')

#         # write to json file
#         fp.write(js)

#         # close the connection to the file and empty the dict
#         fp.close()
#         page_dict = {}

#         print(f"Page {(count_materials_data / rows_per_page)} finished. Now at {count_materials_data} data entries. {count_materials_data - count_MaterialsDataOn} edge cases found.")

# model_dump exclude_none=True, remove null keys
# pop osti_id --> save batch to json files
# make new record 
# post to review_api
