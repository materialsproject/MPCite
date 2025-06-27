# This script will see how many documents in ELink, i.e. ones with a DOI, are not accounted for in the internal DOI collection.

from elinkapi import Elink, Query, Record

import os
from dotenv import load_dotenv

load_dotenv() # depends on the root directory from which you run your python scripts.

api  = Elink(token = os.environ.get("elink_api_PRODUCTION_key"))


query1 = api.query_records(rows=1000)

materials_with_dois : list[Record] = []

for page in query1:
    print(f"Now on Page: {page.title}")
    print(f"Material_ID: {page.site_unique_id} and DOI: http://doi.org/{page.doi}")
    
    if page.site_unique_id.startswith("mp-"):
        materials_with_dois.append(page)

    # for record in page.data:
    #     if record.site_unique_id.startswith("mp-"):
    #         materials_with_dois.append(record)



# set_q1 = [page for page in query1]
# set_q2 = [page for page in query2]

# set_diffq1q2 = set(set_q1) - set(set_q2)
# print (f"Difference matched {len(set)} records")

# filtered = [
#     page for page in query1
#     if page.title.lower().startswith("materials data on")
# ]

# print (f"Filtered Query1 has {len(filtered)} records")

# paginate through ALL results
# for page in query1:
#     print(page.title)
#     print(f"Material_ID: {page.site_unique_id} and DOI: http://doi.org/{page.doi}")
    
#     for record in page.data:
#         print (f"OSTI ID: {record.osti_id} Title: {record.title}")