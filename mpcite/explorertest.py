import requests
import json
from models import ExplorerGetJSONResponseModel


endpoint= "https://staging.osti.gov/dataexplorer/api/v1/records/" # DataCite Explorer API
username= "demo" # user name for explorer.endpoint
password= "letmein" # password for explorer.endpoint

payload = {"osti_id":"1479847"}
header = {"Accept":"application/x-bibtex"}
r = requests.get(url=endpoint, auth=(username, password), params=payload, headers=header)
print(r.status_code)
print(r.content)
