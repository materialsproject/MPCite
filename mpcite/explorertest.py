import requests
import json
from models import ExplorerGetResponseModel


endpoint= "https://www.osti.gov/dataexplorer/api/v1/records" # DataCite Explorer API
username= "wuxiaohua1011@berkeley.edu" # user name for explorer.endpoint
password= "" # password for explorer.endpoint


r = requests.get(url=endpoint + "/1185101", auth=(username, password))
print(r.status_code)
# print(r.content)
j = json.loads(r.content)
content = j[0]
explorer_response = ExplorerGetResponseModel.parse_obj(content)
print(json.dumps(explorer_response.dict(), indent=2))