import requests
import json
from models import ExplorerGetJSONResponseModel


endpoint= "https://www.osti.gov/dataexplorer/api/v1/records" # DataCite Explorer API
username= "wuxiaohua1011@berkeley.edu" # user name for explorer.endpoint
password= "Xiaoxiao1011!" # password for explorer.endpoint
#("1405334", "1350253")
payload = {"osti_id":"1405334"}
header = {"Accept":"application/x-bibtex"}
r = requests.get(url=endpoint, auth=(username, password), params=payload, headers=header)
print(r.status_code)
print(r.content)
# j = json.loads(r.content)
# content = j[0]
# explorer_response = ExplorerGetResponseModel.parse_obj(content)
# print(json.dumps(explorer_response.dict(), indent=2))