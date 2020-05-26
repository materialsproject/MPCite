import requests
import json
from datetime import datetime

from utility import ElsevierPOSTContainerModel

elvisier = ElsevierPOSTContainerModel(identifier="mp-10074",
                                      source="osti.lbnl",
                                      title="GeSe2",
                                      doi="10.17188/1185101",
                                      url="https://materialsproject.org/materials/mp-10074",
                                      keywords=['crystal structure'],
                                      date="2011-05-28",
                                      dateCreated="2011-05-28",
                                      dateAvailable="2020-05-04")

# print(datetime.now().isoformat().__str__())
print("********** data being posted ***************")
print(json.dumps(elvisier.dict(), indent=2))
headers = {"x-api-key": ""}
url = "https://push-feature.datasearch.elsevier.com/container"
r = requests.post(url=url, data=json.dumps(elvisier.dict()), headers=headers)

print("********** response returned posted ***************")
print(r)
print(r.content)
