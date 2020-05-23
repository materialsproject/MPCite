import requests
import json

from utility import ElsevierPOSTContainerModel, MaterialModel

elvisier = ElsevierPOSTContainerModel(identifier="mp-10074",
                                      source="https://materialsproject.org",
                                      title="GeSe2",
                                      doi="10.17188/1185101",
                                      url="https://materialsproject.org/materials/mp-10074",
                                      keywords="crystal structure; GeSe2; Ge-Se; electronic bandstructure")

print("********** data being posted ***************")
print(json.dumps(elvisier.dict(), indent=2))
headers = {"x-api-key": ""}
url = "https://push-feature.datasearch.elsevier.com/container"
r = requests.post(url=url, data=json.dumps(elvisier.dict()), headers=headers)

print("********** response returned posted ***************")
print(r)
print(r.content)
