from mongogrant.client import Client
from pymongo.database import Database

client = Client()

client.set_remote(endpoint="https://grantmedb.materialsproject.org", token="abf5ff813fb545edb9e12120d42084af")

client.set_alias(alias="knowhere", actual="knowhere.lbl.gov", which="host")
client.set_alias(alias="prod", actual="mongodb04.nersc.gov", which="host")
client.set_alias(alias="mp_core", actual="mp_core", which="db")
client.set_alias(alias="mg_core_prod", actual="mg_core_prod", which="db")

mp_core_db: Database = client.db("ro:knowhere/mp_core")
robocrys_db: Database = client.db("ro:prod/mg_core_prod")

"""
dumps dois to file
"""
local_file = "../files/dois2.json"
def dump_dois_to_file(local_file="../files/dois2.json"):
    cursor = mp_core_db.get_collection('dois_next_gen').find({},{"_id": 0,
                                                                 "_bt": 0,
                                                                 "last_updated": 0}).limit(100)
    from bson.json_util import dumps
    import json
    with open(local_file, 'w') as outfile:
        jsons = json.loads(dumps(cursor))
        for j in jsons:
            j["valid"] = True
            j["_status"] = "COMPLETED"
        json.dump(jsons, outfile, indent=2)
# dump_dois_to_file()

"""
load to local mongo database, NOTE that this will clear ALL data before adding it back in
"""
from pymongo import MongoClient
import json
local_client = MongoClient("mongodb://localhost:27017/")
local_mp_core_db = local_client["mp_core"]
local_dois_coll = local_mp_core_db["dois"]
local_dois_coll.delete_many({})
with open(local_file) as f:
    file_data = json.load(f)
local_dois_coll.insert_many(file_data)
print(f"local doi collection now has {local_dois_coll.count_documents(filter={})} documents")
