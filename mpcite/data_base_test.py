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

cursor = mp_core_db.get_collection('dois_next_gen').find({})
print(cursor.count())
