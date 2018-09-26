import sys, yaml, os
from collections import OrderedDict
from xml.dom.minidom import parseString
from dicttoxml import dicttoxml
from mpcite.adapter import OstiMongoAdapter
from mpcite.cli import DictAsMember

mod_dir = os.path.dirname(os.path.abspath(__file__))
default_config = os.path.normpath(os.path.join(mod_dir, 'files', 'config.yaml'))
print default_config
with open(default_config, 'r') as f:
    config = DictAsMember(yaml.load(f))
oma = OstiMongoAdapter.from_config(config)
content = oma.osti_request()
print content.get('@numfound') 
print oma.matcoll.count()
sys.exit(0)

member_id = '10.17188'
osti_id = 1432992
doi = '{}/{}'.format(member_id, osti_id)
landing_page = "https://materialsproject.org/materials/{}".format(doi)
doc = oma.db.doi_collections.find_one({'doi': doi})
contributor_name, contributor_email = doc['contributor'].rsplit(' ', 1)
first_name, last_name = contributor_name.rsplit(' ', 1)
private_email = contributor_email[1:-1]

records = [OrderedDict([
    ('osti_id', osti_id),
    ('title', doc['title']),
    ('site_url', landing_page),
    ('product_type', 'DC'),
    ('contributors', [OrderedDict([
      ('first_name', first_name),
      ('last_name', last_name),
      ('private_email', private_email)
    ])]),
    ('collection_items', doc['mp_ids']),
    ('description', 'Computed materials data using density '
     'functional theory calculations. These calculations determine '
     'the electronic structure of bulk materials by solving '
     'approximations to the Schrodinger equation. For more '
     'information, see https://materialsproject.org/docs/calculations')
])]

my_item_func = lambda x: x[:-1]
records_xml = parseString(dicttoxml(
    records, custom_root='records', attr_type=False, item_func=my_item_func
))
for item in records_xml.getElementsByTagName('collection_item'):
  item.setAttribute("type", "accession_num")
print(records_xml.toprettyxml())

print 'requesting ...'
content = oma.osti_request(req_type='post', payload=records_xml.toxml())
print content
