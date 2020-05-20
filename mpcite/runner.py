from adapter import OstiMongoAdapter
from doi_builder import DoiBuilder
from record import OstiRecord
import logging, yaml
from pathlib import Path
import os
from utility import DictAsMember


# FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
# logging.basicConfig(format=FORMAT, level=logging.DEBUG)
# logger = logging.getLogger('mpcite')

# instantiating necessary connections
"""
establish connection with materials database
    1. establish connection with materials collection (Guaranteed online)
    2. establish connection with doi collection (online or local)
    3. establish connection with robocrys (Guaranteed online) [TODO]

establish connection with ELink to submit info

establish connection with osti explorer to get bibtex
"""
from utility import Connection, OSTI

# configuration stuff
config_file = Path(os.path.abspath(__file__)).parent.parent / "files" / "config.yaml"
config = DictAsMember(yaml.load(open(config_file.as_posix(), 'r'), Loader=yaml.SafeLoader))

# prepare
oma = OstiMongoAdapter.from_config(config)
elink = Connection.parse_obj(config["osti"]["elink"])
explorer = Connection.parse_obj(config["osti"]["explorer"])
osti = OSTI(elink=elink, explorer=explorer)

# decalre builder instance
bld = DoiBuilder(oma, osti)

# run program
bld.run()
# rec = OstiRecord(oma)
# logger.debug('{} loaded'.format(config_file))

# mpid that exist mp-10070 on test e-link
# print(bld.get_doi_from_elink(mpid_or_ostiid="mp-3"))





