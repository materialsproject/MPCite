from adapter import OstiMongoAdapter
from doi_builder import DoiBuilder
from record import OstiRecord
import logging, yaml
from pathlib import Path
import os
from utility import DictAsMember

# configuration stuff
config_file = Path(os.path.abspath(__file__)).parent.parent / "files" / "config.yaml"
config = DictAsMember(yaml.load(open(config_file.as_posix(), 'r'), Loader=yaml.SafeLoader))
FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger('mpcite')

# instantiating necessary connections
"""
establish connection with materials database
    1. establish connection with materials collection (Guaranteed online)
    2. establish connection with doi collection (online or local)
    3. establish connection with robocrys (Guaranteed online) [TODO]

establish connection with ELink to submit info

establish connection with osti explorer to get bibtex
"""
oma = OstiMongoAdapter.from_config(config)
bld = DoiBuilder(oma, config)

bld.run()
# rec = OstiRecord(oma)
# logger.debug('{} loaded'.format(config_file))






