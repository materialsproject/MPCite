from adapter import OstiMongoAdapter
from doi_builder import DoiBuilder
import logging, yaml
from pathlib import Path
import os
from utility import DictAsMember
from utility import Connection, OSTI

# configuration stuff
config_file = Path(os.path.abspath(__file__)).parent.parent / "files" / "config.yaml"
config = yaml.load(open(config_file.as_posix(), 'r'), Loader=yaml.SafeLoader)

# prepare
oma = OstiMongoAdapter.from_config(config)
elink = Connection.parse_obj(config["osti"]["elink"])
explorer = Connection.parse_obj(config["osti"]["explorer"])
osti = OSTI(elink=elink, explorer=explorer)

# decalre builder instance
send_size = 1
bld = DoiBuilder(oma, osti, send_size=1, sync=True)

# run program
bld.run(log_level=logging.DEBUG)

