import logging
import os
import yaml
from pathlib import Path

from adapter import OstiMongoAdapter
from doi_builder import DoiBuilder
from models import ConnectionModel, OSTIModel

# configuration stuff
config_file = Path(os.path.abspath(__file__)).parent.parent / "files" / "config.yaml"
config = yaml.load(open(config_file.as_posix(), 'r'), Loader=yaml.SafeLoader)

# prepare
oma = OstiMongoAdapter.from_config(config)
elink = ConnectionModel.parse_obj(config["osti"]["elink"])
explorer = ConnectionModel.parse_obj(config["osti"]["explorer"])
elsevier = ConnectionModel.parse_obj(config["elsevier"])
osti = OSTIModel(elink=elink, explorer=explorer, elsevier=elsevier)

# decalre builder instance
send_size = 1
bld = DoiBuilder(oma, osti, send_size=send_size, sync=False)

# run program
bld.run(log_level=logging.DEBUG)

