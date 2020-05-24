from adapter import OstiMongoAdapter
from doi_builder import DoiBuilder
import logging, yaml
from pathlib import Path
import os
from models import ConnectionModel, OSTIModel

# configuration stuff
config_file = Path(os.path.abspath(__file__)).parent.parent / "files" / "config.yaml"
config = yaml.load(open(config_file.as_posix(), 'r'), Loader=yaml.SafeLoader)

# prepare
oma = OstiMongoAdapter.from_config(config)
elink = ConnectionModel.parse_obj(config["osti"]["elink"])
explorer = ConnectionModel.parse_obj(config["osti"]["explorer"])
osti = OSTIModel(elink=elink, explorer=explorer)

# decalre builder instance
send_size = 2
bld = DoiBuilder(oma, osti, send_size=send_size, sync=False)

# run program
bld.run(log_level=logging.DEBUG)

