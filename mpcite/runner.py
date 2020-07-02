import logging
import yaml
from pathlib import Path
import os
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
max_doi_requests = 0
sync = True
should_sync_all_materials = True  # if false, it will only sync the ones in existing local DOI collection
bld = DoiBuilder(oma,
                 osti,
                 max_doi_requests=max_doi_requests,
                 sync=sync)

# run program
import time
tic = time.perf_counter()
bld.run(log_level=logging.DEBUG)
toc = time.perf_counter()
print(f"Program run took {toc - tic:0.4f} seconds")

