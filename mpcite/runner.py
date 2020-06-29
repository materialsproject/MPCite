import logging
import os
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
send_size = 0
should_sync_from_remote_sites = True
should_sync_all_materials = True  # if false, it will only sync the ones in existing local DOI collection
should_register_new_DOI = False
log_folder_path = Path(os.getcwd()).parent / "files"
bld = DoiBuilder(oma,
                 osti,
                 send_size=send_size,
                 should_sync_from_remote_sites=should_sync_from_remote_sites,
                 should_register_new_DOI=should_register_new_DOI,
                 should_sync_all_materials=should_sync_all_materials,
                 log_folder_path=log_folder_path.as_posix())

# run program
import time
tic = time.perf_counter()
bld.run(log_level=logging.DEBUG)
toc = time.perf_counter()
print(f"Program run took {toc - tic:0.4f} seconds")

