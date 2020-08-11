import argparse
from mpcite.doi_builder import DoiBuilder
from pathlib import Path
import json
import logging
import time
from monty.json import MontyDecoder


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def main():
    parser = argparse.ArgumentParser(description="Parse Arguments for DOI Builder")
    parser.add_argument(
        "-f",
        "--config_file_path",
        help="File path for the .json config file",
        default="~/Desktop/project/MPCite/files/config_prod.json",
    )
    parser.add_argument(
        "-debug", "--debug", type=str2bool, help="Debug option (T/F)", default="F"
    )
    args = parser.parse_args()
    assert args.config_file_path is not None, "Please provide a configuration file path"
    config_file = Path(args.config_file_path)
    bld: DoiBuilder = json.load(config_file.open("r"), cls=MontyDecoder)
    bld.config_file_path = config_file.as_posix()
    tic = time.perf_counter()
    if args.debug is not None and args.debug:
        bld.run(log_level=logging.DEBUG)
    else:
        bld.run(log_level=logging.INFO)
    toc = time.perf_counter()
    print(f"Program run took {toc - tic:0.4f} seconds")


if __name__ == "__main__":
    main()
