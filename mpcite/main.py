import argparse
from mpcite.doi_builder import DoiBuilder
from pathlib import Path
import json
import logging
import time


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
    parser = argparse.ArgumentParser(description='Parse Arguments for DOI Builder')
    parser.add_argument('-f', "--config_file_path", help="File path for the .json config file")
    parser.add_argument("-debug", "--debug", type=str2bool, help="Debug option (T/F)")
    args = parser.parse_args()
    assert args.config_file_path is not None, "Please provide a configuration file path"
    config_file = Path(args.config_file_path)
    settings = json.load(config_file.open("r"))

    bld = DoiBuilder.from_dict(d=settings)
    tic = time.perf_counter()
    if args.debug is not None and args.debug:
        bld.run(log_level=logging.DEBUG)
    else:
        bld.run(log_level=logging.INFO)
    toc = time.perf_counter()
    print(f"Program run took {toc - tic:0.4f} seconds")


if __name__ == '__main__':
    main()
