from pathlib import Path
import os
import pytest
import json
from mpcite.doi_builder import DoiBuilder


@pytest.fixture
def config_file_path():
    return Path(os.getcwd()) / "files" / "config.json"


def test_builder_serialization(config_file_path: Path):
    config_file = config_file_path.open("r")

    # test deserialize
    d = json.load(config_file)
    try:
        doi_builder = DoiBuilder.from_dict(d=d)
    except Exception as e:
        assert False, f"Unable to build DOI Builder from config file. Error: {e}"

    # test serialization
    new_d = doi_builder.as_dict()
    assert d == new_d, "Serialized result is different from truth"
