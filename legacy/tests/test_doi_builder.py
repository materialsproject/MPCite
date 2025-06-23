from pathlib import Path
import os
import pytest
import json
from mpcite.doi_builder import DoiBuilder
from mpcite.models import OSTIModel, MongoConnectionModel, ConnectionModel


@pytest.fixture
def config_file_path():
    return Path(os.getcwd()) / "files" / "config_test.json"


def test_builder_serialization(config_file_path: Path):
    config_file = config_file_path.open("r")

    # test deserialize
    d: dict = json.load(config_file)
    try:
        doi_builder = DoiBuilder.from_dict(d=d)
    except Exception as e:
        assert False, f"Unable to build DOI Builder from config file. Error: {e}"

    # test serialization

    new_d = doi_builder.as_dict()

    assert new_d.keys() == d.keys()

    new_osti = OSTIModel.parse_obj(new_d["osti"])
    new_elsevier = ConnectionModel.parse_obj(new_d["elsevier"])
    new_materials_connection = MongoConnectionModel.parse_obj(
        new_d["materials_collection"]
    )
    new_dois_collection_connection = MongoConnectionModel.parse_obj(
        new_d["dois_collection"]
    )
    new_robocrys_collection_connection = MongoConnectionModel.parse_obj(
        new_d["robocrys_collection"]
    )

    true_osti = OSTIModel.parse_obj(d["osti"])
    true_elsevier = ConnectionModel.parse_obj(new_d["elsevier"])
    true_materials_connection = MongoConnectionModel.parse_obj(
        new_d["materials_collection"]
    )
    true_dois_collection_connection = MongoConnectionModel.parse_obj(
        new_d["dois_collection"]
    )
    true_robocrys_collection_connection = MongoConnectionModel.parse_obj(
        new_d["robocrys_collection"]
    )

    assert new_d["max_doi_requests"] == d["max_doi_requests"]
    assert new_d["sync"] == d["sync"]
    assert new_osti.dict() == true_osti.dict()
    assert new_elsevier.dict() == true_elsevier.dict()
    assert new_materials_connection.dict() == true_materials_connection.dict()
    assert (
        new_dois_collection_connection.dict() == true_dois_collection_connection.dict()
    )
    assert new_robocrys_collection_connection == true_robocrys_collection_connection
