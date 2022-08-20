# tests/db_api/test_model.py
"""Test creating dynamic models using cassy."""
import os
import tempfile
from pathlib import Path

import cassy.casdriv as cassy_driv
from cassy.model import MetaModel, create_data_model, retrieve_data_model

ccfts_mm = MetaModel(
    name="CrawfordCommonFruitingTrees",
    primary_keys={"Latin": "Text"},
    clustering_keys={
        "English": ("Text", "ASC"),
        "German": ("Text", "ASC"),
    },
    columns={"USDA_Hardiness": "Integer"},
)

ccfts_mm2 = MetaModel(
    name="CrawfordCommonFruitingTrees",
    primary_keys={"Latin": "Text"},
    clustering_keys={
        "English": ("Text", "ASC"),
        "German": ("Text", "ASC"),
    },
    columns={
        "USDA_Hardiness": "Integer",
        "shade_tolerance": "Float",
    },
)


def test_default_model_creation(tmp_path):
    """Test cassy dynamic model creation using default arguments."""
    return_path = create_data_model(
        meta_model=ccfts_mm, path=tmp_path / "pytest_tmp_model.py"
    )
    expected_path = tmp_path / "pytest_tmp_model.py"
    assert return_path == expected_path
    assert expected_path.is_file()


def test_model_creation_overwrite(tmp_path):
    """Test cassy dynamic model creation overwriting."""
    create_data_model(meta_model=ccfts_mm, path=tmp_path / "pytest_tmp_model.py")

    # unsuccesfull overwrite attempt:
    file_exists_error_msg = ""
    try:
        create_data_model(meta_model=ccfts_mm, path=tmp_path / "pytest_tmp_model.py")

    # pylint: disable=broad-except
    except Exception as err:
        file_exists_error_msg = str(err)
    # pylint: enable=broad-except

    fix_msg = "Set 'overwrite=True' or change filepath"

    assert fix_msg in file_exists_error_msg

    # succesfull overwrite w/o backup
    return_path = create_data_model(
        meta_model=ccfts_mm2,
        path=tmp_path / "pytest_tmp_model.py",
        overwrite=True,
        backup=False,
    )

    assert return_path.is_file()

    # succesfull overwrite w/ backup
    return_path = create_data_model(
        meta_model=ccfts_mm2,
        path=tmp_path / "pytest_tmp_model.py",
        overwrite=True,
        backup=True,
    )

    assert return_path.is_file()

    dir_list = os.listdir(tmp_path)
    assert any("pytest_tmp_model_backup_" in entry for entry in dir_list)


def test_create_model_design_case():
    """Test creating a meta model in a home folder located tempdir."""
    home = Path("~").expanduser()
    tempfile.tempdir = home
    with tempfile.TemporaryDirectory() as tempdirname:
        return_path = create_data_model(
            meta_model=ccfts_mm,
            path=os.path.join("~", tempdirname, "pytest_home_model.py"),
        )

        assert return_path.is_file()
        expected_path = home / tempdirname / "pytest_home_model.py"

    assert expected_path == return_path


def test_retrieve_model(tmp_path):
    """Test retrieving a prior created model."""
    return_path = create_data_model(
        meta_model=ccfts_mm, path=tmp_path / "pytest_retrieve_model.py"
    )
    expected_path = tmp_path / "pytest_retrieve_model.py"
    assert return_path == expected_path
    assert expected_path.is_file()

    model = retrieve_data_model(
        path=expected_path,
        model_name=ccfts_mm.name,
    )

    expected_primary_keys = [
        *ccfts_mm.primary_keys.keys(),
        *ccfts_mm.clustering_keys.keys(),
    ]

    expected_clustering_keys = list(ccfts_mm.clustering_keys.keys())

    expected_columns = [
        *expected_primary_keys,
        *ccfts_mm.columns.keys(),
    ]

    # pylint: disable=protected-access
    assert model.__table_name__ == ccfts_mm.name
    assert list(model._primary_keys.keys()) == expected_primary_keys
    assert list(model._clustering_keys.keys()) == expected_clustering_keys
    assert list(model._columns.keys()) == expected_columns
    # pylint: enable=protected-access


def test_create_retrieve_store_load(tmp_path, casdriv_cns):
    """Test full create -> retrieve -> store -> load cycle."""
    # 1. "Dynamically" create hardcoded data model
    create_data_model(
        meta_model=ccfts_mm,
        path=tmp_path / "pytest_fullcycle_model.py",
    )
    expected_path = tmp_path / "pytest_fullcycle_model.py"

    # 2. Retrieve hardcoded data model
    retrieved_model = retrieve_data_model(
        path=expected_path,
        model_name=ccfts_mm.name,
    )

    # 3. Setup connection and keyspace
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)
    kspace = "pytest_casdriv_fullcycle_keyspace"
    cassy_driv.create_simple_keyspace(
        name=kspace,
        replication_factor=1,
        connections=[connection],
    )

    # 4. Create an entry into the database using the retrieved model
    data_dict = {
        "Latin": "quad erat demonstrandum",
        "English": "full cycle plant",
        "German": "schreiben lesen speichern laden",
        "USDA_Hardiness": 9000,
    }

    cassy_driv.create_entry(
        model=retrieved_model,
        data=data_dict,
        prior_syncing=True,
        keyspace=kspace,
        con=connection,
    )

    # 5. Load the entry from the database
    entry = cassy_driv.read_entry(
        model=retrieved_model,
        primary_keys="Latin",
        values="quad erat demonstrandum",
        keyspace=kspace,
        con=connection,
    )

    assert "schreiben lesen speichern laden" in entry.German


# [ ] - test_create_retrieve_store_load aka design case


# def test_create_entry(casdriv_cns):
#     """Test creating a db entry using create_entry."""
#     kspace = "pytest_casdriv_simple_keyspace"
#     connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)

#     data_dict = {
#         "latin": "lupulus",
#         "english": "wolve",
#         "german": "Wolf",
#     }

#     cassy_driv.create_entry(
#         model=Plant,
#         data=data_dict,
#         prior_syncing=True,
#         keyspace=kspace,
#         con=connection,
#     )

#     keyspace_tables = cassy_driv.list_keyspace_tables(
#         cluster=casdriv_cns.cluster,
#         keyspace=kspace,
#     )
#     assert "plant" in keyspace_tables
