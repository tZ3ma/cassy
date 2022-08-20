# tests/db_api/test_casdriv.py
"""Test succesfull local cassandra db setup, used for further testing."""
import pytest
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

import cassy.casdriv as cassy_driv


class Plant(Model):
    """Plant Dataclass, ready to be cassandrad.

    Parameters
    ----------
    latin: str
        String specifying the latin plant name. Used as the primary key
    english: str, ~collections.abc.Container
        String, or container of strings, specifying the english trivial names.
    german: str, ~collections.abc.Container
        String, or container of strings, specifying the german trivial names.
    """

    latin = columns.Text(primary_key=True, required=True)
    english = columns.Text(required=False)
    german = columns.Text(required=False)


class ClusterPlant(Model):
    """Plant Dataclass, ready to be cassandrad.

    Has additional clustering keys sorted in ascending order.

    Parameters
    ----------
    latin: str
        String specifying the latin plant name. Used as the primary key
    english: str, ~collections.abc.Container
        String, or container of strings, specifying the english trivial names.
    german: str, ~collections.abc.Container
        String, or container of strings, specifying the german trivial names.
    """

    latin = columns.Text(primary_key=True, required=True)
    english = columns.Text(primary_key=True, clustering_order="ASC")
    german = columns.Text(required=False)


def test_cluster_name(casdriv_cns):
    """Test correct cluster name getting of casriv."""
    name = cassy_driv.get_cluster_name(casdriv_cns.cluster)
    assert name == "pytest_tmp_cluster"


@pytest.mark.dependency()
def test_keyspace_simple_creation(casdriv_cns):
    """Test creating a keyspace using casdriv."""
    kspace = "pytest_casdriv_simple_keyspace"
    cassy_driv.create_simple_keyspace(
        name=kspace,
        replication_factor=1,
        connections=[casdriv_cns.cluster.metadata.cluster_name],
    )
    keyspaces = casdriv_cns.cluster.metadata.keyspaces
    assert kspace in keyspaces


@pytest.mark.dependency(depends=["test_keyspace_simple_creation"])
def test_list_cluster_keyspaces(casdriv_cns):
    """Test casdriv list_cluster_keyspaces utility."""
    listed = cassy_driv.list_cluster_keyspaces(casdriv_cns.cluster)
    kspace = "pytest_casdriv_simple_keyspace"
    assert kspace in listed


@pytest.mark.dependency(depends=["test_keyspace_simple_creation"])
def test_create_entry(casdriv_cns):
    """Test creating a db entry using create_entry."""
    kspace = "pytest_casdriv_simple_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)

    data_dict = {
        "latin": "lupulus",
        "english": "wolve",
        "german": "Wolf",
    }

    cassy_driv.create_entry(
        model=Plant,
        data=data_dict,
        prior_syncing=True,
        keyspace=kspace,
        con=connection,
    )

    keyspace_tables = cassy_driv.list_keyspace_tables(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
    )
    assert "plant" in keyspace_tables


@pytest.mark.dependency(depends=["test_create_entry"])
def test_create_entry_without_syncing(casdriv_cns):
    """Test creating a db entry using create_entry."""
    kspace = "pytest_casdriv_simple_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)

    data_dict = {
        "latin": "lupulus",
        "english": "wolve",
        "german": "Wolf",
    }

    cassy_driv.create_entry(
        model=Plant,
        data=data_dict,
        keyspace=kspace,
        con=connection,
    )

    keyspace_tables = cassy_driv.list_keyspace_tables(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
    )
    assert "plant" in keyspace_tables


@pytest.mark.dependency(depends=["test_keyspace_simple_creation"])
def test_create_cluster_entry(casdriv_cns):
    """Test creating a db entry using create_entry and clustering keys."""
    kspace = "pytest_casdriv_simple_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)

    data_dict = {
        "latin": "Lupulus Lupin",
        "english": "Hops",
        "german": "Hopfen",
    }

    # set ksapce and con manually to test resepctive if clauses
    ClusterPlant.__keyspace__ = kspace
    ClusterPlant.__connection__ = connection

    cassy_driv.create_entry(
        model=ClusterPlant,
        data=data_dict,
        prior_syncing=True,
    )

    keyspace_tables = cassy_driv.list_keyspace_tables(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
    )
    assert "cluster_plant" in keyspace_tables

    hops_entry = cassy_driv.read_entry(
        model=ClusterPlant,
        primary_keys=("latin", "english"),
        values=("Lupulus Lupin", "Hops"),
    )
    assert data_dict["latin"] == hops_entry.latin
    assert data_dict["latin"] != hops_entry.english
    # pylint: disable=protected-access
    assert "english" in hops_entry._clustering_keys
    # pylint: enable=protected-access

    # reset ksapce and con to not influence other tests
    ClusterPlant.__keyspace__ = None
    ClusterPlant.__connection__ = None


@pytest.mark.dependency(depends=["test_create_entry"])
def test_read_entry(casdriv_cns):
    """Test creating a db entry using create_entry."""
    kspace = "pytest_casdriv_simple_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)

    data_dict = {
        "latin": "test_read_entry",
        "english": "that was english",
        "german": "Teste Eintrag lesen",
    }

    cassy_driv.create_entry(
        model=Plant,
        data=data_dict,
        prior_syncing=True,
        keyspace=kspace,
        con=connection,
    )

    keyspace_tables = cassy_driv.list_keyspace_tables(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
    )
    assert "plant" in keyspace_tables

    # use keyspace and con arguments to test if clauses
    entry = cassy_driv.read_entry(
        model=Plant,
        primary_keys="latin",
        values="test_read_entry",
        keyspace=kspace,
        con=connection,
    )
    assert "Teste Eintrag lesen" in entry.german


@pytest.mark.dependency(depends=["test_create_entry"])
def test_read_entry_exception(casdriv_cns):
    """Test creating a db entry using create_entry."""
    kspace = "pytest_casdriv_simple_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)

    data_dict = {
        "latin": "test_read_entry",
        "english": "that was english",
        "german": "Teste Eintrag lesen",
    }

    cassy_driv.create_entry(
        model=Plant,
        data=data_dict,
        prior_syncing=True,
        keyspace=kspace,
        con=connection,
    )

    keyspace_tables = cassy_driv.list_keyspace_tables(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
    )
    assert "plant" in keyspace_tables

    # use keyspace and con arguments to test if clauses
    msg = "no error yet"
    try:
        cassy_driv.read_entry(
            model=Plant,
            primary_keys=1,
            values=2,
            keyspace=kspace,
            con=connection,
        )
    except ValueError as err:
        msg = str(err)

    assert "'primary_keys' argument must be of type str or tuple" in msg


@pytest.mark.dependency(depends=["test_create_entry"])
def test_list_primary_keys(casdriv_cns):
    """Test creating a db entry using create_entry."""
    kspace = "pytest_casdriv_simple_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)

    data_dict = {
        "latin": "test_list_primary_keys",
        "english": "that was english",
        "german": "Teste Primaerschluessel auflisten",
    }

    cassy_driv.create_entry(
        model=Plant,
        data=data_dict,
        prior_syncing=True,
        keyspace=kspace,
        con=connection,
    )

    prim_keys = cassy_driv.list_table_primary_keys(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
        table="plant",
    )
    assert "latin" in prim_keys


@pytest.mark.dependency(depends=["test_create_entry"])
def test_list_table_columns(casdriv_cns):
    """Test creating a db entry using create_entry."""
    kspace = "pytest_casdriv_simple_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)

    data_dict = {
        "latin": "test_list_primary_keys",
        "english": "that was english",
        "german": "Teste Primaerschluessel auflisten",
    }

    cassy_driv.create_entry(
        model=Plant,
        data=data_dict,
        prior_syncing=True,
        keyspace=kspace,
        con=connection,
    )

    prim_keys = cassy_driv.list_table_columns(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
        table="plant",
    )
    assert ["latin", "english", "german"] == list(prim_keys)


@pytest.mark.dependency(depends=["test_create_entry"])
def test_delete_entry(casdriv_cns):
    """Test deleting a db entry using create_entry."""
    kspace = "pytest_casdriv_delete_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)
    model_used = Plant

    # create keyspace
    cassy_driv.create_simple_keyspace(
        name=kspace,
        replication_factor=1,
        connections=[casdriv_cns.cluster.metadata.cluster_name],
    )

    # assert succesfull creation
    keyspaces = casdriv_cns.cluster.metadata.keyspaces
    assert kspace in keyspaces

    data_dict = {
        "latin": "test_delete_entry",
        "english": "that was english",
        "german": "Teste Eintrag löschen",
    }
    data_dict2 = {
        "latin": "test_not-delete_entry",
        "english": "that was english",
        "german": "Teste Eintrag nicht löschen",
    }

    data_dict3 = {
        "latin": "test_also-delete_entry",
        "english": "that was english",
        "german": "Teste Eintrag auch löschen",
    }

    # create db entries
    for dicts in [data_dict, data_dict2, data_dict3]:
        cassy_driv.create_entry(
            model=model_used,
            data=dicts,
            prior_syncing=True,
            keyspace=kspace,
            con=connection,
        )

    # assert successful entry creation
    keyspace_tables = cassy_driv.list_keyspace_tables(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
    )
    assert "plant" in keyspace_tables

    # assert succesfull to be deleted entry recieving
    entries = cassy_driv.get_all_entries(
        model=model_used,
        keyspace=kspace,
        con=connection,
    )
    del_this = cassy_driv.read_entry(
        model=Plant,
        primary_keys="latin",
        values="test_delete_entry",
        keyspace=kspace,
        con=connection,
    )
    assert del_this in entries

    # delete entry
    cassy_driv.delete_entry(
        model=model_used,
        primary_keys="latin",
        values="test_delete_entry",
        keyspace=kspace,
        con=connection,
    )
    after_del_entries = cassy_driv.get_all_entries(
        model=model_used,
        keyspace=kspace,
        con=connection,
    )

    # assert succesfull deletion
    assert del_this not in after_del_entries

    # delete using default arguments:
    also_del_this = cassy_driv.read_entry(
        model=Plant,
        primary_keys="latin",
        values="test_also-delete_entry",
        keyspace=kspace,
        con=connection,
    )
    assert also_del_this in entries

    model_used.__connection__ = connection
    model_used.__keyspace__ = kspace

    # delete entry
    cassy_driv.delete_entry(
        model=model_used,
        primary_keys="latin",
        values="test_also-delete_entry",
    )
    after_del_entries2 = cassy_driv.get_all_entries(
        model=model_used,
    )

    # reset model to not interfere with other tests
    model_used.__connection__ = None
    model_used.__keyspace__ = None

    # assert succesfull deletion
    assert also_del_this not in after_del_entries2


@pytest.mark.dependency(depends=["test_create_entry"])
def test_get_all_entries(casdriv_cns):
    """Test deleting a db entry using casdriv.delete_entry."""
    kspace = "pytest_casdriv_getall_keyspace"
    connection = cassy_driv.get_cluster_name(casdriv_cns.cluster)
    model_used = Plant

    # create keyspace
    cassy_driv.create_simple_keyspace(
        name=kspace,
        replication_factor=1,
        connections=[casdriv_cns.cluster.metadata.cluster_name],
    )

    # assert succesfull creation
    keyspaces = casdriv_cns.cluster.metadata.keyspaces
    assert kspace in keyspaces

    data_dict = {
        "latin": "test_getall_entry",
        "english": "that was english",
        "german": "Teste Alle Eintraege lesen",
    }
    data_dict2 = {
        "latin": "test_getall_entry2",
        "english": "that was english",
        "german": "Teste Alle Eintraege lesen2",
    }

    # create db entries
    for dicts in [data_dict, data_dict2]:
        cassy_driv.create_entry(
            model=model_used,
            data=dicts,
            prior_syncing=True,
            keyspace=kspace,
            con=connection,
        )

    # assert successful entry creation
    keyspace_tables = cassy_driv.list_keyspace_tables(
        cluster=casdriv_cns.cluster,
        keyspace=kspace,
    )
    assert "plant" in keyspace_tables

    # assert succesfull entry recieving using keyspace and con
    expected_values = [data_dict["latin"], data_dict2["latin"]]
    entries = cassy_driv.get_all_entries(
        model=model_used,
        keyspace=kspace,
        con=connection,
    )

    latin_values = [mod.latin for mod in entries]
    for expected_value in expected_values:
        assert expected_value in latin_values

    # assert succesfull entry recieving NOT using keyspace and con
    model_used.__connection__ = connection
    model_used.__keyspace__ = kspace

    entries = cassy_driv.get_all_entries(
        model=model_used,
    )

    # reset model values to not interfere with other tests
    model_used.__connection__ = None
    model_used.__keyspace__ = None

    latin_values = [mod.latin for mod in entries]
    for expected_value in expected_values:
        assert expected_value in latin_values
