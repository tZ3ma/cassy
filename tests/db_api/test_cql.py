# tests/db_api/test_cql.py
"""Test succesfull local cassandra db setup, used for further testing."""
from cassy import cql


def test_cluster_name(casdriv_cns):
    """Test correct cluster name getting of casriv."""
    name = cql.get_cluster_name(casdriv_cns.session)
    assert name == "pytest_tmp_cluster"


def test_keyspace_simple_creation(casdriv_cns):
    """Test creating a keyspace using casdriv.create_keyspace."""
    kspace = "pytest_cql_simple_keyspace"
    cql.create_keyspace(
        keyspace=kspace,
        session=casdriv_cns.session,
    )
    keyspaces = casdriv_cns.cluster.metadata.keyspaces
    assert kspace in keyspaces


def test_keyspace_replication_creation(casdriv_cns):
    """Test casdriv.create_keyspace using replication argument."""
    kspace = "pytest_cql_replication_keyspace"
    cql.create_keyspace(
        keyspace=kspace,
        session=casdriv_cns.session,
        replication="{'class': 'SimpleStrategy', 'replication_factor': 1}",
    )
    keyspaces = casdriv_cns.cluster.metadata.keyspaces
    assert kspace in keyspaces


def test_list_column_values(casdriv_cns):
    """Test cql.list_column_values."""
    kspace = "pytest_cql_simple_keyspace"
    table = "cql_list_column_values"

    # create table
    casdriv_cns.session.execute(
        f"""CREATE TABLE IF NOT EXISTS {kspace}.{table} (
            latin text,
            english text,
            german text,
            PRIMARY KEY (latin)
        );"""
    )

    # add values
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('deus_ex', 'not_a_plant', 'wasndas')"""
    )

    latin_values = cql.list_column_values(
        cluster=casdriv_cns.cluster,
        session=casdriv_cns.session,
        keyspace=kspace,
        table=table,
        column="latin",
    )

    assert "deus_ex" in latin_values


def test_list_column_values_exceptions(casdriv_cns):
    """Test cql.list_column_values exception rasising."""
    kspace = "pytest_cql_simple_keyspace"
    table = "cql_list_column_values_exception"

    # create table
    casdriv_cns.session.execute(
        f"""CREATE TABLE IF NOT EXISTS {kspace}.{table} (
            latin text,
            english text,
            german text,
            PRIMARY KEY (latin)
        );"""
    )

    # add values
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('deus_ex', 'not_a_plant', 'wasndas')"""
    )

    # keyspace exception
    kspace_error = table_error = column_error = "no error"
    try:
        cql.list_column_values(
            cluster=casdriv_cns.cluster,
            session=casdriv_cns.session,
            keyspace="not a keyspace",
            table=table,
            column="latin",
        )
    except KeyError as err:
        kspace_error = str(err)

    # table excepton
    try:
        cql.list_column_values(
            cluster=casdriv_cns.cluster,
            session=casdriv_cns.session,
            keyspace=kspace,
            table="not in here",
            column="latin",
        )
    except KeyError as err:
        table_error = str(err)

    # column exceptin
    try:
        cql.list_column_values(
            cluster=casdriv_cns.cluster,
            session=casdriv_cns.session,
            keyspace=kspace,
            table=table,
            column=";*/ my injection",
        )
    except KeyError as err:
        column_error = str(err)

    assert "Unknown keyspace: 'not a keyspace'" in kspace_error
    assert "Unknown table: 'not in here'" in table_error
    assert "Unknown column: ';*/ my injection'" in column_error


def test_get_all_entries(casdriv_cns):
    """Test cql.get_all_entries."""
    kspace = "pytest_cql_simple_keyspace"
    table = "cql_get_all_entries"

    # create table
    casdriv_cns.session.execute(
        f"""CREATE TABLE IF NOT EXISTS {kspace}.{table} (
            latin text,
            english text,
            german text,
            PRIMARY KEY (latin)
        );"""
    )

    # add values
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('deus_ex', 'not_a_plant', 'wasndas')"""
    )
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('machina', 'also_not_a_plant', 'wasndas2')"""
    )

    all_entries = cql.get_all_entries(
        cluster=casdriv_cns.cluster,
        session=casdriv_cns.session,
        keyspace=kspace,
        table=table,
    )
    expected_result = [
        {
            "latin": "deus_ex",
            "english": "not_a_plant",
            "german": "wasndas",
        },
        {
            "latin": "machina",
            "english": "also_not_a_plant",
            "german": "wasndas2",
        },
    ]

    for dict_result in expected_result:
        assert dict_result in all_entries


def test_get_all_entries_exceptions(casdriv_cns):
    """Test cql.get_all_entries exception rasising."""
    kspace = "pytest_cql_simple_keyspace"
    table = "cql_get_all_entries_exception"

    # create table
    casdriv_cns.session.execute(
        f"""CREATE TABLE IF NOT EXISTS {kspace}.{table} (
            latin text,
            english text,
            german text,
            PRIMARY KEY (latin)
        );"""
    )

    # add values
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('deus_ex', 'not_a_plant', 'wasndas')"""
    )

    # keyspace exception
    kspace_error = table_error = "no error"
    try:
        cql.get_all_entries(
            cluster=casdriv_cns.cluster,
            session=casdriv_cns.session,
            keyspace="not a keyspace",
            table=table,
        )
    except KeyError as err:
        kspace_error = str(err)

    # table exception
    try:
        cql.get_all_entries(
            cluster=casdriv_cns.cluster,
            session=casdriv_cns.session,
            keyspace=kspace,
            table="not in here",
        )
    except KeyError as err:
        table_error = str(err)

    assert "Unknown keyspace: 'not a keyspace'" in kspace_error
    assert "Unknown table: 'not in here'" in table_error


def test_drop_row(casdriv_cns):
    """Test cql.drop_row."""
    kspace = "pytest_cql_simple_keyspace"
    table = "cql_drop_row"

    # create table
    casdriv_cns.session.execute(
        f"""CREATE TABLE IF NOT EXISTS {kspace}.{table} (
            latin text,
            english text,
            german text,
            PRIMARY KEY (latin)
        );"""
    )

    # add values
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('deus_ex', 'not_a_plant', 'wasndas')"""
    )
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('machina', 'also_not_a_plant', 'wasndas2')"""
    )

    cql.drop_row(
        cns=casdriv_cns,
        keyspace=kspace,
        table=table,
        primary_key="latin",
        value="machina",
    )

    all_entries = cql.get_all_entries(
        cluster=casdriv_cns.cluster,
        session=casdriv_cns.session,
        keyspace=kspace,
        table=table,
    )

    expected_result = [
        {
            "latin": "deus_ex",
            "english": "not_a_plant",
            "german": "wasndas",
        },
    ]
    assert expected_result == all_entries


def test_drop_rows_exceptions(casdriv_cns):
    """Test cql.list_column_values exception rasising."""
    kspace = "pytest_cql_simple_keyspace"
    table = "cql_drop_rows_exception"

    # create table
    casdriv_cns.session.execute(
        f"""CREATE TABLE IF NOT EXISTS {kspace}.{table} (
            latin text,
            english text,
            german text,
            PRIMARY KEY (latin)
        );"""
    )

    # add values
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('deus_ex', 'not_a_plant', 'wasndas')"""
    )

    kspace_error = table_error = pk_error = pkv_error = "no error yet"
    # keyspace exception
    try:
        cql.drop_row(
            cns=casdriv_cns,
            keyspace="not a keyspace",
            table=table,
            primary_key="latin",
            value="machina",
        )
    except KeyError as err:
        kspace_error = str(err)

    # table exception
    try:
        cql.drop_row(
            cns=casdriv_cns,
            keyspace=kspace,
            table="not in here",
            primary_key="latin",
            value="machina",
        )
    except KeyError as err:
        table_error = str(err)

    # primary key exception
    try:
        cql.drop_row(
            cns=casdriv_cns,
            keyspace=kspace,
            table=table,
            primary_key=";*/ my injection",
            value="machina",
        )
    except KeyError as err:
        pk_error = str(err)

    # primary key value exception
    try:
        cql.drop_row(
            cns=casdriv_cns,
            keyspace=kspace,
            table=table,
            primary_key="latin",
            value="*/;status=admin",
        )
    except KeyError as err:
        pkv_error = str(err)

    assert "Unknown keyspace: 'not a keyspace'" in kspace_error
    assert "Unknown table: 'not in here'" in table_error
    assert "Unknown primary key: ';*/ my injection'" in pk_error
    assert "Unknown primary key value: '*/;status=admin'" in pkv_error


def test_drop_all_rows(casdriv_cns):
    """Test cql.drop_row."""
    kspace = "pytest_cql_simple_keyspace"
    table = "cql_drop_all_rows"

    # create table
    casdriv_cns.session.execute(
        f"""CREATE TABLE IF NOT EXISTS {kspace}.{table} (
            latin text,
            english text,
            german text,
            PRIMARY KEY (latin)
        );"""
    )

    # add values
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('deus_ex', 'not_a_plant', 'wasndas')"""
    )
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('machina', 'also_not_a_plant', 'wasndas2')"""
    )

    cql.drop_all_rows(
        cluster=casdriv_cns.cluster,
        session=casdriv_cns.session,
        keyspace=kspace,
        table=table,
    )

    all_entries = cql.get_all_entries(
        cluster=casdriv_cns.cluster,
        session=casdriv_cns.session,
        keyspace=kspace,
        table=table,
    )

    # assert all_entries is an empty list
    assert not all_entries


def test_drop_all_rows_exceptions(casdriv_cns):
    """Test cql.drop_all_rows exception rasising."""
    kspace = "pytest_cql_simple_keyspace"
    table = "cql_drop_all_rows_exception"

    # create table
    casdriv_cns.session.execute(
        f"""CREATE TABLE IF NOT EXISTS {kspace}.{table} (
            latin text,
            english text,
            german text,
            PRIMARY KEY (latin)
        );"""
    )

    # add values
    casdriv_cns.session.execute(
        f"""INSERT INTO {kspace}.{table} (
            latin,
            english,
            german
        ) VALUES ('deus_ex', 'not_a_plant', 'wasndas')"""
    )

    kspace_error = table_error = "no error"
    # keyspace exception
    try:
        cql.drop_all_rows(
            cluster=casdriv_cns.cluster,
            session=casdriv_cns.session,
            keyspace="not a keyspace",
            table=table,
        )
    except KeyError as err:
        kspace_error = str(err)

    # table exception
    try:
        cql.drop_all_rows(
            cluster=casdriv_cns.cluster,
            session=casdriv_cns.session,
            keyspace=kspace,
            table="not in here",
        )
    except KeyError as err:
        table_error = str(err)

    assert "Unknown keyspace: 'not a keyspace'" in kspace_error
    assert "Unknown table: 'not in here'" in table_error
