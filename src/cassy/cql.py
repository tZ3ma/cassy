# src/fogd_db/cql.py
"""Cassandra Query Language interface."""
from .casdriv import (
    list_cluster_keyspaces,
    list_keyspace_tables,
    list_table_columns,
    list_table_primary_keys,
)


def create_keyspace(keyspace, session, replication="simple"):
    """Create keyspace via session if neccessary."""
    if replication == "simple":
        replication = "{'class': 'SimpleStrategy', 'replication_factor': 1}"

    query = (
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {replication};"
    )
    session.execute(query)


def get_cluster_name(session):
    """Querry session for current cluster name.

    Parameters
    ----------
    session: cassandra.cluster.Session
        `cassandra.session.Session
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/#cassandra.cluster.Session>`_
        Used to query the cluster for its name.

    Returns
    -------
    str:
        Name of the cassandra cluster, the current session is connected to.
    """
    # infer cluster name
    query = "SELECT cluster_name FROM system.local"
    prepped_query = session.prepare(query)
    rows = session.execute(prepped_query)
    cluster_name = rows.one()["cluster_name"]

    return cluster_name


def list_column_values(cluster, session, keyspace, table, column):
    """List all column values of table in keyspace.

    Parameters
    ----------
    cluster: cassandra.cluster.Cluster
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_
        object holding the cassandra database.
    session: cassandra.cluster.Session, default=None
        `cassandra.session.Session
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/#cassandra.cluster.Session>`_
        used to query the table for clumn values.
    keyspace: str
        String specifying the keyspace of the
        :paramref:`~list_column_values.cluster` where
        :paramref:`~list_column_values.table` is to be found.
    table: str
        String specifying the table inside the
        :paramref:`~list_column_values.keyspace` of
        :paramref:`~list_column_values.cluster` of which the column labels
        are to be listed.
    column: ~numbers.Number, str
        Column specifier of which the values are to be listed.

    Returns
    -------
    list
        List of strings specifying the found column values

    Raises
    ------
    KeyError
        Key Error raised if :paramref:`~list_column_values.keyspace`,
        :paramref:`~list_column_values.table` or
        :paramref:`~list_column_values.column` are unsuccesfully
        white-listed.
    """
    # create white_lists so sanitize tables and keyspaces:
    if keyspace not in list_cluster_keyspaces(cluster):
        raise KeyError(f"Unknown keyspace: '{keyspace}'")
    if table not in list_keyspace_tables(cluster, keyspace):
        raise KeyError(f"Unknown table: '{table}'")
    if column not in list_table_columns(cluster, keyspace, table):
        raise KeyError(f"Unknown column: '{column}'")

    query = f"SELECT {column} FROM {keyspace}.{table}"
    rows = session.execute(query)
    return [result[column] for result in rows]


def get_all_entries(cluster, session, keyspace, table):
    """Get all rows of a table.

    Parameters
    ----------
    cluster: cassandra.cluster.Cluster
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_
        object holding the cassandra database.
    session: cassandra.cluster.Session
        `cassandra.session.Session
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/#cassandra.cluster.Session>`_
        Use to execute query statement.
    keyspace: str
        String specifying the default keyspace the
        :paramref:`~get_all_entries.table` is found.
    table: str
        String specifying the table queried.

    Returns
    -------
    list
        List of dictionairies of found entries.

    Raises
    ------
    KeyError
        Key Error raised if :paramref:`~get_all_entries.keyspace` or
        :paramref:`~get_all_entries.table` are unsuccesfully white-listed.
    """
    # create white_lists so sanitize tables and keyspaces:
    if keyspace not in list_cluster_keyspaces(cluster):
        raise KeyError(f"Unknown keyspace: '{keyspace}'")
    if table not in list_keyspace_tables(cluster, keyspace):
        raise KeyError(f"Unknown table: '{table}'")

    # keyspace and talbe sanitized, so ignore S608
    query = f"SELECT * FROM {keyspace}.{table}"  # noqa: S608
    rows = session.execute(query)
    return [*rows]


def drop_row(cns, keyspace, table, primary_key, value):
    """Drop(delete) a cassandra table row.

    Parameters
    ----------
    cns: ~typing.NamedTuple
        Tuple of `cassandra.session.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/#cassandra.cluster.Cluster>`_
        and `cassandra.session.Session
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/#cassandra.cluster.Session>`_
        as in ``('cluster'=cluster, 'session'=session)`` and returned by
        :func:`cassy.casdriv.connect_session`.
    keyspace: str
        String specifying the default keyspace the
        :paramref:`~drop_row.table` is found.
    table: str
        String specifying the table of which to drop a row.
    primary_key: str
        String specifying/identifying the primary key of the row to be dropped.
    value
        :paramref:`~drop_row.primary_key` value of the row to be dropped.

    Raises
    ------
    KeyError
        Key Error raised if :paramref:`~drop_row.keyspace`,
        :paramref:`~drop_row.table`,
        :paramref:`~drop_row.primary_key`,
        :paramref:`~drop_row.value` are unsuccesfully
        white-listed.

    Examples
    --------
    Dropping a row from the Martin Crawford database assuming a cluster and
    session were created using :func:`cassy.casdriv.connect_session`::

        drop_row(
            cns=(cluster, session),
            keyspace="crawford",
            table="common_fruiting_trees",
            primary_key="latin",
            value="Prunus domestica",
        )
    """
    # create white_lists so sanitize tables and keyspaces:
    if keyspace not in list_cluster_keyspaces(cns.cluster):
        raise KeyError(f"Unknown keyspace: '{keyspace}'")
    if table not in list_keyspace_tables(cns.cluster, keyspace):
        raise KeyError(f"Unknown table: '{table}'")
    if primary_key not in list_table_primary_keys(cns.cluster, keyspace, table):
        raise KeyError(f"Unknown primary key: '{primary_key}'")
    if value not in list_column_values(
        cns.cluster,
        cns.session,
        keyspace,
        table,
        primary_key,
    ):
        raise KeyError(f"Unknown primary key value: '{value}'")

    # query sanitized, so ignore S608
    cns.session.execute(
        f"DELETE FROM {keyspace}.{table} WHERE {primary_key} = '{value}'"  # noqa: S608
    )


def drop_all_rows(cluster, session, keyspace, table):
    """Drop(delete) an entire tables rows/content.

    Parameters
    ----------
    cluster: cassandra.cluster.Cluster
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_
        object holding the cassandra database.
    session: cassandra.cluster.Session
        `cassandra.session.Session
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/#cassandra.cluster.Session>`_
        Use to execute query statement.
    keyspace: str
        String specifying the default keyspace the
        :paramref:`~drop_row.table` is found.
    table: str
        String specifying the table of which to drop all rows.

    Raises
    ------
    KeyError
        Key Error raised if :paramref:`~drop_all_rows.keyspace` or
        :paramref:`~drop_all_rows.table` are unsuccesfully white-listed.
    """
    if keyspace not in list_cluster_keyspaces(cluster):
        raise KeyError(f"Unknown keyspace: '{keyspace}'")
    if table not in list_keyspace_tables(cluster, keyspace):
        raise KeyError(f"Unknown table: '{table}'")

    session.execute(f"TRUNCATE {keyspace}.{table}")
