# src/fogd_db/casdriv.py
"""Cassandra-Driver CRUD interface."""
from collections import abc, namedtuple

import cassandra.cluster
import cassandra.cqlengine.management as cql_manage
from cassandra.cqlengine import connection


def connect_session(ips=("127.0.0.1",), port=9042, **kwargs):
    """Connect and return session using :mod:`fogd_db.casdb`.

    Parameters
    ----------
    ips: ~collections.abc.Container
        Container holding contact_points to connect the
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_.
        Defaults to ("127.0.0.1",), which is the local host.

    port: ~numbers.Number
        The server-side port to open connections to. Defaults to 9042.

    kwargs
        Additional arguments relegated to
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_

    Returns
    -------
    ~typing.NamedTuple
        Tuple of `cassandra.session.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/#cassandra.cluster.Cluster>`_
        and `cassandra.session.Session
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/#cassandra.cluster.Session>`_
        as in ``('cluster'=cluster, 'session'=session)``.
    """
    # create cluster object
    cluster = cassandra.cluster.Cluster(contact_points=ips, port=port, **kwargs)

    # and connect non keyspace session
    session = cluster.connect()

    # infer cluster name
    cluster_name = get_cluster_name(cluster)

    # register connection using the inferred cluster name
    connection.register_connection(cluster_name, session=session)
    return namedtuple("CnS", ["cluster", "session"])(cluster, session)


def create_simple_keyspace(
    name, replication_factor, durable_writes=True, connections=None
):
    """Create a keyspace with SimpleStrategy for replica placement.

    If the keyspace already exists, it will not be modified. Basically a very
    close wrapper of `cassandra driver's cqlengine functionality
    <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/management/>`_

    Parameters
    ----------
    name: str
        name of keyspace to create
    replication_factor: int
        keyspace replication factor, used with SimpleStrategy
    durable_writes: bool, default=True
        Write log is bypassed if set to False.
    connections: list
        List of connection names.
    """
    cql_manage.create_keyspace_simple(
        name=name,
        replication_factor=replication_factor,
        durable_writes=durable_writes,
        connections=connections,
    )


def create_entry(model, data, prior_syncing=False, keyspace=None, con=None):
    """Create a new entry using a python data class model.

    Parameters
    ----------
    model
        One of the cassandra python-driver `data class models
        <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_.
    data: dict
        Keyword value pairings of the data to be added. Most conform to the
        :paramref:`~create_entry.model` used.
    prior_syncing: bool, default=False
        If ``True`` :func:`synchronize_model` is called before creation,
        to synchronize database table and model. (Required if you call
        ``model.create`` for the first time, or change any model attributes.
    keyspace: str, None, default=None
        String to specify the keyspace the table is created. If ``None``,
        ``model.__keyspace__`` is used.
    con: str, None, default=None
        String to specify the connection name  the table is created with.
        :mod:`casdriv` uses :attr:`cluster name <get_cluster_name>` as
        default connection name.

        If ``None``, ``model.__connection__`` is used.

    Returns
    -------
    model class
        Instance of the :paramref:`~create_entry.model` created using
        :paramref:`~create_entry.data`.
    """
    if keyspace:
        model.__keyspace__ = keyspace
    if con:
        model.__connection__ = con

    if prior_syncing:
        synchronize_model(model)

    return model.create(**data)


def delete_entry(model, primary_keys, values, keyspace=None, con=None):
    """Delete an existing entry using a python data class model.

    Parameters
    ----------
    model
        One of the cassandra python-driver `data class models
        <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_.
    primary_keys: str, tuple
        String or tuple of strings specifying the label/schema of the primary
        key(s) to be read.
    values: str, tuple
        String or tuple of strings specifying the value of the primary key to
        be queried for.
    keyspace: str, None, default=None
        String to specify the keyspace the table is created. If ``None``,
        ``model.__keyspace__`` is used.
    con: str, None, default=None
        String to specify the connection name  the table is created with.
        :mod:`casdriv` uses :attr:`cluster name <get_cluster_name>` as
        default connection name.

        If ``None``, ``model.__connection__`` is used.
    """
    entry = read_entry(model, primary_keys, values, keyspace, con)
    entry.delete()


def get_all_entries(model, keyspace=None, con=None):
    """Get all entries of an existing data class model table.

    Uses the `cassandra python-driver queries
    <https://docs.datastax.com/en/developer/python-driver/3.25/cqlengine/queryset/#retrieving-objects-with-filters>`_

    Parameters
    ----------
    model
        One of the cassandra python-driver `data class models
        <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_.
    keyspace: str, None, default=None
        String to specify the keyspace the table is created. If ``None``,
        ``model.__keyspace__`` is used.
    con: str, None, default=None
        String to specify the connection name  the table is created with.
        :mod:`casdriv` uses :attr:`cluster name <get_cluster_name>` as
        default connection name.

        If ``None``, ``model.__connection__`` is used.

    Returns
    -------
    list
        List of table entries created via a cassandra python-driver
        `data class models
        <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_.
    """
    if keyspace:
        model.__keyspace__ = keyspace
    if con:
        model.__connection__ = con
    return list(model.objects().all())


def get_cluster_name(cluster):
    """Return clusters registered name.

    Parameters
    ----------
    cluster: cassandra.cluster.Cluster
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_
        object holding the cassandra database.

    Returns
    -------
    str:
        cluster.metadata.cluster_name
    """
    return cluster.metadata.cluster_name


def list_keyspace_tables(cluster, keyspace):
    """List all tables present in keyspaces.

    Parameters
    ----------
    cluster: cassandra.cluster.Cluster
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_
        object holding the cassandra database.
    keyspace: str
        String specifying the keyspace of the cluster of which all existing
        tables are to be listed

    Returns
    -------
    list
        List of strings specifying the tables currently present inside the
        :paramref:`~list_keyspace_tables.keyspace` of
        :paramref:`~list_keyspace_tables.cluster`.
    """
    tables = cluster.metadata.keyspaces[keyspace].tables
    return list(tables.keys())


def list_cluster_keyspaces(cluster):
    """List all keyspaces present in cluster.

    Parameters
    ----------
    cluster: cassandra.cluster.Cluster
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_
        object holding the cassandra database.

    Returns
    -------
    list
        List of strings specifying the keyspaces currently present inside the
        :paramref:`~list_cluster_keyspaces.cluster`.
    """
    keyspaces = cluster.metadata.keyspaces
    return list(keyspaces.keys())


def list_table_primary_keys(cluster, keyspace, table):
    """List all primary key labels (schemas?).

    Parameters
    ----------
    cluster: cassandra.cluster.Cluster
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_
        object holding the cassandra database.
    keyspace: str
        String specifying the keyspace of the
        :paramref:`~list_table_primary_keys.cluster` where
        :paramref:`~list_table_primary_keys.table` is to be found.
    table: str
        String specifying the table inside the
        :paramref:`~list_table_primary_keys.keyspace` of
        :paramref:`~list_table_primary_keys.cluster` of which the primary key
        lable(s) is(are) to be listed.

    Returns
    -------
    list
        List of strings specifying the found primary key lables
    """
    keys = [
        key.name
        for key in cluster.metadata.keyspaces[keyspace].tables[table].primary_key
    ]
    return keys


def list_table_columns(cluster, keyspace, table):
    """List all primary key lables (schemas?).

    Parameters
    ----------
    cluster: cassandra.cluster.Cluster
        `cassandra.cluster.Cluster
        <https://docs.datastax.com/en/developer/python-driver/3.18/api/cassandra/cluster/>`_
        object holding the cassandra database.
    keyspace: str
        String specifying the keyspace of the
        :paramref:`~list_table_primary_keys.cluster` where
        :paramref:`~list_table_primary_keys.table` is to be found.
    table: str
        String specifying the table inside the
        :paramref:`~list_table_primary_keys.keyspace` of
        :paramref:`~list_table_primary_keys.cluster` of which the column labels
        are to be listed.

    Returns
    -------
    list
        List of strings specifying the found column lables
    """
    columns = cluster.metadata.keyspaces[keyspace].tables[table].columns.keys()
    return columns


def read_entry(model, primary_keys, values, keyspace=None, con=None):
    """Read existing entry using a python data class model.

    Uses the `cassandra python-driver queries
    <https://docs.datastax.com/en/developer/python-driver/3.25/cqlengine/queryset/#retrieving-objects-with-filters>`_

    Parameters
    ----------
    model
        One of the cassandra python-driver `data class models
        <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_.
    primary_keys: str, tuple
        String or tuple of strings specifying the label/schema of the primary
        key(s) to be read.
    values: str, tuple
        String or tuple of strings specifying the value of the primary key to
        be queried for.
    keyspace: str, None, default=None
        String to specify the keyspace the table is created. If ``None``,
        ``model.__keyspace__`` is used.
    con: str, None, default=None
        String to specify the connection name  the table is created with.
        :mod:`casdriv` uses :attr:`cluster name <get_cluster_name>` as
        default connection name.

        If ``None``, ``model.__connection__`` is used.

    Returns
    -------
    model class
        Instance of the :paramref:`~read_entry.model` read.

    Raises
    ------
    ValueError
        Raised when :paramref:`~read_entry.primary_keys` is neither of type
        tuple or str.
    """
    # parse keyspace and connection
    if keyspace:
        model.__keyspace__ = keyspace
    if con:
        model.__connection__ = con

    # parse key and value pairings
    if isinstance(primary_keys, str):
        filter_dict = {primary_keys: values}
    elif isinstance(primary_keys, abc.Container):
        filter_dict = dict(zip(primary_keys, values))
    else:
        msg1 = "'primary_keys' argument must be of type str or tuple"
        msg2 = "not {type(primary_keys)}"
        raise ValueError(msg1 + msg2)

    entry = model.get(**filter_dict)
    return entry


def synchronize_model(model):
    """Synchronize the cassandra table with a python data class model.

    Effectively creating a cassandra table out of a data class model,
    if not present.

    Parameters
    ----------
    model
        One of the cassandra python-driver `data class models
        <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_.
        Python data class model to synch.
    """
    cql_manage.sync_table(model)
