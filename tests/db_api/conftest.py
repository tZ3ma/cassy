# tests/db_api/conftest.py
# pylint: disable=protected-access
# pylint: disable=import-error
# pylint: disable=redefined-outer-name
"""Configure and setup cassy api testings."""
from collections import namedtuple

import ccmlib.cluster
import pytest

import cassy.casdriv as cassy_driv


# pylint: enable=import-error
@pytest.fixture(scope="session")
def ccmlib_cluster(tmp_path_factory):
    """Create cassandra cluster on temp directory."""
    cluster_path = tmp_path_factory.mktemp("clusterp")
    cluster = ccmlib.cluster.Cluster(
        cluster_path, "pytest_tmp_cluster", cassandra_version="4.0.4"
    )

    cluster.populate(1, ipprefix="127.0.1.").start(timeout=180)

    cluster._bnry_contacts = [
        node.network_interfaces["binary"][0] for node in cluster.nodelist()
    ]
    cluster._bnry_port = cluster.nodelist()[0].network_interfaces["binary"][1]

    yield cluster

    print("teardown cluster")
    cluster.remove()


@pytest.fixture(scope="session")
def casdriv_cns(ccmlib_cluster):
    """Test getting cluster object with a connected session using casriv."""
    cluster, session = cassy_driv.connect_session(
        ips=ccmlib_cluster._bnry_contacts,
        port=ccmlib_cluster._bnry_port,
    )

    yield namedtuple("CnS", ["cluster", "session"])(cluster, session)

    print("disconnect session")
    # session.shutdown()
