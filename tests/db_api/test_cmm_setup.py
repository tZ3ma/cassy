# tests/db_api/test_ccm_setup.py
"""Test succesfull local cassandra db setup, used for further testing."""
import pytest


@pytest.mark.parametrize(
    ("nname", "status_query", "result"),
    [
        ("node1", "is_running", True),
        ("node1", "is_live", True),
    ],
)
def test_node_status(nname, status_query, result, ccmlib_cluster):
    """Test for expected node initializaton."""
    node = ccmlib_cluster.nodes[nname]
    status = getattr(node, status_query)()
    assert status == result


def test_cluster_version(ccmlib_cluster):
    """Test succesful cassandra cluster initializtation."""
    vers = ccmlib_cluster.version()
    assert vers == "4.0.4"


def test_node_name(ccmlib_cluster):
    """Test succesful cassandra cluster node initializtation."""
    nodes = ccmlib_cluster.nodelist()
    assert nodes[0].name == "node1"


@pytest.mark.parametrize(
    ("nname", "iface", "result"),
    [
        ("node1", "thrift", None),
        ("node1", "storage", ("127.0.1.1", 7000)),
        ("node1", "binary", ("127.0.1.1", 9042)),
    ],
)
def test_node_network_interfaces(nname, iface, result, ccmlib_cluster):
    """Test for expected node initializaton."""
    node = ccmlib_cluster.nodes[nname]
    assert node.network_interfaces[iface] == result
