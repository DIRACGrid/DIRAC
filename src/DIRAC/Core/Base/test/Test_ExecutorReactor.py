"""Unit tests for the reconnection of ExecutorReactor to its Mind"""

# pylint: disable=protected-access

from unittest.mock import MagicMock

import pytest

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base import ExecutorReactor as module
from DIRAC.Core.Base.ExecutorReactor import ExecutorReactor


@pytest.fixture
def cluster(mocker):
    sleep = mocker.patch.object(module.time, "sleep")
    cluster = ExecutorReactor.MindCluster("WorkloadManagement/OptimizationMind", MagicMock())
    msgClient = MagicMock()
    cluster._MindCluster__msgClient = msgClient
    return cluster, msgClient, sleep


def test_reconnects(cluster):
    cluster, msgClient, sleep = cluster
    msgClient.connected = False
    msgClient.connect.side_effect = [S_ERROR("Connection refused"), S_OK()]
    assert cluster._MindCluster__disconnected(msgClient)["OK"]
    assert msgClient.connect.call_count == 2
    sleep.assert_called_once()


def test_alreadyReconnectedElsewhere(cluster):
    """The connection was re-established by MessageClient.sendMessage: stop retrying"""
    cluster, msgClient, sleep = cluster
    msgClient.connected = False

    def connectedMeanwhile(**kwargs):
        # Fail the test rather than loop for ever
        assert msgClient.connect.call_count < 5, "still retrying although connected"
        msgClient.connected = "trid"
        return S_ERROR("Already connected")

    msgClient.connect.side_effect = connectedMeanwhile
    assert cluster._MindCluster__disconnected(msgClient)["OK"]
    msgClient.connect.assert_called_once()
    sleep.assert_called_once()
