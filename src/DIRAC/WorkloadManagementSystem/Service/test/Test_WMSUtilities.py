""" Test class for WMSUtilities
"""
from unittest.mock import MagicMock

from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import killPilotsInQueues
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import QueueCECache

# The CE factory is reached through QueueCECache; getQueue and setPilotCredentials
# are module-level names looked up inside killPilotsInQueues, so we patch them there.
GET_CE = "DIRAC.Resources.Computing.ComputingElementFactory.ComputingElementFactory.getCE"
GET_QUEUE = "DIRAC.WorkloadManagementSystem.Service.WMSUtilities.getQueue"
SET_CREDS = "DIRAC.WorkloadManagementSystem.Service.WMSUtilities.setPilotCredentials"


def _twoQueues():
    return {
        "vo@@@site1@@@ce1@@@queue1": {"GridType": "ssh", "PilotList": ["p1", "p2"]},
        "vo@@@site2@@@ce2@@@queue2": {"GridType": "ssh", "PilotList": ["p3"]},
    }


def test_killPilotsInQueues_allSucceed(mocker):
    """One killJob call per queue when everything succeeds."""
    mocker.patch(GET_QUEUE, return_value=S_OK({"CEType": "ssh"}))
    mocker.patch(SET_CREDS, return_value=S_OK())
    ce = MagicMock()
    ce.killJob = MagicMock(return_value=S_OK())
    mocker.patch(GET_CE, return_value=S_OK(ce))

    result = killPilotsInQueues(_twoQueues())

    assert result["OK"]
    assert ce.killJob.call_count == 2  # one call per queue


def test_killPilotsInQueues_attemptsAllAndAggregates(mocker):
    """A queue whose killJob fails must not stop the others from being attempted."""
    mocker.patch(GET_QUEUE, return_value=S_OK({"CEType": "ssh"}))
    mocker.patch(SET_CREDS, return_value=S_OK())
    ce = MagicMock()
    # First queue fails to kill, second succeeds (dict preserves insertion order)
    ce.killJob = MagicMock(side_effect=[S_ERROR("boom"), S_OK()])
    mocker.patch(GET_CE, return_value=S_OK(ce))

    result = killPilotsInQueues(_twoQueues())

    assert not result["OK"]  # the failure is reported...
    assert ce.killJob.call_count == 2  # ...but every queue was still attempted


def test_killPilotsInQueues_reusesProvidedCache(mocker):
    """A provided QueueCECache reuses the CE across calls instead of rebuilding it."""
    mocker.patch(GET_QUEUE, return_value=S_OK({"CEType": "ssh"}))
    mocker.patch(SET_CREDS, return_value=S_OK())
    ce = MagicMock()
    ce.killJob = MagicMock(return_value=S_OK())
    getCEMock = mocker.patch(GET_CE, return_value=S_OK(ce))

    cache = QueueCECache()
    refDict = {"vo@@@site1@@@ce1@@@queue1": {"GridType": "ssh", "PilotList": ["p1"]}}

    assert killPilotsInQueues(refDict, ceCache=cache)["OK"]
    assert killPilotsInQueues(refDict, ceCache=cache)["OK"]

    # Same queue, unchanged parameters -> the CE is built once and reused on the 2nd call
    getCEMock.assert_called_once()
