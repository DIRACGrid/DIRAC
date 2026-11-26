"""Unit tests for CachedJobState"""

# pylint: disable=protected-access

from unittest.mock import MagicMock

import pytest

from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.Client.JobState import CachedJobState as module
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest

INIT_STATE = {"Status": "Checking", "MinorStatus": "JobScheduling", "LastUpdateTime": "2026-11-15 00:00:00"}


@pytest.fixture
def jobState(mocker):
    jobState = MagicMock()
    jobState.getAttributes.return_value = S_OK(dict(INIT_STATE))
    jobState.commitCache.return_value = S_OK(dict(INIT_STATE))
    jobState.setManifest.return_value = S_OK()
    jobState.insertIntoTQ.return_value = S_OK()
    mocker.patch.object(module, "JobState", return_value=jobState)
    return jobState


@pytest.mark.parametrize("dirty", [True, False])
def test_insertIntoTQ_usesCachedManifest(jobState, dirty):
    """The manifest already known is used for the TQ insertion, instead of being read again"""
    manifest = JobManifest()
    assert manifest.loadJDL('[JobRequirements = [CPUTime = 1000; Sites = {"A", "B"};];]')["OK"]
    cjs = CachedJobState(1)
    assert cjs.setManifest(manifest)["OK"]
    if not dirty:
        manifest.clearDirty()
    assert cjs.insertIntoTQ()["OK"]

    assert cjs.commitChanges()["OK"]
    assert jobState.setManifest.called is dirty
    jobState.insertIntoTQ.assert_called_once_with(manifest)
    jobState.getManifest.assert_not_called()


def test_insertIntoTQ_withoutManifest(jobState):
    """Without a cached manifest, JobState reads it from the JobDB as before"""
    cjs = CachedJobState(1)
    assert cjs.insertIntoTQ()["OK"]

    assert cjs.commitChanges()["OK"]
    jobState.setManifest.assert_not_called()
    jobState.insertIntoTQ.assert_called_once_with(None)
