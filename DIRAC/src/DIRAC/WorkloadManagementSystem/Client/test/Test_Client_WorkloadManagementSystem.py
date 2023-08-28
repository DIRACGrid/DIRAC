""" Test for WMS clients
"""
# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import os
from io import BytesIO

import pytest
from unittest.mock import MagicMock

from DIRAC import gLogger

gLogger.setLevel("DEBUG")

# sut
from DIRAC.WorkloadManagementSystem.Client.Matcher import Matcher
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient


pilotAgentsDBMock = MagicMock()
jobDBMock = MagicMock()
tqDBMock = MagicMock()
jlDBMock = MagicMock()
opsHelperMock = MagicMock()
matcher = Matcher(
    pilotAgentsDB=pilotAgentsDBMock,
    jobDB=jobDBMock,
    tqDB=tqDBMock,
    jlDB=jlDBMock,
    opsHelper=opsHelperMock,
)


@pytest.fixture
def setUp():
    yield setUp

    try:
        os.remove("1.txt")
        os.remove("InputData_*")
    except OSError:
        pass


def test__processResourceDescription(setUp):
    resourceDescription = {
        "Architecture": "x86_64-slc6",
        "CEQueue": "jenkins-queue_not_important",
        "CPUNormalizationFactor": "9.5",
        "CPUTime": 1080000,
        "CPUTimeLeft": 5000,
        "DIRACVersion": "v8r0p1",
        "FileCatalog": "LcgFileCatalogCombined",
        "GridCE": "jenkins.cern.ch",
        "LocalSE": ["CERN-SWTEST"],
        "MaxTotalJobs": 100,
        "MaxWaitingJobs": 10,
        "OutputURL": "gsiftp://localhost",
        "PilotBenchmark": 9.5,
        "PilotReference": "somePilotReference",
        "Platform": "x86_64-slc6",
        "ReleaseVersion": "v8r0p1",
        "Site": "DIRAC.Jenkins.ch",
        "WaitingToRunningRatio": 0.05,
    }

    res = matcher._processResourceDescription(resourceDescription)
    resExpected = {
        "ReleaseVersion": "v8r0p1",
        "CPUTime": 1080000,
        "DIRACVersion": "v8r0p1",
        "PilotReference": "somePilotReference",
        "PilotBenchmark": 9.5,
        "Platform": "x86_64-slc6",
        "Site": "DIRAC.Jenkins.ch",
        "GridCE": "jenkins.cern.ch",
    }

    assert res == resExpected


def test_uploadFilesAsSandbox(mocker, setUp):
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient.TransferClient", return_value=MagicMock())
    ssc = SandboxStoreClient()
    fileList = [BytesIO(b"try")]
    res = ssc.uploadFilesAsSandbox(fileList)
    print(res)
