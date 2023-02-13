import os
import pytest
import tempfile
import errno
import DIRAC

from DIRAC.tests.Utilities.utils import generateDIRACConfig

from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from diraccfg import CFG
from DIRAC.DataManagementSystem.private.FTS3Plugins.DefaultFTS3Plugin import DefaultFTS3Plugin
from DIRAC import S_OK
from DIRAC.Core.Utilities.DErrno import cmpError

from DIRAC.Resources.Storage.StorageBase import StorageBase

from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3Operation


DIRAC.gLogger.setLevel("DEBUG")
# pylint: disable=redefined-outer-name


class fakePlugin(StorageBase):
    """Fake HTTPS plugin."""

    def getTransportURL(self, path, protocols=False):
        return S_OK({"Successful": {p: p for p in path}, "Failed": {}})


def mock_StorageFactory_generateStorageObject(self, storageName, pluginName, parameters, hideExceptions=False):
    """Generate fake storage object"""
    storageObj = fakePlugin(storageName, parameters)

    storageObj.pluginName = pluginName

    return S_OK(storageObj)


CFG_CONTENT = """
    DIRAC
    {
        VirtualOrganization = lhcb
    }
    Resources
    {
      # We define a few SEBases with various protocols
      StorageElementBases
      {
        CERN-Disk
        {
          BackendType =  EOS
          GFAL2_HTTPS
          {
            Host = cerneos.cern.ch
            Protocol = https
            Path = /eos
            Access = remote
          }
          GFAL2_XROOT
          {
            Host = cerneos.cern.ch
            Protocol = root
            Path = /eos
            Access = remote
          }
        }
        CERN-Tape
        {
          BackendType =  CTA
          SEType = T1D0
          # This StageProtocol will triger some multihop staging cases
          StageProtocols = root
          CTA
          {
            Host = cerncta.cern.ch
            Protocol = root
            Path = /eos/ctalhcbpps/archivetest/
            Access = remote
          }
          GFAL2_HTTPS
          {
            Host = cerncta.cern.ch
            Protocol = https
            Path = /eos/ctalhcbpps/archivetest/
            Access = remote
          }
        }
        RAL-Disk
        {
          BackendType =  Echo
          GFAL2_XROOT
          {
            Host = ralecho.gridpp.uk
            Protocol = root
            Path = /echo
            Access = remote
          }
        }
        RAL-Tape
        {
          BackendType =  Castor
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
          }
        }
        CNAF-Disk
        {
          BackendType =  Storm
          GFAL2_HTTPS
          {
            Host = cnafstorm.infc.it
            Protocol = https
            Path = /storm
            Access = remote
          }
        }
      }
      StorageElements
      {
        CERN-DST
        {
          BaseSE = CERN-Disk
        }
        CERN-RAW
        {
          BaseSE = CERN-Tape
        }
        CNAF-DST
        {
          BaseSE = CNAF-Disk
        }
        RAL-DST
        {
          BaseSE = RAL-Disk
        }
      }

    }
    Operations{
      Defaults
      {
        DataManagement
        {
          AccessProtocols=https,root
          WriteProtocols=https,root
          ThirdPartyProtocols = https,root
        }
      }
    }
"""


@pytest.fixture(scope="module", autouse=True)
def loadCS():
    """Load the CFG_CONTENT as a DIRAC Configuration for this module"""
    with generateDIRACConfig(CFG_CONTENT, "test_FTS3Objects.cfg"):
        yield


@pytest.fixture(scope="function", autouse=True)
def monkeypatchForAllTest(monkeypatch):
    """ " This fixture will run for all test method and will mockup
    a few DMS methods
    """
    monkeypatch.setattr(
        DIRAC.Resources.Storage.StorageFactory.StorageFactory,
        "_StorageFactory__generateStorageObject",
        mock_StorageFactory_generateStorageObject,
    )
    monkeypatch.setattr(
        DIRAC.DataManagementSystem.Client.FTS3Job.FTS3Job,
        "_FTS3Job__fetchSpaceToken",
        lambda _self, _seName, _vo: S_OK(),
    )


def generateFTS3Job(sourceSE, targetSE, lfns, multiHopSE=None):
    """Utility to create a new FTS3Job object with some FTS3Files

    The FileIDs are filled in order, starting with 1

    :param src sourceSE: source SE Name
    :param src targetSE: target SE Name
    :param list lfns: list of lfns (str)
    :param src multiHopSE: hop SE Name

    """

    newJob = FTS3Job()
    newJob.type = "Transfer"
    newJob.sourceSE = sourceSE
    newJob.targetSE = targetSE
    newJob.multiHopSE = multiHopSE
    filesToSubmit = []

    for i, lfn in enumerate(lfns, start=1):
        ftsFile = FTS3File()
        ftsFile.fileID = i
        ftsFile.checksum = lfn
        ftsFile.lfn = lfn
        filesToSubmit.append(ftsFile)

    newJob.filesToSubmit = filesToSubmit
    newJob.operationID = 123
    newJob.rmsReqID = 456

    return newJob


def test_submit_directJob():
    """A simple transfer of two files between two SE"""

    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1", "/lhcb/f2"])

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")

    assert res["OK"], res

    job, fileIDsInTheJob = res["Value"]

    # Make sure all the files were submitted
    assert len(job["files"]) == len(newJob.filesToSubmit)
    # It is not a multihop
    assert not job["params"]["multihop"]
    # All fileIDs are returned
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit}

    # Check the returned transfers
    f1Trans = job["files"][0]
    f2Trans = job["files"][1]

    assert f1Trans["sources"][0].startswith("https://cerneos")
    assert f1Trans["destinations"][0].startswith("https://cnafstorm")

    assert f2Trans["sources"][0].startswith("https://cerneos")
    assert f2Trans["destinations"][0].startswith("https://cnafstorm")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in job["files"] if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 2


def test_submit_directJob_secondFailed():
    """Simple transfer of two files, with second LFN problematic"""

    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1", "/badLFN/f2"])

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")

    assert res["OK"], res

    job, fileIDsInTheJob = res["Value"]

    # We expect only one file
    assert len(job["files"]) == 1

    # Check that the submitted fileID is the one with the correct LFN
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit if f.lfn == "/lhcb/f1"}

    # Not a multihop
    assert not job["params"]["multihop"]

    f1Trans = job["files"][0]

    assert f1Trans["sources"][0].startswith("https://cerneos")
    assert f1Trans["destinations"][0].startswith("https://cnafstorm")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in job["files"] if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 1


def test_submit_directJob_firstFailed():
    """Simple transfer of two files, with first LFN problematic"""

    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/badLFN/f1", "/lhcb/f2"])

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")

    assert res["OK"], res

    job, fileIDsInTheJob = res["Value"]

    # We expect only one file
    assert len(job["files"]) == 1

    # Check that the submitted fileID is the one with the correct LFN
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit if f.lfn == "/lhcb/f2"}

    # Not a multihop
    assert not job["params"]["multihop"]

    f1Trans = job["files"][0]

    assert f1Trans["sources"][0].startswith("https://cerneos")
    assert f1Trans["destinations"][0].startswith("https://cnafstorm")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in job["files"] if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 1


def test_submit_directJob_allFailed():
    """Simple transfer of two files, with all LFNs problematic"""

    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/badLFN/f1", "/badLFN/f2"])
    # We should get a complete failure
    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert not res["OK"]
    assert cmpError(res, errno.ENODATA)


def test_submit_direct_noPotocol():
    """Direct transfer with no common protocol. It is a failure"""

    newJob = generateFTS3Job("CNAF-DST", "RAL-DST", ["/lhcb/f1", "/lhcb/f2"])

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")

    assert not res["OK"]
    # Check that the error is no common protocol
    assert cmpError(res, errno.ENOPROTOOPT)


def test_submit_multiHopStaging():
    """We do a transfer between a tape system and a disk. The protocol used for
    staging is not the same as the one for transfer, so it needs a multihop stage"""

    newJob = generateFTS3Job("CERN-RAW", "CNAF-DST", ["/lhcb/f1"])

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert res["OK"]

    job, fileIDsInTheJob = res["Value"]

    # It is a multihop
    assert job["params"]["multihop"]

    # All fileIDs are returned
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit}

    # It involves two distinct transfers
    assert len(job["files"]) == 2

    stagingTrans = job["files"][0]
    copyTrans = job["files"][1]

    assert stagingTrans["sources"][0].startswith("root://cerncta")
    assert stagingTrans["destinations"][0].startswith("root://cerncta")

    assert copyTrans["sources"][0].startswith("https://cerncta")
    assert copyTrans["destinations"][0].startswith("https://cnafstorm")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in job["files"] if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 1


def test_submit_multiHopStaging_failureBadLFN():
    """We do a multi hop stage that fails because of a bad LFN"""

    newJob = generateFTS3Job("CERN-RAW", "CNAF-DST", ["/badLFN/f1"])
    # We should get a complete failure
    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert not res["OK"]
    assert cmpError(res, errno.ENODATA)


def test_submit_multiHopStaging_multipleFiles():
    """A multihop transfer cannot have more than one file at the time"""

    newJob = generateFTS3Job("CERN-RAW", "CNAF-DST", ["/lhcb/f1", "/lhcb/f2"])

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert not res["OK"]
    assert cmpError(res, errno.E2BIG)


def test_submit_multiHopTransfer():
    """Standard multihop transfer, with protocol translation"""

    newJob = generateFTS3Job("CNAF-DST", "RAL-DST", ["/lhcb/f1"], multiHopSE="CERN-DST")

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert res["OK"]

    job, fileIDsInTheJob = res["Value"]

    assert job["params"]["multihop"]

    # All fileIDs are returned
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit}

    assert len(job["files"]) == 2

    firstHop = job["files"][0]
    secondHop = job["files"][1]

    assert firstHop["sources"][0].startswith("https://cnafstorm")
    assert firstHop["destinations"][0].startswith("https://cerneos")

    assert secondHop["sources"][0].startswith("root://cerneos")
    assert secondHop["destinations"][0].startswith("root://ralecho")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in job["files"] if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 1


def test_submit_multiHopTransfer_failure_multipleFiles():
    """multihop with more than one file (not allowed)"""

    newJob = generateFTS3Job("CNAF-DST", "RAL-DST", ["/lhcb/f1", "/lhcb/f2"], multiHopSE="CERN-DST")

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert not res["OK"]
    assert cmpError(res, errno.E2BIG)


def test_submit_multiHopTransfer_failure_badLFN():
    """Do a a multiHop transfer, but the LFN is bad (first loop failure)"""

    newJob = generateFTS3Job("CNAF-DST", "RAL-DST", ["/badLFN/f1"], multiHopSE="CERN-DST")

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert not res["OK"]
    assert cmpError(res, errno.ENODATA)


def test_submit_multiHopTransfer_failure_protocolFirstHop():
    """Multi hop with first hop impossible (no protocol compatible between RAL & CNAF"""

    newJob = generateFTS3Job("RAL-DST", "CERN-DST", ["/lhcb/f1"], multiHopSE="CNAF-DST")

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert not res["OK"]
    # Check that the error is no common protocol
    assert cmpError(res, errno.ENOPROTOOPT)


def test_submit_multiHopTransfer_failure_protocolSecondHop():
    """Multi hop with second hop impossible (no protocol compatible between CNAF and RAL"""

    newJob = generateFTS3Job("CERN-DST", "RAL-DST", ["/lhcb/f1"], multiHopSE="CNAF-DST")

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert not res["OK"]
    # Check that the error is no common protocol
    assert cmpError(res, errno.ENOPROTOOPT)


def test_submit_doubleMultiHopStaging():
    """Idiot. We do a multihop transfer (CTA->CNAF->EOS), and on top of that, the first hop (CTA->CNAF)
    needs a staging multihop. If you do that, you are insane"""

    newJob = generateFTS3Job("CERN-RAW", "CERN-DST", ["/lhcb/f1"], multiHopSE="CNAF-DST")

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")
    assert res["OK"]

    job, fileIDsInTheJob = res["Value"]

    # It is a multihop
    assert job["params"]["multihop"]

    # All fileIDs are returned
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit}

    # It involves three (!!!!!) distinct transfers
    assert len(job["files"]) == 3

    stagingTrans = job["files"][0]
    hopTrans = job["files"][1]
    copyTrans = job["files"][2]

    assert stagingTrans["sources"][0].startswith("root://cerncta")
    assert stagingTrans["destinations"][0].startswith("root://cerncta")

    assert hopTrans["sources"][0].startswith("https://cerncta")
    assert hopTrans["destinations"][0].startswith("https://cnafstorm")

    assert copyTrans["sources"][0].startswith("https://cnafstorm")
    assert copyTrans["destinations"][0].startswith("https://cerneos")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in job["files"] if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 1
