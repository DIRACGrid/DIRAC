import errno
from unittest import mock

import pytest

import DIRAC
from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Utilities.DErrno import cmpError
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.private import FTSRestLibs
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.tests.Utilities.utils import generateDIRACConfig

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

    def mock_init(self, useProxy=False, vo=None):
        self.proxy = False
        self.proxy = useProxy
        self.resourceStatus = mock.MagicMock()
        self.vo = vo
        self.remoteProtocolSections = []
        self.localProtocolSections = []
        self.name = ""
        self.options = {}
        self.protocols = {}
        self.storages = {}

    monkeypatch.setattr(DIRAC.Resources.Storage.StorageFactory.StorageFactory, "__init__", mock_init)


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
    newJob.vo = "lhcb"
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

    fileIDsInTheJob = res["Value"]

    # Make sure all the files were submitted
    assert len(newJob.transfers) == len(newJob.filesToSubmit)
    # It is not a multihop
    assert not newJob.multihop
    # All fileIDs are returned
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit}

    # Check the returned transfers
    f1Trans = newJob.transfers[0]
    f2Trans = newJob.transfers[1]

    assert f1Trans["source"].startswith("https://cerneos")
    assert f1Trans["destination"].startswith("https://cnafstorm")

    assert f2Trans["source"].startswith("https://cerneos")
    assert f2Trans["destination"].startswith("https://cnafstorm")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in newJob.transfers if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 2


def test_submit_directJob_secondFailed():
    """Simple transfer of two files, with second LFN problematic"""

    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1", "/badLFN/f2"])

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")

    assert res["OK"], res

    fileIDsInTheJob = res["Value"]

    # We expect only one file
    assert len(newJob.transfers) == 1

    # Check that the submitted fileID is the one with the correct LFN
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit if f.lfn == "/lhcb/f1"}

    # Not a multihop
    assert not newJob.multihop

    f1Trans = newJob.transfers[0]

    assert f1Trans["source"].startswith("https://cerneos")
    assert f1Trans["destination"].startswith("https://cnafstorm")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in newJob.transfers if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 1


def test_submit_directJob_firstFailed():
    """Simple transfer of two files, with first LFN problematic"""

    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/badLFN/f1", "/lhcb/f2"])

    res = newJob._constructTransferJob(3600, [f.lfn for f in newJob.filesToSubmit], "")

    assert res["OK"], res

    fileIDsInTheJob = res["Value"]

    # We expect only one file
    assert len(newJob.transfers) == 1

    # Check that the submitted fileID is the one with the correct LFN
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit if f.lfn == "/lhcb/f2"}

    # Not a multihop
    assert not newJob.multihop

    f1Trans = newJob.transfers[0]

    assert f1Trans["source"].startswith("https://cerneos")
    assert f1Trans["destination"].startswith("https://cnafstorm")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in newJob.transfers if "fileID" in f["metadata"]]
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

    fileIDsInTheJob = res["Value"]

    # It is a multihop
    assert newJob.multihop

    # All fileIDs are returned
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit}

    # It involves two distinct transfers
    assert len(newJob.transfers) == 2

    stagingTrans = newJob.transfers[0]
    copyTrans = newJob.transfers[1]

    assert stagingTrans["source"].startswith("root://cerncta")
    assert stagingTrans["destination"].startswith("root://cerncta")

    assert copyTrans["source"].startswith("https://cerncta")
    assert copyTrans["destination"].startswith("https://cnafstorm")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in newJob.transfers if "fileID" in f["metadata"]]
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

    fileIDsInTheJob = res["Value"]

    assert newJob.multihop

    # All fileIDs are returned
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit}

    assert len(newJob.transfers) == 2

    firstHop = newJob.transfers[0]
    secondHop = newJob.transfers[1]

    assert firstHop["source"].startswith("https://cnafstorm")
    assert firstHop["destination"].startswith("https://cerneos")

    assert secondHop["source"].startswith("root://cerneos")
    assert secondHop["destination"].startswith("root://ralecho")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in newJob.transfers if "fileID" in f["metadata"]]
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

    fileIDsInTheJob = res["Value"]

    # It is a multihop
    assert newJob.multihop

    # All fileIDs are returned
    assert fileIDsInTheJob == {f.fileID for f in newJob.filesToSubmit}

    # It involves three (!!!!!) distinct transfers
    assert len(newJob.transfers) == 3

    stagingTrans = newJob.transfers[0]
    hopTrans = newJob.transfers[1]
    copyTrans = newJob.transfers[2]

    assert stagingTrans["source"].startswith("root://cerncta")
    assert stagingTrans["destination"].startswith("root://cerncta")

    assert hopTrans["source"].startswith("https://cerncta")
    assert hopTrans["destination"].startswith("https://cnafstorm")

    assert copyTrans["source"].startswith("https://cnafstorm")
    assert copyTrans["destination"].startswith("https://cerneos")

    # Only "real" (i.e. non intermediate hop) have 'FileID' in their metadata and monitored
    monitoredJob = [f["metadata"] for f in newJob.transfers if "fileID" in f["metadata"]]
    assert len(monitoredJob) == 1


def test_getIdpClient_isCachedPerGroup():
    """The IdP client of a group is created once, from the class or from any job"""
    FTS3Job._idp_cache.clear()
    with mock.patch(
        "DIRAC.DataManagementSystem.Client.FTS3Job.getIdProviderClient",
        side_effect=lambda group, _, **kwargs: S_OK(f"client-{group}"),
    ) as getIdProviderClient:
        try:
            assert FTS3Job._getIdpClient("group1") == "client-group1"
            assert FTS3Job()._getIdpClient("group1") == "client-group1"
            assert FTS3Job()._getIdpClient("group2") == "client-group2"
        finally:
            FTS3Job._idp_cache.clear()
    assert getIdProviderClient.call_count == 2


class FakeFTSRestLibs:
    """Replaces the FTSRestLibs module in FTS3Job, to test FTS3Job independently of any library"""

    def __init__(self, jobStatus=None, error=None):
        self.jobStatus = jobStatus
        self.error = error
        self.submitted = []

    @staticmethod
    def usesTokens(context):
        return context == "tokenContext"

    def submit(self, context, ftsJob):
        if self.error:
            return self.error
        self.submitted.append(ftsJob)
        return S_OK("myGUID")

    def getJobStatus(self, context, ftsGUID):
        return self.error or S_OK(self.jobStatus)

    def cancel(self, context, ftsGUID):
        return self.error or S_OK({"job_state": "CANCELED"})


@pytest.fixture
def fakeFTSRestLibs(monkeypatch):
    """Install a FakeFTSRestLibs in FTS3Job, and return a function to configure it"""

    def install(**kwargs):
        client = FakeFTSRestLibs(**kwargs)
        monkeypatch.setattr(DIRAC.DataManagementSystem.Client.FTS3Job, "FTSRestLibs", client)
        return client

    return install


def test_submit_withClient(fakeFTSRestLibs):
    """prepareSubmission + submit go through FTSRestLibs and update the files"""
    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1", "/lhcb/f2"])
    client = fakeFTSRestLibs()

    # Not prepared
    assert not newJob.submit("x509Context")["OK"]

    res = newJob.prepareSubmission(pinTime=3600)
    assert res["OK"], res
    assert not newJob.useTokens

    res = newJob.submit("x509Context")
    assert res["OK"], res
    assert res["Value"] == {1, 2}
    assert client.submitted == [newJob]
    assert newJob.ftsGUID == "myGUID"
    for ftsFile in newJob.filesToSubmit:
        assert ftsFile.ftsGUID == "myGUID"
        assert ftsFile.status == "Submitted"
        assert ftsFile.attempt == 1


def test_submit_error(fakeFTSRestLibs):
    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1"])
    fakeFTSRestLibs(error=S_ERROR("Error at submission"))
    assert newJob.prepareSubmission(pinTime=3600)["OK"]
    res = newJob.submit("x509Context")
    assert not res["OK"]
    assert newJob.ftsGUID is None


def test_submit_tokenJobNeedsTokenContext(fakeFTSRestLibs):
    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1"])
    assert newJob.prepareSubmission(pinTime=3600)["OK"]
    newJob.useTokens = True
    fakeFTSRestLibs()
    assert not newJob.submit("x509Context")["OK"]
    assert newJob.submit("tokenContext")["OK"]


def test_monitor_notFound(fakeFTSRestLibs):
    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1"])
    newJob.ftsGUID = "myGUID"
    fakeFTSRestLibs(error=S_ERROR(errno.ESRCH, "not there"))
    res = newJob.monitor("x509Context")
    assert cmpError(res, errno.ESRCH)
    assert newJob.status == "Failed"


def test_monitor_withClient(fakeFTSRestLibs):
    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1", "/lhcb/f2"])
    newJob.ftsGUID = "myGUID"
    newJob.status = "Active"
    jobStatus = {
        "job_state": "FINISHEDDIRTY",
        "reason": "One failed",
        "job_metadata": {"sourceSE": "CERN-DST", "targetSE": "CNAF-DST"},
        "files": [
            {
                "file_state": "FINISHED",
                "reason": "",
                "file_metadata": {"fileID": 1},
                "filesize": 10,
                "tx_duration": 2,
            },
            {
                "file_state": "FAILED",
                "reason": "No space",
                "file_metadata": {"fileID": 2},
                "recoverable": False,
                "filesize": 10,
                "tx_duration": 0,
            },
            # Intermediate staging transfer, ignored
            {
                "file_state": "FINISHED",
                "reason": "",
                "file_metadata": {"desc": "PreStage 1"},
                "filesize": None,
                "tx_duration": None,
            },
        ],
    }
    fakeFTSRestLibs(jobStatus=jobStatus)
    res = newJob.monitor("x509Context")
    assert res["OK"], res
    assert res["Value"] == {
        1: {"status": "Finished", "error": "", "ftsGUID": None},
        2: {"status": "Defunct", "error": "No space", "ftsGUID": None},
    }
    assert newJob.status == "Finisheddirty"
    assert newJob.completeness == 100
    assert {d["FinalStatus"]: d["TransferTotal"] for d in newJob.accountingDicts} == {"Finished": 1, "Failed": 1}


def test_cancel_withClient(fakeFTSRestLibs):
    newJob = generateFTS3Job("CERN-DST", "CNAF-DST", ["/lhcb/f1"])
    newJob.ftsGUID = "myGUID"
    fakeFTSRestLibs()
    assert newJob.cancel("x509Context")["OK"]
    assert newJob.status == "Canceled"

    fakeFTSRestLibs(error=S_ERROR(errno.ESRCH, "not there"))
    assert cmpError(newJob.cancel("x509Context"), errno.ESRCH)


@pytest.mark.parametrize("flavour", ["FTS3", "FTS4"])
def test_buildJob(flavour):
    """The prepared job is converted into a job of the library of each flavour"""
    if FTSRestLibs.EASY_FTS[flavour] is None:
        pytest.skip(f"No client library for {flavour}")

    newJob = generateFTS3Job("CERN-RAW", "CNAF-DST", ["/lhcb/f1"])
    newJob.priority = 3
    assert newJob.prepareSubmission(pinTime=3600)["OK"]

    job = FTSRestLibs.buildJob(flavour, newJob)

    assert len(job["files"]) == len(newJob.transfers)
    for ftsTrans, trans in zip(job["files"], newJob.transfers):
        assert ftsTrans["sources"] == [trans["source"]]
        assert ftsTrans["destinations"] == [trans["destination"]]
        assert ftsTrans["metadata"] == trans["metadata"]
        assert "source_tokens" not in ftsTrans
    params = job["params"]
    assert params["multihop"] is True
    assert params["bring_online"] == newJob.bringOnline
    assert params["priority"] == 3
    assert params["retry"] == 3
    assert params["job_metadata"] == newJob.jobMetadata
    assert params["unmanaged_tokens"] is True
    # SRM parameters only exist in FTS3
    assert ("copy_pin_lifetime" in params) == (flavour == "FTS3")


def test_newJob_unknownParameter():
    """Parameters a library does not know are refused instead of being silently ignored"""
    if FTSRestLibs.easy_fts4 is None:
        pytest.skip("No client library for FTS4")
    with pytest.raises(TypeError, match="copy_pin_lifetime"):
        FTSRestLibs._newJob(FTSRestLibs.easy_fts4, transfers=[], copy_pin_lifetime=3600)
