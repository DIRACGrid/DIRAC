import errno
import json
import pytest
import os
import re
import time
import DIRAC
from DIRAC import S_OK, S_ERROR
from zlib import adler32
from DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister import filterReplicas

from DIRAC.RequestManagementSystem.Client.File import File


# LFN for a non existing file
NON_EXISTING_LFN = "/lhcb/i/dont/exist.txt"

# Complete failure of getting replicas
ERROR_GETTING_REPLICAS = "/lhcb/S_ERROR/replicas"

# The file simply has no replicas available
NO_AVAILABLE_REPLICAS = "/lhcb/no/available/replicas"


# Complete failure of getting FC metadata
ERROR_GETTING_FC_METADATA = "/lhcb/S_ERROR/fc_metadata"

# Complete failure of getting SE metadata
ERROR_GETTING_SE_METADATA = "/lhcb/S_ERROR/se_metadata"


# SE at which we will find the files by default
DEFAULT_SE = "DiskSE"


# The file replicas at given SEs
# Should be /MARKER_WITH_REPLICAS/Disk1/Disk2/Tape1/END_MARKER
# The "Disk" and "Tape" part are interpreted as such
MARKER_WITH_REPLICAS = "WITH_REPLICAS"

# To declare that the specific replica has wrong cks
MARKER_BAD_SE_REPLICAS = "BAD_SE_REPLICAS"

# The cks is wrong in the FC
MARKER_BAD_FC_REPLICAS = "BAD_FC_REPLICAS"

# The file physicaly does not exist but is registered
MARKER_BAD_SE_NOT_EXIST = "BAD_SE_NOT_EXIST"

END_MARKER = "END"


MARKER_PATTERN = rf"/*(?P<pat>({MARKER_BAD_SE_REPLICAS}|{MARKER_BAD_FC_REPLICAS}|{MARKER_WITH_REPLICAS}|{MARKER_BAD_SE_NOT_EXIST})+)/(?P<values>.*)/"


# lfn = f"{PAT1}/1/2/{END_MARKER}/{PAT2}/3/{END_MARKER}"


def _splitLFN(lfn):
    """Split the LFN based on the various pattern"""
    instructions = {}
    try:
        for group in re.split(rf"{END_MARKER}", lfn):
            if not group:
                continue

            matchgroup = re.match(MARKER_PATTERN, group).groupdict()
            instructions[matchgroup["pat"]] = matchgroup["values"]
    except Exception:
        pass
    return instructions


def mock_DM_getReplicas(self, lfns, **kwargs):
    """Mock the DataManager.getReplicas method.

    It returns based on the LFN.
    See the code for details

    """
    # time.sleep(1)
    if isinstance(lfns, str):
        lfns = [lfns]

    successful = {}
    failed = {}

    for lfn in lfns:

        instructions = _splitLFN(lfn)
        # If the lfn does not exist, put it in Failed dict
        if lfn == NON_EXISTING_LFN:
            failed[lfn] = os.strerror(errno.ENOENT)
        # Return a complete failure if it is expected (ERROR_GETTING_REPLICAS)
        elif lfn == ERROR_GETTING_REPLICAS:
            return S_ERROR(f"Complete failure {lfn}")
        elif lfn == NO_AVAILABLE_REPLICAS:
            continue
        # If the LFN specifies the replicas
        elif MARKER_WITH_REPLICAS in instructions:
            ses = instructions[MARKER_WITH_REPLICAS].split("/")
            # If we have the preferDisk flag, and we have disk replicas,
            #  only return disk storage, otherwise return them all
            if kwargs.get("preferDisk"):
                if any(["Disk" in se for se in ses]):
                    ses = [se for se in ses if "Disk" in se]

            successful[lfn] = dict.fromkeys(ses)

        else:
            successful[lfn] = {DEFAULT_SE: None}
    return S_OK({"Successful": successful, "Failed": failed})


def mock_FC_getFileMetadata(self, lfns, **kwargs):
    if isinstance(lfns, str):
        lfns = [lfns]

    successful = {}
    failed = {}
    for lfn in lfns:
        if lfn == ERROR_GETTING_FC_METADATA:
            S_ERROR(f"Complete failure {lfn}")
        else:
            successful[lfn] = {"Size": len(lfn), "Checksum": adler32(lfn.encode())}

    return S_OK({"Successful": successful, "Failed": failed})


def mock_SE_getFileMetadata(self, lfns, **kwargs):
    if isinstance(lfns, str):
        lfns = [lfns]

    successful = {}
    failed = {}
    for lfn in lfns:
        instructions = _splitLFN(lfn)
        if lfn == ERROR_GETTING_SE_METADATA:
            return S_ERROR(f"Complete failure {lfn}")

        elif MARKER_BAD_SE_NOT_EXIST in instructions:
            badSEs = instructions[MARKER_BAD_SE_NOT_EXIST].split("/")
            if self.name in badSEs:
                failed[lfn] = os.strerror(errno.ENOENT)
                continue
        elif MARKER_BAD_SE_REPLICAS in instructions:
            badSEs = instructions[MARKER_BAD_SE_REPLICAS].split("/")
            if self.name in badSEs:
                successful[lfn] = {"Size": len(lfn), "Checksum": adler32(b"bad!")}
                continue

        successful[lfn] = {"Size": len(lfn), "Checksum": adler32(lfn.encode())}

    return S_OK({"Successful": successful, "Failed": failed})


@pytest.fixture(scope="function", autouse=True)
def monkeypatchForAllTest(monkeypatch):
    """ " This fixture will run for all test method and will mockup
    a few DMS methods
    """
    monkeypatch.setattr(
        DIRAC.DataManagementSystem.Client.DataManager.DataManager,
        "getReplicas",
        mock_DM_getReplicas,
    )
    monkeypatch.setattr(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog, "getFileMetadata", mock_FC_getFileMetadata, raising=False
    )
    monkeypatch.setattr(
        DIRAC.Resources.Storage.StorageElement.StorageElementItem,
        "getFileMetadata",
        mock_SE_getFileMetadata,
        raising=False,
    )


def _compareFileAttr(stateBefore, stateAfter, attrExpectedToDiffer):
    """Given a json dump of the state before and after,
    make sure that the attributes that changed are those expected
    """
    setBefore = set(json.loads(stateBefore["Value"]).items())
    setAfter = set(json.loads(stateAfter["Value"]).items())
    assert {t[0] for t in setBefore ^ setAfter} == attrExpectedToDiffer


def test_nonExistingFile():
    """Test case of a file that does not exist"""
    rmsFile = File()
    rmsFile.LFN = NON_EXISTING_LFN
    res = filterReplicas(rmsFile)
    assert not res["OK"]
    assert rmsFile.Status == "Failed"
    assert rmsFile.Error == os.strerror(errno.ENOENT)


def test_errorGettingReplicas():
    """When we have a complete failure of getting the replicas"""
    rmsFile = File()
    rmsFile.LFN = ERROR_GETTING_REPLICAS
    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    # The failure should be transmitted
    assert not res["OK"]
    assert ERROR_GETTING_REPLICAS in res["Message"]
    # Make sure the File status did not change
    assert stateBefore == stateAfter


def test_errorGettingFCMetadata():
    """When we have a complete failure of getting the file metadata from FC,
    we just keep on going
    """
    rmsFile = File()
    rmsFile.LFN = ERROR_GETTING_FC_METADATA
    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]

    # We should have a Valid replicas
    assert DEFAULT_SE in res["Value"]["Valid"]

    # Since the rest of the function goes fine
    # and we can get the checksum through the SE,
    # the state does change: checksum and checksum types are set
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})


def test_fileHasTwoOKReplicas():
    """Here we test that if a file has two replicas that
    are fine, we get them both as option"""
    rmsFile = File()
    rmsFile.LFN = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER)
    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()
    assert res["OK"]
    assert "Disk1" in res["Value"]["Valid"]
    assert "Disk2" in res["Value"]["Valid"]

    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})


def test_filterOutReplicas():
    """We test that the filtering works"""

    # Take a file with two replicas, and restrict the source to a single one
    rmsFile = File()
    rmsFile.LFN = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER)
    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile, opSources=["Disk1"])
    stateAfter = rmsFile.toJSON()
    assert res["OK"]
    assert ["Disk1"] == res["Value"]["Valid"]
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})


def test_filterOutImpossibleReplicas():
    """Restrict the replicas to none of the possibility
    The Valid replicas will be empty,
    while all the others will be considered as NoActiveReplicas
    """

    # Take a file with two replicas, and restrict the source to a third one
    rmsFile = File()
    rmsFile.LFN = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER)
    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile, opSources=["Disk3"])
    stateAfter = rmsFile.toJSON()

    assert res["OK"]
    assert res["Value"]["Valid"] == []
    assert set(res["Value"]["NoActiveReplicas"]) == {"Disk1", "Disk2"}

    # The attributes should still be updated
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})


def test_rmsFileChecksumDifferentFromSE():
    """If the RMS File has a checksum different from the one in the SE,
    the replicas at that SE should be marked as bad
    """

    rmsFile = File()
    # We specify a checksum different from the SE
    rmsFile.Checksum = adler32(b"notthegoodone")

    rmsFile.LFN = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", END_MARKER)
    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]
    # The replicas should be considered bad
    assert res["Value"]["Bad"] == ["Disk1"]
    # No changes should have happened to the file

    assert stateBefore == stateAfter


def test_fileHasMultipleBadReplicas():
    """One file has multiple replicas, and all of them are bad"""

    rmsFile = File()

    # We want two replicas at disk1 and  disk2
    lfnParts = ["/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER]

    # all of which are bad
    lfnParts += [MARKER_BAD_SE_REPLICAS, "Disk1", "Disk2", END_MARKER]

    rmsFile.LFN = os.path.join(*lfnParts)

    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]
    # The replicas should be considered bad
    assert res["Value"]["Bad"] == ["Disk1", "Disk2"]

    # We should have added the catalog checksum
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})
    assert rmsFile.Checksum == adler32(rmsFile.LFN.encode())


def test_fileHasOneGoodAndOneBadReplicas():
    """The file has one good and one bad replicas"""
    rmsFile = File()

    # We want two replicas at disk1 and  disk2
    lfnParts = ["/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER]

    # one of which are bad
    lfnParts += [MARKER_BAD_SE_REPLICAS, "Disk1", END_MARKER]

    rmsFile.LFN = os.path.join(*lfnParts)

    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]
    # The replicas at Disk1 should be considered bad
    assert res["Value"]["Bad"] == ["Disk1"]
    # The replicas at Disk2 should be considered valid
    assert res["Value"]["Valid"] == ["Disk2"]

    # We should have added the catalog checksum
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})
    assert rmsFile.Checksum == adler32(rmsFile.LFN.encode())


def test_fileHasDiskAndTapeReplicas():
    """The file has two disks and one tape replicas, we should favor the disk"""
    rmsFile = File()

    lfnParts = ["/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", "Disk3", "Tape1", END_MARKER]

    # Assume disk2 is bad
    lfnParts += [MARKER_BAD_SE_REPLICAS, "Disk3", END_MARKER]

    rmsFile.LFN = os.path.join(*lfnParts)

    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]

    # We only want the disk replicas
    assert res["Value"]["Valid"] == ["Disk1", "Disk2"]
    assert res["Value"]["Bad"] == ["Disk3"]

    # We should have added the catalog checksum
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})
    assert rmsFile.Checksum == adler32(rmsFile.LFN.encode())


def test_fileHasOnlyTapeReplicas():
    """The file has only tape replicas, we should ues them"""
    rmsFile = File()

    lfnParts = ["/", MARKER_WITH_REPLICAS, "Tape1", "Tape2", END_MARKER]

    rmsFile.LFN = os.path.join(*lfnParts)

    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]

    assert res["Value"]["Valid"] == ["Tape1", "Tape2"]

    # We should have added the catalog checksum
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})
    assert rmsFile.Checksum == adler32(rmsFile.LFN.encode())


@pytest.mark.xfail(reason="https://github.com/DIRACGrid/DIRAC/issues/6689")
def test_fileHasDiskReplicasButWeFilterOnTape():
    """If we have a disk and tape replicas, but specify the tape replicas as source,
    we want the tape one
    """
    rmsFile = File()
    rmsFile.LFN = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", "Tape1", END_MARKER)
    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile, opSources=["Tape1"])
    stateAfter = rmsFile.toJSON()

    assert res["OK"]
    assert res["Value"]["Valid"] == ["Tape1"]

    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})


def test_noActiveReplicas():
    """The file has no replicas available"""
    rmsFile = File()

    rmsFile.LFN = NO_AVAILABLE_REPLICAS

    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]
    # When no replicas are available, we just get "None"
    assert res["Value"]["NoReplicas"] == [None]

    # We should have added the catalog checksum
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})
    assert rmsFile.Checksum == adler32(rmsFile.LFN.encode())


def test_errorGettingSEMetadata():
    """When we have a complete failure of getting the file metadata from SE,
    the files goes to "NoMetadata"
    """
    rmsFile = File()
    rmsFile.LFN = ERROR_GETTING_SE_METADATA
    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]

    # We should have a Valid replicas
    assert DEFAULT_SE in res["Value"]["NoMetadata"]

    # We got the checksum from the FC
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})


def test_fileRegisteredButDoesNotPhysicalyExist():
    """File has two replicas registered, but one of them does not exist"""
    rmsFile = File()

    # Two replicas
    lfnParts = ["/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER]
    # but it doesn't exist
    lfnParts += [MARKER_BAD_SE_NOT_EXIST, "Disk2", END_MARKER]

    rmsFile.LFN = os.path.join(*lfnParts)

    stateBefore = rmsFile.toJSON()
    res = filterReplicas(rmsFile)
    stateAfter = rmsFile.toJSON()

    assert res["OK"]

    assert res["Value"]["Valid"] == ["Disk1"]
    assert res["Value"]["NoReplicas"] == ["Disk2"]
    # We should have added the catalog checksum
    _compareFileAttr(stateBefore, stateAfter, {"ChecksumType", "Checksum"})
