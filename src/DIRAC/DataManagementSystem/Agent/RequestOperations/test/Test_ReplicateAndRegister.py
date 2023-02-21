""" That is probably one of the most convoluted unit test file we have...
It could have been done in a simpler way, but I want to use this test as a "doc"
as I am sure this trick will show useful one day...

The function to be tested is filterReplicas. The feature I am adding when writing these
tests is a cache: instead of calling getReplicas everytime in the filterReplicas function,
I call it once before for all the files, and pass the result to filterReplicas.

In order to test this new feature, I want to be able to repeat each test, once with the cache,
and once without.
This could easily be done with a fixture, where I would call getReplicas if needed.

But what if I want to call getReplicas with ALL the lfns at once, and use that result
(just like I would do in real life) ? If I want to avoid keeping a static list of LFNs
up to date, I have to collect it dynamically. And this is what I do

Another interesting feature of this test is that I am basing the behavior of the mocks
(getReplicas, getFileMetadata, etc) on the LFN itself.
"""
import errno
import json
import os
import re
import pytest
import DIRAC

from abc import ABC, abstractmethod

from zlib import adler32

from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister import filterReplicas

from DIRAC.RequestManagementSystem.Client.File import File


# Although we will in general use regular expression to drive the behavior
# of the mocks, there are a few very specific files.
# So we may as well just use them as constant.
# They are mostly cases that return S_ERROR straight away....

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


# Now starts the regular expression magic.
# The idea is that the LFN will be structured like
# /MARKER_XXX/value1/value2/END/MARKER_YYY/value3/END...
# We split the LFN based on the marker and end mark,
# and each mock looks for the marker it cares about
# (for example MARKER_BAD_SE_REPLICAS for the mock_SE_getReplicas)

# The file replicas at given SEs
# The "Disk" and "Tape" part are interpreted as such
MARKER_WITH_REPLICAS = "WITH_REPLICAS"

# To declare that the specific replica has wrong cks
MARKER_BAD_SE_REPLICAS = "BAD_SE_REPLICAS"

# The cks is wrong in the FC
MARKER_BAD_FC_REPLICAS = "BAD_FC_REPLICAS"

# The file physicaly does not exist but is registered
MARKER_BAD_SE_NOT_EXIST = "BAD_SE_NOT_EXIST"

# The end marker, same for all
END_MARKER = "END"

# Collect all the markers to build the regular expression
ALL_MARKERS = "|".join(v for k, v in globals().items() if k.startswith("MARKER_"))
# That is the regular expression to split the LFN
# /MARKER1/val1/val2/END/MARKER2/val3/END...
MARKER_PATTERN = rf"/*(?P<pat>({ALL_MARKERS})+)/(?P<values>.*)/"


def _splitLFN(lfn):
    """Split the LFN based on the pattern"""
    instructions = {}
    try:
        # Split at each end marker
        for group in re.split(rf"{END_MARKER}", lfn):
            if not group:
                continue

            # interpret the specific pattern
            matchgroup = re.match(MARKER_PATTERN, group).groupdict()
            instructions[matchgroup["pat"]] = matchgroup["values"]
    except Exception:
        pass
    return instructions


def mock_DM_getReplicas(self, lfns, **kwargs):
    """Mock the DataManager.getReplicas method.

    It returns based on the LFN.
    See the code for details.

    :param preferDisk: only return disk replicas if any

    """
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
                if any("Disk" in se for se in ses):
                    ses = [se for se in ses if "Disk" in se]

            successful[lfn] = dict.fromkeys(ses)

        else:
            successful[lfn] = {DEFAULT_SE: None}
    return S_OK({"Successful": successful, "Failed": failed})


def mock_FC_getFileMetadata(self, lfns, **kwargs):
    """
    Return the FC metadata of the files.
    The returned checksum just corresponds to the adler32 of the LFN
    See code for details
    """

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
    """
    Return the SE metadata of the files.
    See code for details
    """
    if isinstance(lfns, str):
        lfns = [lfns]

    successful = {}
    failed = {}
    for lfn in lfns:
        instructions = _splitLFN(lfn)
        # If a complete failure, return
        if lfn == ERROR_GETTING_SE_METADATA:
            return S_ERROR(f"Complete failure {lfn}")
        # if a replicas is marked as not existing, return
        # the specific No such file or directory error
        elif MARKER_BAD_SE_NOT_EXIST in instructions:
            badSEs = instructions[MARKER_BAD_SE_NOT_EXIST].split("/")
            if self.name in badSEs:
                failed[lfn] = os.strerror(errno.ENOENT)
                continue
        # If a replicas is marked as bad, return a wrong checksum
        elif MARKER_BAD_SE_REPLICAS in instructions:
            badSEs = instructions[MARKER_BAD_SE_REPLICAS].split("/")
            if self.name in badSEs:
                successful[lfn] = {"Size": len(lfn), "Checksum": adler32(b"bad!")}
                continue

        successful[lfn] = {"Size": len(lfn), "Checksum": adler32(lfn.encode())}

    return S_OK({"Successful": successful, "Failed": failed})


@pytest.fixture(scope="function", autouse=True)
def monkeypatchForAllTest(monkeypatch):
    """This fixture will run for all test methods and will mock
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


def runAsPytest(cls):
    """
    Wizardry
    This is a class decorator. It has several purposes:

    * allows pytest to discover the test classes (see below) by turning a class
        to a function (a callable class does not cut it for pytest)
    * parametrize all the tests (with or without the replicas cache)
    * implements the test logic (prepare, execute filterReplicas, assert results)
    * collects all the LFNs of all the tests
    """

    # instantiate the test class
    clsInst = cls()

    # Parametrize the function
    @pytest.mark.parametrize("withReplicasCache", [True, False])
    def func(withReplicasCache):
        # Get the rmsFile to test
        rmsFile = clsInst.initialize_test()
        # Get the activeReplicas cache (or None...)
        activeReplicas = getattr(runAsPytest, "allValidReplicas") if withReplicasCache else None

        # Call the filterReplicas function on the rmsFile, and specify the kwargs
        # specific to the test case (like opSources)
        res = filterReplicas(rmsFile, activeReplicas=activeReplicas, **clsInst.filterReplicasKwargs)

        # Check the test results
        clsInst.check_result(res)

    # We keep the instantiated class as an attribute
    # of the decorator.
    # This is used in test_all
    func.inner_cls = clsInst

    # Here we collect all the LFNs in each test and add them to the set
    # "allLFNs", that we keep as a runAsPytest function attribute.
    # This part is executed upon import, so by the time the tests actually
    # execute, the list of LFN is complete
    # The replicas cache is built from this list of LFN at the end of the file
    #
    # Extra note about:
    # {clsInst.lfn} if isinstance(clsInst.lfn, str) else set(clsInst.lfn)
    # this is not needed here because each test case is for a single LFN,
    # but there could be test cases with multiple LFNs, in which case this is needed
    # (see test_case_all below )
    setattr(
        runAsPytest,
        "allLFNs",
        getattr(runAsPytest, "allLFNs", set()) | {clsInst.lfn} if isinstance(clsInst.lfn, str) else set(clsInst.lfn),
    )

    return func


class BaseTestCase(ABC):
    """
    This is the base class for all our tests. These classes are meant to be
    instantiated, and the logic executed by the runAsPytest decorator
    """

    # kwargs to pass to filterReplicas
    filterReplicasKwargs = {}
    # The rmsFile generated by the test
    rmsFile = None

    # The lfn that will be used for the rmsFile
    # It must be a class attribute to be availabe
    # for runAsPytest decorator to collect it
    lfn = None

    # A json dump of the state of the rmsFile
    # before going through the filterReplicas function
    stateBefore = None

    def initialize_test(self):
        """
        Create the rmsFile and store the following attribute

        :returns: the rmsFile
        """
        self.rmsFile = File()
        self.rmsFile.LFN = self.lfn
        self.stateBefore = self.rmsFile.toJSON()
        return self.rmsFile

    @abstractmethod
    def check_result(self, res):
        """This takes the output of filterReplicas
        and perform whichever assert it expects
        """
        ...


@runAsPytest
class test_case_nonExistingFile(BaseTestCase):
    """Test case of a file that does not exist"""

    lfn = NON_EXISTING_LFN

    def check_result(self, res):
        assert not res["OK"]
        assert self.rmsFile.Status == "Failed"
        assert self.rmsFile.Error == os.strerror(errno.ENOENT)


# We don't run that one as a parametric case,
# as the failure of getting replicas would show up
# before calling filterReplicas in case we use the
# replicas cache
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


@runAsPytest
class test_case_errorGettingFCMetadata(BaseTestCase):
    """When we have a complete failure of getting the file metadata from FC,
    we just keep on going
    """

    lfn = ERROR_GETTING_FC_METADATA

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]

        # We should have a Valid replicas
        assert DEFAULT_SE in res["Value"]["Valid"]

        # Since the rest of the function goes fine
        # and we can get the checksum through the SE,
        # the state does change: checksum and checksum types are set
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})


@runAsPytest
class test_case_fileHasTwoOKReplicas(BaseTestCase):
    """Here we test that if a file has two replicas that
    are fine, we get them both as option"""

    lfn = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER)

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()
        assert res["OK"]
        assert sorted(res["Value"]["Valid"]) == sorted(["Disk1", "Disk2"])

        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})


@runAsPytest
class test_case_filterOutReplicas(BaseTestCase):
    """We test that the filtering works"""

    # Take a file with two replicas, and restrict the source to a single one
    lfn = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER)

    def initialize_test(self):
        rmsFile = super().initialize_test()
        self.filterReplicasKwargs = {"opSources": ["Disk1"]}
        return rmsFile

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()
        assert res["OK"]
        assert ["Disk1"] == res["Value"]["Valid"]
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})


@runAsPytest
class test_case_filterOutImpossibleReplicas(BaseTestCase):
    """Restrict the replicas to none of the possibility
    The Valid replicas will be empty,
    while all the others will be considered as NoActiveReplicas
    """

    # Take a file with two replicas, and restrict the source to a third one
    lfn = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER)

    def initialize_test(self):
        rmsFile = super().initialize_test()
        self.filterReplicasKwargs = {"opSources": ["Disk3"]}
        return rmsFile

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]
        assert res["Value"]["Valid"] == []
        assert set(res["Value"]["NoActiveReplicas"]) == {"Disk1", "Disk2"}

        # The attributes should still be updated
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})


@runAsPytest
class test_case_rmsFileChecksumDifferentFromSE(BaseTestCase):
    """If the RMS File has a checksum different from the one in the SE,
    the replicas at that SE should be marked as bad
    """

    lfn = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", END_MARKER)

    def initialize_test(self):
        rmsFile = super().initialize_test()
        # We specify a checksum different from the SE
        rmsFile.Checksum = adler32(b"notthegoodone")
        self.stateBefore = rmsFile.toJSON()
        return rmsFile

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]
        # The replicas should be considered bad
        assert res["Value"]["Bad"] == ["Disk1"]
        # No changes should have happened to the file

        assert self.stateBefore == stateAfter


@runAsPytest
class test_case_fileHasMultipleBadReplicas(BaseTestCase):
    """One file has multiple replicas, and all of them are bad"""

    # We want two replicas at disk1 and  disk2
    lfnParts = ["/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER]

    # all of which are bad
    lfnParts += [MARKER_BAD_SE_REPLICAS, "Disk1", "Disk2", END_MARKER]

    lfn = os.path.join(*lfnParts)

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]
        # The replicas should be considered bad
        assert res["Value"]["Bad"] == ["Disk1", "Disk2"]

        # We should have added the catalog checksum
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})

        assert self.rmsFile.Checksum == adler32(self.rmsFile.LFN.encode())


@runAsPytest
class test_case_fileHasOneGoodAndOneBadReplicas(BaseTestCase):
    """The file has one good and one bad replicas"""

    # We want two replicas at disk1 and  disk2
    lfnParts = ["/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER]

    # one of which are bad
    lfnParts += [MARKER_BAD_SE_REPLICAS, "Disk1", END_MARKER]

    lfn = os.path.join(*lfnParts)

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]
        # The replicas at Disk1 should be considered bad
        assert res["Value"]["Bad"] == ["Disk1"]
        # The replicas at Disk2 should be considered valid
        assert res["Value"]["Valid"] == ["Disk2"]

        # We should have added the catalog checksum
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})
        assert self.rmsFile.Checksum == adler32(self.rmsFile.LFN.encode())


@runAsPytest
class test_case_fileHasDiskAndTapeReplicas(BaseTestCase):
    """The file has two disks and one tape replicas, we should favor the disk"""

    lfnParts = ["/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", "Disk3", "Tape1", END_MARKER]

    # Assume disk2 is bad
    lfnParts += [MARKER_BAD_SE_REPLICAS, "Disk3", END_MARKER]

    lfn = os.path.join(*lfnParts)

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]

        # We only want the disk replicas
        assert res["Value"]["Valid"] == ["Disk1", "Disk2"]
        assert res["Value"]["Bad"] == ["Disk3"]

        # We should have added the catalog checksum
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})
        assert self.rmsFile.Checksum == adler32(self.rmsFile.LFN.encode())


@runAsPytest
class test_case_fileHasOnlyTapeReplicas(BaseTestCase):
    """The file has only tape replicas, we should ues them"""

    lfnParts = ["/", MARKER_WITH_REPLICAS, "Tape1", "Tape2", END_MARKER]

    lfn = os.path.join(*lfnParts)

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]

        assert res["Value"]["Valid"] == ["Tape1", "Tape2"]

        # We should have added the catalog checksum
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})

        assert self.rmsFile.Checksum == adler32(self.rmsFile.LFN.encode())


@pytest.mark.xfail(reason="https://github.com/DIRACGrid/DIRAC/issues/6689")
@runAsPytest
class test_case_fileHasDiskReplicasButWeFilterOnTape(BaseTestCase):
    """If we have a disk and tape replicas, but specify the tape replicas as source,
    we want the tape one
    """

    lfn = os.path.join("/", MARKER_WITH_REPLICAS, "Disk1", "Tape1", END_MARKER)

    def initialize_test(self):
        rmsFile = super().initialize_test()
        self.filterReplicasKwargs = {"opSources": ["Tape1"]}
        return rmsFile

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]
        assert res["Value"]["Valid"] == ["Tape1"]

        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})


@runAsPytest
class test_case_noActiveReplicas(BaseTestCase):
    """The file has no replicas available"""

    lfn = NO_AVAILABLE_REPLICAS

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]
        # When no replicas are available, we just get "None"
        assert res["Value"]["NoReplicas"] == [None]

        # We should have added the catalog checksum
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})

        assert self.rmsFile.Checksum == adler32(self.rmsFile.LFN.encode())


@runAsPytest
class test_case_errorGettingSEMetadata(BaseTestCase):
    """When we have a complete failure of getting the file metadata from SE,
    the files goes to "NoMetadata"
    """

    lfn = ERROR_GETTING_SE_METADATA

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]

        # We should have a Valid replicas
        assert DEFAULT_SE in res["Value"]["NoMetadata"]

        # We got the checksum from the FC

        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})


@runAsPytest
class test_case_fileRegisteredButDoesNotPhysicalyExist(BaseTestCase):
    """File has two replicas registered, but one of them does not exist"""

    # Two replicas
    lfnParts = ["/", MARKER_WITH_REPLICAS, "Disk1", "Disk2", END_MARKER]
    # but it doesn't exist
    lfnParts += [MARKER_BAD_SE_NOT_EXIST, "Disk2", END_MARKER]

    lfn = os.path.join(*lfnParts)

    def check_result(self, res):
        stateAfter = self.rmsFile.toJSON()

        assert res["OK"]

        assert res["Value"]["Valid"] == ["Disk1"]
        assert res["Value"]["NoReplicas"] == ["Disk2"]
        # We should have added the catalog checksum
        _compareFileAttr(self.stateBefore, stateAfter, {"ChecksumType", "Checksum"})


@pytest.mark.xfail(reason="filterReplicas does not support bulk")
@runAsPytest
class test_all(BaseTestCase):
    """
    This test re-run all the other tests, but by giving all the LFNs at once.
    I wanted to make filterReplicas a bulk method (not a single rmsFile, but multiple),
    so this would have been a neat way of testing it.
    I've decided not to do it in the end, but I keep the test here, again for doc purpose
    (It could run as a standard pytest, but it is so elegant to re-use the
     BaseTestClass and runAsPytest decorator...)
    """

    # We collect all the test cases that were decorated with @runAsPytest
    # and store the underlying class instance
    allTestCases = [getattr(v, "inner_cls") for k, v in globals().items() if k.startswith("test_case")]

    # Use all the LFNs as input for filterReplicas
    lfn = getattr(runAsPytest, "allLFNs")

    def initialize_test(self):
        # Initialize all the rmsFiles
        self.allRmsFiles = [x.initialize_test() for x in self.allTestCases]
        # Store all the json states
        self.allStateBefore = [rf.toJSON() for rf in self.allRmsFiles]
        return self.allRmsFiles

    def check_result(self, res):
        # Here res would be a bulk output like Successful/Failed
        # So you would need to map the entry in it to one of the
        # instance in allTestCases (you could do a dict indexed by the LFN)
        # And you would call check_result on each of them
        raise NotImplementedError


# Here we store the output of getReplicas as the allValidReplicas attribute
# of the runAsPytest decorator.
# By the time we reach here, all the tests with decorator have been parsed,
# so the list of LFNs is complete
setattr(
    runAsPytest,
    "allValidReplicas",
    mock_DM_getReplicas(None, getattr(runAsPytest, "allLFNs"), preferDisk=True)["Value"],
)
