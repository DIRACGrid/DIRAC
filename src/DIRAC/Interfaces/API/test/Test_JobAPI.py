""" Basic unit tests for the Job API
"""

# pylint: disable=missing-docstring, protected-access

from io import StringIO
from os.path import dirname, join
import pytest

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Interfaces.API.Job import Job


def test_basicJob():
    job = Job()

    job.setOwner("ownerName")
    job.setOwnerGroup("ownerGroup")
    job.setName("jobName")
    job.setJobGroup("jobGroup")
    job.setExecutable("someExe")
    job.setType("jobType")
    job.setDestination("ANY")

    xml = job._toXML()

    with open(join(dirname(__file__), "testWF.xml")) as fd:
        expected = fd.read()

    assert xml == expected

    with open(join(dirname(__file__), "testWFSIO.jdl")) as fd:
        expected = fd.read()

    jdlSIO = job._toJDL(jobDescriptionObject=StringIO(job._toXML()))
    assert jdlSIO == expected


def test_SimpleParametricJob():
    job = Job()
    job.setExecutable("myExec")
    job.setLogLevel("DEBUG")
    parList = [1, 2, 3]
    job.setParameterSequence("JOB_ID", parList, addToWorkflow=True)
    inputDataList = [
        ["/lhcb/data/data1", "/lhcb/data/data2"],
        ["/lhcb/data/data3", "/lhcb/data/data4"],
        ["/lhcb/data/data5", "/lhcb/data/data6"],
    ]
    job.setParameterSequence("InputData", inputDataList, addToWorkflow=True)

    jdl = job._toJDL()

    with open(join(dirname(__file__), "testWF.jdl")) as fd:
        expected = fd.read()

    assert jdl == expected

    clad = ClassAd("[" + jdl + "]")

    arguments = clad.getAttributeString("Arguments")
    job_id = clad.getAttributeString("JOB_ID")
    inputData = clad.getAttributeString("InputData")

    assert job_id == "%(JOB_ID)s"
    assert inputData == "%(InputData)s"
    assert "jobDescription.xml" in arguments
    assert "-o LogLevel=DEBUG" in arguments
    assert "-p JOB_ID=%(JOB_ID)s" in arguments
    assert "-p InputData=%(InputData)s" in arguments


@pytest.mark.parametrize(
    "proc, minProc, maxProc, expectedProc, expectedMinProc, expectedMaxProc",
    [
        (4, None, None, 4, None, 4),
        (4, 2, None, 4, None, 4),
        (4, 2, 8, 4, None, 4),
        (4, 8, 6, 8, None, 8),  # non-sense
        (None, 2, 8, None, 2, 8),
        (None, 1, None, None, 1, None),
        (None, None, 8, None, 1, 8),
        (None, 8, 8, 8, None, 8),
        (None, 12, 8, 8, None, 8),  # non-sense
    ],
)
def test_setNumberOfProcessors(proc, minProc, maxProc, expectedProc, expectedMinProc, expectedMaxProc):
    # Arrange
    job = Job()

    # Act
    res = job.setNumberOfProcessors(proc, minProc, maxProc)

    # Assert
    assert res["OK"], res["Message"]
    jobDescription = ClassAd(f"[{job._toJDL()}]")
    assert expectedProc == jobDescription.getAttributeInt("NumberOfProcessors")
    assert expectedMinProc == jobDescription.getAttributeInt("MinNumberOfProcessors")
    assert expectedMaxProc == jobDescription.getAttributeInt("MaxNumberOfProcessors")


@pytest.mark.parametrize(
    "sites, expectedSites",
    [
        ("", ""),
        ("Any", ""),
        ("ANY", ""),
        ([""], ""),
        (["ANY"], ""),
        (["", "ANY"], ""),
        ("LCG.CERN.ch", ["LCG.CERN.ch"]),
        (["LCG.CERN.ch", "ANY", ""], ["LCG.CERN.ch"]),
        (["LCG.CERN.ch", "LCG.IN2P3.fr"], ["LCG.CERN.ch", "LCG.IN2P3.fr"]),
        (["LCG.CERN.ch", "ANY", "LCG.IN2P3.fr"], ["LCG.CERN.ch", "LCG.IN2P3.fr"]),
    ],
)
def test_setDestination_successful(sites, expectedSites):
    # Arrange
    job = Job()
    job._siteSet = {"LCG.CERN.ch", "LCG.IN2P3.fr"}

    # Act
    res = job.setDestination(sites)

    # Assert
    assert res["OK"], res["Message"]
    jobDescription = ClassAd(f"[{job._toJDL()}]")

    if expectedSites:
        assert jobDescription.lookupAttribute("Site")
        assert set(jobDescription.getListFromExpression("Site")) == set(expectedSites)
    else:
        assert not jobDescription.lookupAttribute("Site"), jobDescription.getListFromExpression("Site")


@pytest.mark.parametrize(
    "sites",
    [
        (["LCG.NCBJ.pl"]),
        (["LCG.CERN.ch", "LCG.NCBJ.pl"]),
    ],
)
def test_setDestination_unsuccessful(sites):
    # Arrange
    job = Job()
    job._siteSet = {"LCG.CERN.ch"}

    # Act
    res = job.setDestination(sites)

    # Assert
    assert not res["OK"], res["Value"]
