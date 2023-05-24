""" Unit tests for JDL module"""

# pylint: disable=protected-access, invalid-name

from io import StringIO
from unittest.mock import patch

import pytest

from DIRAC import S_OK
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.JDL import (
    OWNER,
    OWNER_GROUP,
    VO,
    jdlToBaseJobDescriptionModel,
    dumpJobDescriptionModelAsJDL,
    jdlToJobDescriptionModel,
)
from DIRAC.Interfaces.API.Job import Job
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.WorkloadManagementSystem.Utilities.JobModel import JobDescriptionModel


@pytest.fixture(name="job")
def fixture_job():
    """Fixture for a complete job object"""
    with patch("DIRAC.Core.Base.API.getSites", return_value=S_OK(["LCG.IN2P3.fr"])):
        job = Job()
    job.setConfigArgs("configArgs")
    job.setCPUTime(3600)
    job.setDestination("LCG.IN2P3.fr")
    with patch("DIRAC.Interfaces.API.Job.getCESiteMapping", return_value=S_OK({"some.ce.IN2P3.fr": "LCG.IN2P3.fr"})):
        job.setDestinationCE("some.ce.IN2P3.fr")
    job.setExecutable("/bin/echo", arguments="arguments", logFile="logFile")
    job.setExecutionEnv({"INTVAR": 1, "STRVAR": "a"})
    job.setInputData(["/lhcb/production/DC04/v2/DST/00000742_00003493_10.dst"])
    with patch(
        "DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations.getValue",
        return_value="DIRAC.WorkloadManagementSystem.Client.DownloadInputData",
    ):
        job.setInputDataPolicy("download")
    job.setInputSandbox(["inputfile.opts"])
    job.setJobGroup("1234abcd")
    job.setLogLevel("DEBUG")
    job.setName("JobName")
    job.setNumberOfProcessors(3)
    job.setOutputData(["outputfile.root"], outputSE="IN2P3-disk", outputPath="/myjobs/1234")
    job.setOutputSandbox(["inputfile.opts"])
    with patch("DIRAC.Interfaces.API.Job.getDIRACPlatforms", return_value=S_OK("x86_64-slc6-gcc49-opt")):
        job.setPlatform("x86_64-slc6-gcc49-opt")
    job.setPriority(10)
    job.setTag(["WholeNode", "8GBMemory"])
    job.setType("Test")

    # Custom job
    job._addJDLParameter("ExtraInt", 1)
    job._addJDLParameter("ExtraFloat", 1.0)
    job._addJDLParameter("ExtraString", "test")
    # The 3 lines below are not a use case given that workflow parameters
    # must be strings, ints or floats
    # job._addJDLParameter("ExtraIntList", ";".join(["1", "2", "3"]))
    # job._addJDLParameter("ExtraFloatList", ";".join(["1.0", "2.0", "3.0"]))
    # job._addJDLParameter("ExtraStringList",";".join(["a", "b", "c"]))

    yield job


def test_jdlToBaseJobDescriptionModel_valid(job: Job):
    """This test makes sure that a job object can be parsed by the jdlToBaseJobDescriptionModel method"""
    # Arrange
    job.setParameterSequence("IntSequence", [1, 2, 3])
    job.setParameterSequence("StrSequence", ["a", "b", "c"])
    job.setParameterSequence("FloatSequence", [1.0, 2.0, 3.0])

    assert not job.errorDict

    xml = job._toXML()
    jdl = f"[{job._toJDL(jobDescriptionObject=StringIO(xml))}]"

    # Assert
    res = jdlToBaseJobDescriptionModel(ClassAd(jdl))
    assert res["OK"], res["Message"]

    data = res["Value"].dict()
    with patch(
        "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getDIRACPlatforms",
        return_value=S_OK(["x86_64-slc6-gcc49-opt"]),
    ):
        with patch(
            "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getSites",
            return_value=S_OK(["LCG.IN2P3.fr"]),
        ):
            assert JobDescriptionModel(owner="owner", ownerGroup="ownerGroup", vo="lhcb", **data)


@pytest.mark.parametrize(
    "jdl",
    [
        """[]""",  # No executable
        """[Executable="";]""",  # Empty executable
        """Executable="executable";""",  # Missing brackets
    ],
)
def test_jdlToBaseJobDescriptionModel_invalid(jdl):
    """This test makes sure that a job object without an executable raises an error"""
    # Arrange

    # Act
    res = jdlToBaseJobDescriptionModel(ClassAd(jdl))

    # Assert
    assert not res["OK"], res["Value"]


def test_jdlToJobDescriptionModel_valid(job: Job):
    """Test that the jdlToJobDescriptionModel method can parse a valid JDL with credentials"""
    # Arrange
    assert not job.errorDict
    xml = job._toXML()
    jdl = f"[{job._toJDL(jobDescriptionObject=StringIO(xml))}]"
    classAd = ClassAd(jdl)
    classAd.insertAttributeString(OWNER, "owner")
    classAd.insertAttributeString(OWNER_GROUP, "ownerGroup")
    classAd.insertAttributeString(VO, "lhcb")

    # Act
    with patch(
        "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getDIRACPlatforms",
        return_value=S_OK(["x86_64-slc6-gcc49-opt"]),
    ):
        with patch(
            "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getSites",
            return_value=S_OK(["LCG.IN2P3.fr"]),
        ):
            res = jdlToJobDescriptionModel(classAd)

    # Assert
    assert res["OK"], res["Message"]
    jobDescriptionModel = res["Value"]
    assert jobDescriptionModel.owner == "owner"
    assert jobDescriptionModel.ownerGroup == "ownerGroup"
    assert jobDescriptionModel.vo == "lhcb"


def test_jdlToJobDescriptionModel_invalid(job: Job):
    """Test that the jdlToJobDescriptionModel method raises an error when the JDL does not contain credentials"""
    # Arrange
    xml = job._toXML()
    jdl = f"[{job._toJDL(jobDescriptionObject=StringIO(xml))}]"
    classAd = ClassAd(jdl)

    # Act
    with patch(
        "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getDIRACPlatforms",
        return_value=S_OK(["x86_64-slc6-gcc49-opt"]),
    ):
        with patch(
            "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getSites",
            return_value=S_OK(["LCG.IN2P3.fr"]),
        ):
            res = jdlToJobDescriptionModel(classAd)

    # Assert
    assert not res["OK"], res["Value"]


def test_dumpJobDescriptionModelAsJDL(job: Job):
    """This test makes sure that a job that has been converted to the JSON format can be dumped as a JDL again"""
    # Arrange
    assert not job.errorDict

    xml = job._toXML()
    jdl = f"[{job._toJDL(jobDescriptionObject=StringIO(xml))}]"

    res = jdlToBaseJobDescriptionModel(ClassAd(jdl))
    assert res["OK"], res["Message"]

    data = res["Value"].dict(exclude_none=True)
    with patch(
        "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getDIRACPlatforms",
        return_value=S_OK(["x86_64-slc6-gcc49-opt"]),
    ):
        with patch(
            "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getSites",
            return_value=S_OK(["LCG.IN2P3.fr"]),
        ):
            jobDescriptionModel = JobDescriptionModel(owner="owner", ownerGroup="ownerGroup", vo="lhcb", **data)

    # Act
    resolvedJDL = dumpJobDescriptionModelAsJDL(jobDescriptionModel).asJDL()

    # Assert
    # We make sure that, besides the spaces, the output from JobManifest is the same as the input
    assert JobManifest(resolvedJDL).dumpAsJDL().replace(" ", "") == resolvedJDL.replace(" ", "")
