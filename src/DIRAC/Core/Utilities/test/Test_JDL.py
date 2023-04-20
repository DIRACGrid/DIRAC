""" Unit tests for JDL module"""

# pylint: disable=protected-access, invalid-name

from io import StringIO
from unittest.mock import patch

import pytest

from DIRAC import S_OK
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.JDL import jdlToBaseJobDescriptionModel
from DIRAC.Interfaces.API.Job import Job
from DIRAC.WorkloadManagementSystem.Utilities.JobModel import JobDescriptionModel


def test_jdlToBaseJobDescriptionModel_valid():
    """This test makes sure that a job object can be parsed by the jdlToBaseJobDescriptionModel method"""
    # Arrange
    with patch("DIRAC.Core.Base.API.getSites", return_value=S_OK(["LCG.IN2P3.fr"])):
        job = Job()
    job.setConfigArgs("configArgs")
    job.setCPUTime(3600)
    job.setExecutable("/bin/echo", arguments="arguments", logFile="logFile")
    job.setName("JobName")
    with patch(
        "DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations.getValue",
        return_value="DIRAC.WorkloadManagementSystem.Client.DownloadInputData",
    ):
        job.setInputDataPolicy("download")
    job.setInputSandbox(["inputfile.opts"])
    job.setOutputSandbox(["inputfile.opts"])
    job.setInputData(["/lhcb/production/DC04/v2/DST/00000742_00003493_10.dst"])
    job.setParameterSequence("IntSequence", [1, 2, 3])
    job.setParameterSequence("StrSequence", ["a", "b", "c"])
    job.setParameterSequence("FloatSequence", [1.0, 2.0, 3.0])

    job.setOutputData(["outputfile.root"], outputSE="IN2P3-disk", outputPath="/myjobs/1234")
    with patch("DIRAC.Interfaces.API.Job.getDIRACPlatforms", return_value=S_OK("x86_64-slc6-gcc49-opt")):
        job.setPlatform("x86_64-slc6-gcc49-opt")
    job.setPriority(10)

    job.setDestination("LCG.IN2P3.fr")
    job.setNumberOfProcessors(3)
    with patch("DIRAC.Interfaces.API.Job.getCESiteMapping", return_value=S_OK({"some.ce.IN2P3.fr": "LCG.IN2P3.fr"})):
        job.setDestinationCE("some.ce.IN2P3.fr")
    job.setType("Test")
    job.setTag(["WholeNode", "8GBMemory"])
    job.setJobGroup("1234abcd")
    job.setLogLevel("DEBUG")
    job.setConfigArgs("configArgs")
    job.setExecutionEnv({"INTVAR": 1, "STRVAR": "a"})
    job._addJDLParameter("ExtraInt", 1)
    job._addJDLParameter("ExtraFloat", 1.0)
    job._addJDLParameter("ExtraString", "test")
    # The 3 lines below are not a use case given that workflow parameters
    # must be strings, ints, floats or booleans
    # job._addJDLParameter("ExtraIntList", ";".join(["1", "2", "3"]))
    # job._addJDLParameter("ExtraFloatList", ";".join(["1.0", "2.0", "3.0"]))
    # job._addJDLParameter("ExtraStringList",";".join(["a", "b", "c"]))

    # We make sure that the job above is valid
    assert not job.errorDict

    # Act
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
