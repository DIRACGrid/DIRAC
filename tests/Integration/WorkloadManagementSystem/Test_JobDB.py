""" This tests only need the JobDB, and connects directly to it

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_JobDB.py
"""

# pylint: disable=wrong-import-position, missing-docstring

from datetime import datetime, timedelta
from io import StringIO
from unittest.mock import MagicMock, patch
import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.JDL import MANDATORY_FIELDS, dumpJobDescriptionModelAsJDL, jdlToBaseJobDescriptionModel
from DIRAC.WorkloadManagementSystem.Utilities.JobModel import JobDescriptionModel
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus

from DIRAC.tests.Utilities.WMS import helloWorldJob

# sut
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


gLogger.setLevel("DEBUG")


@pytest.fixture(name="jobDB")
def fixturejobDB():
    jobDB = JobDB()
    jobDB.getDIRACPlatform = MagicMock(return_value=S_OK("pipoo"))

    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.getVOForGroup", MagicMock(return_value="vo")):
        yield jobDB

    # remove the job entries
    res = jobDB.selectJobs({})
    assert res["OK"], res["Message"]
    jobs = res["Value"]
    for job in jobs:
        res = jobDB.removeJobFromDB(job)
        assert res["OK"], res["Message"]


@pytest.fixture(name="jdl")
def fixture_jdl():
    job = helloWorldJob()

    xml = job._toXML()
    yield f"[{job._toJDL(jobDescriptionObject=StringIO(xml))}]"


@pytest.fixture(name="validatedJDL")
def fixture_validatedJDL(jdl: str):
    res = jdlToBaseJobDescriptionModel(jdl)
    assert res["OK"], res["Message"]
    baseJobDescriptionModel = res["Value"]
    job = JobDescriptionModel(
        **baseJobDescriptionModel.dict(exclude_none=True),
        diracSetup="test",
        owner="Unknown",
        ownerDN="Unknown",
        ownerGroup="Unknown",
        vo="vo",
    )

    yield dumpJobDescriptionModelAsJDL(job).asJDL()


def test_insertAndRemoveJobIntoDB(jobDB: JobDB, validatedJDL: str):
    # Act
    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"] is True, res["Message"]
    jobID = int(res["JobID"])
    res = jobDB.getJobAttribute(jobID, "Status")
    assert res["OK"], res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobAttribute(jobID, "MinorStatus")
    assert res["OK"], res["Message"]
    assert res["Value"] == "Job accepted"
    res = jobDB.getJobAttributes(jobID, ["Status", "MinorStatus"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {"Status": JobStatus.RECEIVED, "MinorStatus": "Job accepted"}
    res = jobDB.getJobsAttributes(jobID, ["Status", "MinorStatus"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID: {"Status": JobStatus.RECEIVED, "MinorStatus": "Job accepted"}}
    res = jobDB.getJobOptParameters(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"] == {}
    res = jobDB.removeJobFromDB(jobID)
    assert res["OK"], res["Message"]


def test_getJobsAttributes_multipleJobs(jobDB: JobDB, validatedJDL: str):
    """Test of the getJobJDL method with the original parameter set to True"""

    # Arrange
    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"], res["Message"]
    jobID_1 = int(res["JobID"])

    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"], res["Message"]
    jobID_2 = int(res["JobID"])

    # Act
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status", "MinorStatus"])

    # Assert
    assert res["OK"], res["Message"]
    assert res["Value"] == {
        jobID_1: {"Status": JobStatus.RECEIVED, "MinorStatus": "Job accepted"},
        jobID_2: {"Status": JobStatus.RECEIVED, "MinorStatus": "Job accepted"},
    }


@pytest.mark.parametrize(
    "originalJDL",
    [
        "jdl",
        "validatedJDL",
    ],
)
def test_rescheduleJob(jobDB: JobDB, originalJDL: str, validatedJDL: str, request):
    """Test that rescheduling a job works when the job has been parsed by Pydantic or not"""

    # Arrange
    originalJDL = request.getfixturevalue(originalJDL)
    res = jobDB.insertNewJobIntoDB(originalJDL)
    assert res["OK"] is True, res["Message"]
    jobID = res["JobID"]

    # Act
    res = jobDB.rescheduleJob(jobID)

    # Assert
    assert res["OK"], res["Message"]

    res = jobDB.getJobJDL(jobID, original=True)
    assert res["OK"], res["Message"]
    assert MANDATORY_FIELDS.issubset(ClassAd(res["Value"]).getAttributes())

    res = jobDB.getJobJDL(jobID)
    assert res["OK"], res["Message"]
    assert MANDATORY_FIELDS.issubset(ClassAd(res["Value"]).getAttributes())
    assert ClassAd(res["Value"]).lookupAttribute("JobID")

    res = jobDB.getJobAttribute(jobID, "Status")
    assert res["OK"], res["Message"]
    assert res["Value"] == JobStatus.RECEIVED

    res = jobDB.getJobAttribute(jobID, "MinorStatus")
    assert res["OK"], res["Message"]
    assert res["Value"] == JobMinorStatus.RESCHEDULED


def test_getCounters(jobDB):
    res = jobDB.getCounters("Jobs", ["Status", "MinorStatus"], {}, "2007-04-22 00:00:00")
    assert res["OK"], res["Message"]


def test_heartBeatLogging(jobDB: JobDB, validatedJDL: str):
    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"] is True, res["Message"]
    jobID = res["JobID"]

    res = jobDB.setJobStatus(jobID, status=JobStatus.CHECKING)
    assert res["OK"], res["Message"]
    res = jobDB.setJobStatus(jobID, status=JobStatus.WAITING)
    assert res["OK"], res["Message"]
    res = jobDB.setJobStatus(jobID, status=JobStatus.MATCHED)
    assert res["OK"], res["Message"]
    res = jobDB.setJobStatus(jobID, status=JobStatus.RUNNING)
    assert res["OK"], res["Message"]
    res = jobDB.setHeartBeatData(jobID, dynamicDataDict={"CPU": 2345})
    assert res["OK"], res["Message"]
    res = jobDB.setHeartBeatData(jobID, dynamicDataDict={"Memory": 5555})
    assert res["OK"], res["Message"]
    res = jobDB.getHeartBeatData(jobID)
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 2, str(res)

    for name, value, _hbt in res["Value"]:
        if name == "Memory":
            assert value == "5555.0"
        elif name == "CPU":
            assert value == "2345.0"
        else:
            assert False, f"Unknown entry: {name}: {value}"

    res = jobDB.setJobStatus(jobID, status=JobStatus.DONE)
    assert res["OK"], res["Message"]

    tomorrow = datetime.today() + timedelta(1)
    delTime = datetime.strftime(tomorrow, "%Y-%m-%d")
    res = jobDB.removeInfoFromHeartBeatLogging(status=JobStatus.DONE, delTime=delTime, maxLines=100)
    assert res["OK"], res["Message"]

    res = jobDB.getHeartBeatData(jobID)
    assert res["OK"], res["Message"]
    assert not res["Value"], str(res)


def test_jobParameters(jobDB: JobDB, validatedJDL: str):
    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"] is True, res["Message"]
    jobID = res["JobID"]

    res = jobDB.getJobParameters(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"] == {}, res["Value"]

    res = jobDB.getJobParameters([jobID])
    assert res["OK"], res["Message"]
    assert res["Value"] == {}, res["Value"]

    res = jobDB.getJobParameters(jobID, "not")
    assert res["OK"], res["Message"]
    assert res["Value"] == {}, res["Value"]


def test_setJobsMajorStatus(jobDB: JobDB, validatedJDL: str):
    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"] is True, res["Message"]
    jobID_1 = res["JobID"]
    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"] is True, res["Message"]
    jobID_2 = res["JobID"]

    res = jobDB.getJobAttribute(jobID_1, "Status")
    assert res["OK"], res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobAttribute(jobID_2, "Status")
    assert res["OK"], res["Message"]
    assert res["Value"] == JobStatus.RECEIVED

    res = jobDB.setJobsMajorStatus([jobID_1, jobID_2], JobStatus.CHECKING)
    assert res["OK"], res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.CHECKING}, jobID_2: {"Status": JobStatus.CHECKING}}

    res = jobDB.setJobsMajorStatus([jobID_1, jobID_2], JobStatus.RUNNING)
    assert res["OK"], res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.CHECKING}, jobID_2: {"Status": JobStatus.CHECKING}}

    res = jobDB.setJobsMajorStatus([jobID_1], JobStatus.WAITING)
    assert res["OK"], res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.WAITING}, jobID_2: {"Status": JobStatus.CHECKING}}

    res = jobDB.setJobsMajorStatus([jobID_1], JobStatus.KILLED)
    assert res["OK"], res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.WAITING}, jobID_2: {"Status": JobStatus.CHECKING}}

    res = jobDB.setJobsMajorStatus([jobID_1], JobStatus.KILLED, force=True)
    assert res["OK"], res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.KILLED}, jobID_2: {"Status": JobStatus.CHECKING}}


def test_attributes(jobDB: JobDB, validatedJDL: str):
    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"] is True, res["Message"]
    jobID_1 = res["JobID"]
    res = jobDB.insertNewJobIntoDB(validatedJDL)
    assert res["OK"] is True, res["Message"]
    jobID_2 = res["JobID"]

    res = jobDB.getJobAttribute(jobID_1, "Status")
    assert res["OK"], res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobAttribute(jobID_2, "Status")
    assert res["OK"], res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.RECEIVED}, jobID_2: {"Status": JobStatus.RECEIVED}}

    res = jobDB.setJobAttributes(jobID_1, ["ApplicationStatus"], ["ApplicationStatus_1"], True)
    assert res["OK"], res["Message"]
    res = jobDB.getJobAttribute(jobID_1, "ApplicationStatus")
    assert res["OK"], res["Message"]
    assert res["Value"] == "ApplicationStatus_1"

    res = jobDB.setJobAttributes(jobID_1, ["ApplicationStatus"], ["ApplicationStatus_1_2"], True)
    assert res["OK"], res["Message"]
    res = jobDB.getJobAttribute(jobID_1, "ApplicationStatus")
    assert res["OK"], res["Message"]
    assert res["Value"] == "ApplicationStatus_1_2"

    res = jobDB.setJobAttributes(jobID_1, ["JobName", "Site"], ["JobName_1", "DIRAC.Client.org"], True)
    assert res["OK"], res["Message"]
    res = jobDB.getJobAttribute(jobID_1, "Site")
    assert res["OK"], res["Message"]
    assert res["Value"] == "DIRAC.Client.org"

    res = jobDB.setJobAttributes(jobID_1, ["Status"], [JobStatus.CHECKING], True)
    assert res["OK"], res["Message"]
    res = jobDB.setJobAttributes(jobID_1, ["Status"], [JobStatus.WAITING], True)
    assert res["OK"], res["Message"]
    res = jobDB.getJobAttribute(jobID_1, "Status")
    assert res["OK"], res["Message"]
    assert res["Value"] == JobStatus.WAITING

    res = jobDB.setJobAttributes(jobID_1, ["Status", "MinorStatus"], [JobStatus.MATCHED, "minor"], True)
    assert res["OK"], res["Message"]
    res = jobDB.getJobAttributes(jobID_1, ["Status", "MinorStatus"])
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.MATCHED
    assert res["Value"]["MinorStatus"] == "minor"
    res = jobDB.getJobAttributes(jobID_2, ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.RECEIVED
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.MATCHED}, jobID_2: {"Status": JobStatus.RECEIVED}}

    res = jobDB.setJobAttributes(jobID_2, ["Status"], [JobStatus.CHECKING], True)
    assert res["OK"], res["Message"]
    res = jobDB.setJobAttributes(jobID_2, ["Status"], [JobStatus.WAITING], True)
    assert res["OK"], res["Message"]
    res = jobDB.setJobAttributes(jobID_2, ["Status"], [JobStatus.MATCHED], True)
    assert res["OK"], res["Message"]

    res = jobDB.setJobAttributes([jobID_1, jobID_2], ["Status", "MinorStatus"], [JobStatus.RUNNING, "minor_2"], True)
    assert res["OK"], res["Message"]
    res = jobDB.getJobAttributes(jobID_1, ["Status", "MinorStatus"])
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.RUNNING
    assert res["Value"]["MinorStatus"] == "minor_2"
    res = jobDB.getJobAttributes(jobID_2, ["Status", "MinorStatus"])
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.RUNNING
    assert res["Value"]["MinorStatus"] == "minor_2"

    jobDB.setJobAttributes(jobID_1, ["Status"], [JobStatus.DONE], True)
    jobDB.setJobAttributes([jobID_1, jobID_2], ["Status", "MinorStatus"], [JobStatus.COMPLETED, "minor_3"], True)
    res = jobDB.getJobAttributes(jobID_1, ["Status", "MinorStatus"])
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.DONE
    assert res["Value"]["MinorStatus"] == "minor_3"
    res = jobDB.getJobAttributes(jobID_2, ["Status", "MinorStatus"])
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.RUNNING
    assert res["Value"]["MinorStatus"] == "minor_3"
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.DONE}, jobID_2: {"Status": JobStatus.RUNNING}}
