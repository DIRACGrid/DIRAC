""" This tests only need the JobDB, and connects directly to it

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_JobDB.py
"""

# pylint: disable=wrong-import-position, missing-docstring

from datetime import datetime, timedelta
from mock import MagicMock, patch
import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger, S_OK
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus

# sut
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

jdl = """[
    Executable = "dirac-jobexec";
    StdError = "std.err";
    LogLevel = "info";
    Site = "ANY";
    JobName = "helloWorld";
    Priority = "1";
    InputSandbox =
        {
            "../../Integration/WorkloadManagementSystem/exe-script.py",
            "exe-script.py",
            "/tmp/tmpMQEink/jobDescription.xml",
            "SB:FedericoSandboxSE|/SandBox/f/fstagni.lhcb_user/0c2/9f5/0c29f53a47d051742346b744c793d4d0.tar.bz2"
        };
    Arguments = "jobDescription.xml -o LogLevel=info";
    JobGroup = "lhcb";
    OutputSandbox =
        {
            "helloWorld.log",
            "std.err",
            "std.out"
        };
    StdOutput = "std.out";
    InputData = "";
    JobType = "User";
]"""

originalJDL = """[
    Executable = "dirac-jobexec";
    StdError = "std.err";
    LogLevel = "info";
    Site = "ANY";
    JobName = "helloWorld";
    Priority = "1";
    InputSandbox =
        {
            "../../Integration/WorkloadManagementSystem/exe-script.py",
            "exe-script.py",
            "/tmp/tmpMQEink/jobDescription.xml",
            "SB:FedericoSandboxSE|/SandBox/f/fstagni.lhcb_user/0c2/9f5/0c29f53a47d051742346b744c793d4d0.tar.bz2"
        };
    Arguments = "jobDescription.xml -o LogLevel=info";
    JobGroup = "lhcb";
    OutputSandbox =
        {
            "helloWorld.log",
            "std.err",
            "std.out"
        };
    StdOutput = "std.out";
    InputData = "";
    JobType = "User";
]"""


def getExpectedJDL(jobID):
    return f"""[
    Arguments = "jobDescription.xml -o LogLevel=info";
    CPUTime = 86400;
    DIRACSetup = "someSetup";
    Executable = "dirac-jobexec";
    InputData = "";
    InputSandbox =
        {{
            "../../Integration/WorkloadManagementSystem/exe-script.py",
            "exe-script.py",
            "/tmp/tmpMQEink/jobDescription.xml",
            "SB:FedericoSandboxSE|/SandBox/f/fstagni.lhcb_user/0c2/9f5/0c29f53a47d051742346b744c793d4d0.tar.bz2"
        }};
    JobGroup = "lhcb";
    JobID = {jobID};
    JobName = "helloWorld";
    JobRequirements =
        [
            CPUTime = 86400;
            OwnerDN = "/DN/OF/owner";
            OwnerGroup = "ownerGroup";
            Setup = "someSetup";
            UserPriority = 1;
            VirtualOrganization = "vo";
        ];
    JobType = "User";
    LogLevel = "info";
    OutputSandbox =
        {{
            "helloWorld.log",
            "std.err",
            "std.out"
        }};
    Owner = "owner";
    OwnerDN = "/DN/OF/owner";
    OwnerGroup = "ownerGroup";
    OwnerName = "owner";
    Priority = 1;
    Site = "ANY";
    StdError = "std.err";
    StdOutput = "std.out";
    VirtualOrganization = "vo";
]"""


gLogger.setLevel("DEBUG")


@pytest.fixture(name="jobDB")
def fixturejobDB():
    jobDB = JobDB()
    jobDB.getDIRACPlatform = MagicMock(return_value=S_OK("pipoo"))

    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.getVOForGroup", MagicMock(return_value="vo")):
        yield jobDB

    # remove the job entries
    res = jobDB.selectJobs({})
    assert res["OK"] is True, res["Message"]
    jobs = res["Value"]
    for job in jobs:
        res = jobDB.removeJobFromDB(job)
        assert res["OK"] is True, res["Message"]


def test_insertAndRemoveJobIntoDB(jobDB):

    res = jobDB.insertNewJobIntoDB(jdl, "owner", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID = int(res["JobID"])
    res = jobDB.getJobAttribute(jobID, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobAttribute(jobID, "MinorStatus")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == "Job accepted"
    res = jobDB.getJobAttributes(jobID, ["Status", "MinorStatus"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {"Status": JobStatus.RECEIVED, "MinorStatus": "Job accepted"}
    res = jobDB.getJobsAttributes(jobID, ["Status", "MinorStatus"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID: {"Status": JobStatus.RECEIVED, "MinorStatus": "Job accepted"}}
    res = jobDB.getJobOptParameters(jobID)
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {}

    res = jobDB.getJobJDL(jobID, original=True)
    print(" ".join(res["Value"].split()))
    assert res["OK"] is True, res["Message"]
    assert " ".join(res["Value"].split()) == " ".join(originalJDL.split())

    res = jobDB.getJobJDL(jobID)
    print(" ".join(res["Value"].split()))
    assert res["OK"] is True, res["Message"]
    print(res["Value"])
    assert " ".join(res["Value"].split()) == " ".join(getExpectedJDL(jobID).split())

    res = jobDB.insertNewJobIntoDB(jdl, "owner", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID_2 = int(res["JobID"])

    # Check that the original jdl is insensitive to the jobID
    assert jobDB.getJobJDL(jobID, original=True) == jobDB.getJobJDL(jobID_2, original=True)

    res = jobDB.getJobsAttributes([jobID, jobID_2], ["Status", "MinorStatus"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {
        jobID: {"Status": JobStatus.RECEIVED, "MinorStatus": "Job accepted"},
        jobID_2: {"Status": JobStatus.RECEIVED, "MinorStatus": "Job accepted"},
    }

    res = jobDB.selectJobs({})
    assert res["OK"] is True, res["Message"]
    jobs = res["Value"]
    for job in jobs:
        res = jobDB.removeJobFromDB(job)
        assert res["OK"] is True, res["Message"]


def test_rescheduleJob(jobDB):

    res = jobDB.insertNewJobIntoDB(jdl, "owner", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID = res["JobID"]

    res = jobDB.getJobAttribute(jobID, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.RECEIVED

    res = jobDB.rescheduleJob(jobID)
    assert res["OK"] is True, res["Message"]

    res = jobDB.getJobAttribute(jobID, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobAttribute(jobID, "MinorStatus")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobMinorStatus.RESCHEDULED


def test_getCounters(jobDB):

    res = jobDB.getCounters("Jobs", ["Status", "MinorStatus"], {}, "2007-04-22 00:00:00")
    assert res["OK"] is True, res["Message"]


def test_heartBeatLogging(jobDB):

    res = jobDB.insertNewJobIntoDB(jdl, "owner", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID = res["JobID"]

    res = jobDB.setJobStatus(jobID, status=JobStatus.CHECKING)
    assert res["OK"] is True, res["Message"]
    res = jobDB.setJobStatus(jobID, status=JobStatus.WAITING)
    assert res["OK"] is True, res["Message"]
    res = jobDB.setJobStatus(jobID, status=JobStatus.MATCHED)
    assert res["OK"] is True, res["Message"]
    res = jobDB.setJobStatus(jobID, status=JobStatus.RUNNING)
    assert res["OK"] is True, res["Message"]
    res = jobDB.setHeartBeatData(jobID, dynamicDataDict={"CPU": 2345})
    assert res["OK"] is True, res["Message"]
    res = jobDB.setHeartBeatData(jobID, dynamicDataDict={"Memory": 5555})
    assert res["OK"] is True, res["Message"]
    res = jobDB.getHeartBeatData(jobID)
    assert res["OK"] is True, res["Message"]
    assert len(res["Value"]) == 2, str(res)

    for name, value, _hbt in res["Value"]:
        if name == "Memory":
            assert value == "5555.0"
        elif name == "CPU":
            assert value == "2345.0"
        else:
            assert False, "Unknown entry: %s: %s" % (name, value)

    res = jobDB.setJobStatus(jobID, status=JobStatus.DONE)
    assert res["OK"] is True, res["Message"]

    tomorrow = datetime.today() + timedelta(1)
    delTime = datetime.strftime(tomorrow, "%Y-%m-%d")
    res = jobDB.removeInfoFromHeartBeatLogging(status=JobStatus.DONE, delTime=delTime, maxLines=100)
    assert res["OK"] is True, res["Message"]

    res = jobDB.getHeartBeatData(jobID)
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], str(res)


def test_jobParameters(jobDB):
    res = jobDB.insertNewJobIntoDB(jdl, "owner", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID = res["JobID"]

    res = jobDB.getJobParameters(jobID)
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {}, res["Value"]

    res = jobDB.getJobParameters([jobID])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {}, res["Value"]

    res = jobDB.getJobParameters(jobID, "not")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {}, res["Value"]


def test_setJobsMajorStatus(jobDB):
    res = jobDB.insertNewJobIntoDB(jdl, "owner_1", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID_1 = res["JobID"]
    res = jobDB.insertNewJobIntoDB(jdl, "owner_2", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID_2 = res["JobID"]

    res = jobDB.getJobAttribute(jobID_1, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobAttribute(jobID_2, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.RECEIVED

    res = jobDB.setJobsMajorStatus([jobID_1, jobID_2], JobStatus.CHECKING)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.CHECKING}, jobID_2: {"Status": JobStatus.CHECKING}}

    res = jobDB.setJobsMajorStatus([jobID_1, jobID_2], JobStatus.RUNNING)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.CHECKING}, jobID_2: {"Status": JobStatus.CHECKING}}

    res = jobDB.setJobsMajorStatus([jobID_1], JobStatus.WAITING)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.WAITING}, jobID_2: {"Status": JobStatus.CHECKING}}

    res = jobDB.setJobsMajorStatus([jobID_1], JobStatus.KILLED)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.WAITING}, jobID_2: {"Status": JobStatus.CHECKING}}

    res = jobDB.setJobsMajorStatus([jobID_1], JobStatus.KILLED, force=True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.KILLED}, jobID_2: {"Status": JobStatus.CHECKING}}


def test_attributes(jobDB):

    res = jobDB.insertNewJobIntoDB(jdl, "owner_1", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID_1 = res["JobID"]
    res = jobDB.insertNewJobIntoDB(jdl, "owner_2", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID_2 = res["JobID"]

    res = jobDB.getJobAttribute(jobID_1, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobAttribute(jobID_2, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.RECEIVED
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.RECEIVED}, jobID_2: {"Status": JobStatus.RECEIVED}}

    res = jobDB.setJobAttributes(jobID_1, ["ApplicationStatus"], ["ApplicationStatus_1"], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobAttribute(jobID_1, "ApplicationStatus")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == "ApplicationStatus_1"

    res = jobDB.setJobAttributes(jobID_1, ["ApplicationStatus"], ["ApplicationStatus_1_2"], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobAttribute(jobID_1, "ApplicationStatus")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == "ApplicationStatus_1_2"

    res = jobDB.setJobAttributes(jobID_1, ["JobName", "Site"], ["JobName_1", "DIRAC.Client.org"], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobAttribute(jobID_1, "Site")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == "DIRAC.Client.org"

    res = jobDB.setJobAttributes(jobID_1, ["Status"], [JobStatus.CHECKING], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.setJobAttributes(jobID_1, ["Status"], [JobStatus.WAITING], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobAttribute(jobID_1, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.WAITING

    res = jobDB.setJobAttributes(jobID_1, ["Status", "MinorStatus"], [JobStatus.MATCHED, "minor"], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobAttributes(jobID_1, ["Status", "MinorStatus"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"]["Status"] == JobStatus.MATCHED
    assert res["Value"]["MinorStatus"] == "minor"
    res = jobDB.getJobAttributes(jobID_2, ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"]["Status"] == JobStatus.RECEIVED
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.MATCHED}, jobID_2: {"Status": JobStatus.RECEIVED}}

    res = jobDB.setJobAttributes(jobID_2, ["Status"], [JobStatus.CHECKING], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.setJobAttributes(jobID_2, ["Status"], [JobStatus.WAITING], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.setJobAttributes(jobID_2, ["Status"], [JobStatus.MATCHED], True)
    assert res["OK"] is True, res["Message"]

    res = jobDB.setJobAttributes([jobID_1, jobID_2], ["Status", "MinorStatus"], [JobStatus.RUNNING, "minor_2"], True)
    assert res["OK"] is True, res["Message"]
    res = jobDB.getJobAttributes(jobID_1, ["Status", "MinorStatus"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"]["Status"] == JobStatus.RUNNING
    assert res["Value"]["MinorStatus"] == "minor_2"
    res = jobDB.getJobAttributes(jobID_2, ["Status", "MinorStatus"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"]["Status"] == JobStatus.RUNNING
    assert res["Value"]["MinorStatus"] == "minor_2"

    jobDB.setJobAttributes(jobID_1, ["Status"], [JobStatus.DONE], True)
    jobDB.setJobAttributes([jobID_1, jobID_2], ["Status", "MinorStatus"], [JobStatus.COMPLETED, "minor_3"], True)
    res = jobDB.getJobAttributes(jobID_1, ["Status", "MinorStatus"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"]["Status"] == JobStatus.DONE
    assert res["Value"]["MinorStatus"] == "minor_3"
    res = jobDB.getJobAttributes(jobID_2, ["Status", "MinorStatus"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"]["Status"] == JobStatus.RUNNING
    assert res["Value"]["MinorStatus"] == "minor_3"
    res = jobDB.getJobsAttributes([jobID_1, jobID_2], ["Status"])
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == {jobID_1: {"Status": JobStatus.DONE}, jobID_2: {"Status": JobStatus.RUNNING}}
