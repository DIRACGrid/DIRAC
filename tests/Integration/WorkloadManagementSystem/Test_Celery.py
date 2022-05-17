from DIRAC.Core.Celery.Task.OptimizeTask import optimize
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from tests.Integration.WorkloadManagementSystem.Test_JobDB import fakegetDIRACPlatform


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
]
"""


def test_celery():
    jobDB = JobDB()
    jobDB.getDIRACPlatform = fakegetDIRACPlatform

    res = jobDB.insertNewJobIntoDB(jdl, "owner", "/DN/OF/owner", "ownerGroup", "someSetup")
    assert res["OK"] is True, res["Message"]
    jobID = int(res["JobID"])
    res = jobDB.getJobAttribute(jobID, "Status")
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == JobStatus.RECEIVED

    jobState = JobState(jobID)
    optimize(jobState)
    optimize.delay(jobState)
