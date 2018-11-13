""" This tests only need the JobDB, and connects directly to it

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_JobDB.py
"""

# pylint: disable=wrong-import-position

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

jdl = """
[
    Origin = "DIRAC";
    Executable = "$DIRACROOT/scripts/dirac-jobexec";
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


gLogger.setLevel('DEBUG')


def fakegetDIRACPlatform(OSList):
  return {'OK': True, 'Value': 'pippo'}

jobDB = JobDB()
jobDB.getDIRACPlatform = fakegetDIRACPlatform


def test_insertAndRemoveJobIntoDB():

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True
  jobID = res['JobID']
  res = jobDB.getJobAttribute(jobID, 'Status')
  assert res['OK'] is True
  assert res['Value'] == 'Received'
  res = jobDB.getJobAttribute(jobID, 'MinorStatus')
  assert res['OK'] is True
  assert res['Value'] == 'Job accepted'
  res = jobDB.getJobOptParameters(jobID)
  assert res['OK'] is True
  assert res['Value'] == {}

  res = jobDB.selectJobs({})
  assert res['OK'] is True
  jobs = res['Value']
  for job in jobs:
    res = jobDB.removeJobFromDB(job)
    assert res['OK'] is True


def test_rescheduleJob():

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True
  jobID = res['JobID']

  res = jobDB.rescheduleJob(jobID)
  assert res['OK'] is True

  res = jobDB.getJobAttribute(jobID, 'Status')
  assert res['OK'] is True
  assert res['Value'] == 'Received'
  res = jobDB.getJobAttribute(jobID, 'MinorStatus')
  assert res['OK'] is True
  assert res['Value'] == 'Job Rescheduled'


def test_getCounters():

  res = jobDB.getCounters('Jobs', ['Status', 'MinorStatus'], {}, '2007-04-22 00:00:00')
  assert res['OK'] is True
