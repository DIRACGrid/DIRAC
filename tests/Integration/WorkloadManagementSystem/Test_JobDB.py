""" This tests only need the JobDB, and connects directly to it

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_JobDB.py
"""

# pylint: disable=wrong-import-position

from __future__ import print_function, absolute_import
from __future__ import division

from datetime import datetime, timedelta

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
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
]
"""


gLogger.setLevel('DEBUG')


def fakegetDIRACPlatform(OSList):
  return {'OK': True, 'Value': 'pippo'}


jobDB = JobDB()
jobDB.getDIRACPlatform = fakegetDIRACPlatform


def test_insertAndRemoveJobIntoDB():

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID = res['JobID']
  res = jobDB.getJobAttribute(jobID, 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == 'Received'
  res = jobDB.getJobAttribute(jobID, 'MinorStatus')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == 'Job accepted'
  res = jobDB.getJobOptParameters(jobID)
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {}

  res = jobDB.selectJobs({})
  assert res['OK'] is True, res['Message']
  jobs = res['Value']
  for job in jobs:
    res = jobDB.removeJobFromDB(job)
    assert res['OK'] is True, res['Message']


def test_rescheduleJob():

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID = res['JobID']

  res = jobDB.rescheduleJob(jobID)
  assert res['OK'] is True, res['Message']

  res = jobDB.getJobAttribute(jobID, 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == 'Received'
  res = jobDB.getJobAttribute(jobID, 'MinorStatus')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == 'Job Rescheduled'

  res = jobDB.selectJobs({})
  assert res['OK'] is True, res['Message']
  jobs = res['Value']
  for job in jobs:
    res = jobDB.removeJobFromDB(job)
    assert res['OK'] is True, res['Message']


def test_getCounters():

  res = jobDB.getCounters('Jobs', ['Status', 'MinorStatus'], {}, '2007-04-22 00:00:00')
  assert res['OK'] is True, res['Message']


def test_heartBeatLogging():

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID = res['JobID']

  res = jobDB.setJobStatus(jobID, status='Running')
  assert res['OK'] is True, res['Message']
  res = jobDB.setHeartBeatData(jobID, dynamicDataDict={'CPU': 2345})
  assert res['OK'] is True, res['Message']
  res = jobDB.setHeartBeatData(jobID, dynamicDataDict={'Memory': 5555})
  assert res['OK'] is True, res['Message']
  res = jobDB.getHeartBeatData(jobID)
  assert res['OK'] is True, res['Message']
  assert len(res['Value']) == 2, str(res)

  for name, value, _hbt in res['Value']:
    if name == 'Memory':
      assert value == '5555.0'
    elif name == 'CPU':
      assert value == '2345.0'
    else:
      assert False, 'Unknown entry: %s: %s' % (name, value)

  res = jobDB.setJobStatus(jobID, status='Done')
  assert res['OK'] is True, res['Message']

  tomorrow = datetime.today() + timedelta(1)
  delTime = datetime.strftime(tomorrow, '%Y-%m-%d')
  res = jobDB.removeInfoFromHeartBeatLogging(status='Done', delTime=delTime, maxLines=100)
  assert res['OK'] is True, res['Message']

  res = jobDB.getHeartBeatData(jobID)
  assert res['OK'] is True, res['Message']
  assert not res['Value'], str(res)

  res = jobDB.selectJobs({})
  assert res['OK'] is True, res['Message']
  jobs = res['Value']
  for job in jobs:
    res = jobDB.removeJobFromDB(job)
    assert res['OK'] is True, res['Message']


def test_jobParameters():
  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID = res['JobID']

  res = jobDB.getJobParameters(jobID)
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {}, res['Value']

  res = jobDB.getJobParameters([jobID])
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {}, res['Value']

  res = jobDB.getJobParameters(jobID, 'not')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {}, res['Value']

  res = jobDB.selectJobs({})
  assert res['OK'] is True, res['Message']
  jobs = res['Value']
  for job in jobs:
    res = jobDB.removeJobFromDB(job)
    assert res['OK'] is True, res['Message']
