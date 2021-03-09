""" This tests only need the JobDB, and connects directly to it

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_JobDB.py
"""

# pylint: disable=wrong-import-position

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from datetime import datetime, timedelta

import pytest

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Client import JobStatus

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
]
"""


gLogger.setLevel('DEBUG')


# fixture for teardown
@pytest.fixture
def putAndDelete():

  yield putAndDelete
  # from here on is teardown

  # remove the job entries
  res = jobDB.selectJobs({})
  assert res['OK'] is True, res['Message']
  jobs = res['Value']
  for job in jobs:
    res = jobDB.removeJobFromDB(job)
    assert res['OK'] is True, res['Message']


def fakegetDIRACPlatform(OSList):
  return {'OK': True, 'Value': 'pippo'}


jobDB = JobDB()
jobDB.getDIRACPlatform = fakegetDIRACPlatform


# #### real tests #

def test_insertAndRemoveJobIntoDB(putAndDelete):

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID = int(res['JobID'])
  res = jobDB.getJobAttribute(jobID, 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == JobStatus.RECEIVED
  res = jobDB.getJobAttribute(jobID, 'MinorStatus')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == 'Job accepted'
  res = jobDB.getJobAttributes(jobID, ['Status', 'MinorStatus'])
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {'Status': JobStatus.RECEIVED, 'MinorStatus': 'Job accepted'}
  res = jobDB.getJobsAttributes(jobID, ['Status', 'MinorStatus'])
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {jobID: {'Status': JobStatus.RECEIVED, 'MinorStatus': 'Job accepted'}}
  res = jobDB.getJobOptParameters(jobID)
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {}

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID_2 = int(res['JobID'])

  res = jobDB.getJobsAttributes([jobID, jobID_2], ['Status', 'MinorStatus'])
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {
      jobID: {'Status': JobStatus.RECEIVED, 'MinorStatus': 'Job accepted'},
      jobID_2: {'Status': JobStatus.RECEIVED, 'MinorStatus': 'Job accepted'}}

  res = jobDB.selectJobs({})
  assert res['OK'] is True, res['Message']
  jobs = res['Value']
  for job in jobs:
    res = jobDB.removeJobFromDB(job)
    assert res['OK'] is True, res['Message']


def test_rescheduleJob(putAndDelete):

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID = res['JobID']

  res = jobDB.getJobAttribute(jobID, 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == JobStatus.RECEIVED

  res = jobDB.rescheduleJob(jobID)
  assert res['OK'] is True, res['Message']

  res = jobDB.getJobAttribute(jobID, 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == JobStatus.RECEIVED
  res = jobDB.getJobAttribute(jobID, 'MinorStatus')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == 'Job Rescheduled'


def test_getCounters():

  res = jobDB.getCounters('Jobs', ['Status', 'MinorStatus'], {}, '2007-04-22 00:00:00')
  assert res['OK'] is True, res['Message']


def test_heartBeatLogging(putAndDelete):

  res = jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID = res['JobID']

  res = jobDB.setJobStatus(jobID, status=JobStatus.RUNNING)
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

  res = jobDB.setJobStatus(jobID, status=JobStatus.DONE)
  assert res['OK'] is True, res['Message']

  tomorrow = datetime.today() + timedelta(1)
  delTime = datetime.strftime(tomorrow, '%Y-%m-%d')
  res = jobDB.removeInfoFromHeartBeatLogging(status=JobStatus.DONE, delTime=delTime, maxLines=100)
  assert res['OK'] is True, res['Message']

  res = jobDB.getHeartBeatData(jobID)
  assert res['OK'] is True, res['Message']
  assert not res['Value'], str(res)


def test_jobParameters(putAndDelete):
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


def test_attributes(putAndDelete):

  res = jobDB.insertNewJobIntoDB(jdl, 'owner_1', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID_1 = res['JobID']
  res = jobDB.insertNewJobIntoDB(jdl, 'owner_2', '/DN/OF/owner', 'ownerGroup', 'someSetup')
  assert res['OK'] is True, res['Message']
  jobID_2 = res['JobID']

  res = jobDB.getJobAttribute(jobID_1, 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == JobStatus.RECEIVED
  res = jobDB.getJobAttribute(jobID_2, 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == JobStatus.RECEIVED
  res = jobDB.getJobsAttribute([jobID_1, jobID_2], 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == [JobStatus.RECEIVED, JobStatus.RECEIVED]

  res = jobDB.setJobAttributes(jobID_1, ['Status'], ['Waiting'], True)
  assert res['OK'] is True, res['Message']
  res = jobDB.getJobAttribute(jobID_1, 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == 'Waiting'

  res = jobDB.setJobAttributes(jobID_1, ['Status', 'MinorStatus'], [JobStatus.MATCHED, 'minor'], True)
  assert res['OK'] is True, res['Message']
  res = jobDB.getJobAttributes(jobID_1, ['Status', 'MinorStatus'])
  assert res['OK'] is True, res['Message']
  assert res['Value']['Status'] == JobStatus.MATCHED
  assert res['Value']['MinorStatus'] == 'minor'
  res = jobDB.getJobAttributes(jobID_2, ['Status'])
  assert res['OK'] is True, res['Message']
  assert res['Value']['Status'] == JobStatus.RECEIVED
  res = jobDB.getJobsAttribute([jobID_1, jobID_2], 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == [JobStatus.MATCHED, JobStatus.RECEIVED]

  res = jobDB.setJobAttributes([jobID_1, jobID_2], ['Status', 'MinorStatus'], [JobStatus.RUNNING, 'minor_2'], True)
  assert res['OK'] is True, res['Message']
  res = jobDB.getJobAttributes(jobID_1, ['Status', 'MinorStatus'])
  assert res['OK'] is True, res['Message']
  assert res['Value']['Status'] == JobStatus.RUNNING
  assert res['Value']['MinorStatus'] == 'minor_2'
  res = jobDB.getJobAttributes(jobID_2, ['Status', 'MinorStatus'])
  assert res['OK'] is True, res['Message']
  assert res['Value']['Status'] == JobStatus.RUNNING
  assert res['Value']['MinorStatus'] == 'minor_2'

  jobDB.setJobAttributes(jobID_1, ['Status'], [JobStatus.DONE], True)
  jobDB.setJobAttributes([jobID_1, jobID_2], ['Status', 'MinorStatus'], [JobStatus.COMPLETED, 'minor_3'], True)
  res = jobDB.getJobAttributes(jobID_1, ['Status', 'MinorStatus'])
  assert res['OK'] is True, res['Message']
  assert res['Value']['Status'] == JobStatus.DONE
  assert res['Value']['MinorStatus'] == 'minor_2'
  res = jobDB.getJobAttributes(jobID_2, ['Status', 'MinorStatus'])
  assert res['OK'] is True, res['Message']
  assert res['Value']['Status'] == JobStatus.COMPLETED
  assert res['Value']['MinorStatus'] == 'minor_3'
  res = jobDB.getJobsAttribute([jobID_1, jobID_2], 'Status')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == [JobStatus.DONE, JobStatus.COMPLETED]
