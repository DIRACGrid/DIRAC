""" This tests only need the JobDB, and connects directly to it
"""

# pylint: disable=invalid-name,wrong-import-position

import unittest

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


class JobDBTestCase(unittest.TestCase):
  """ Base class for the JobDB test cases
  """

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.jobDB = JobDB()

  def tearDown(self):
    result = self.jobDB.selectJobs({})
    self.assertTrue(result['OK'], 'Status after selectJobs')
    jobs = result['Value']
    for job in jobs:
      result = self.jobDB.removeJobFromDB(job)
      self.assertTrue(result['OK'])


class JobSubmissionCase(JobDBTestCase):
  """  TestJobDB represents a test suite for the JobDB database front-end
  """

  def test_insertAndRemoveJobIntoDB(self):

    res = self.jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
    self.assertTrue(res['OK'])
    jobID = res['JobID']
    res = self.jobDB.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Status'], 'Received')
    self.assertEqual(res['Value']['MinorStatus'], 'Job accepted')
    res = self.jobDB.setJobStatus(jobID, 'Not received', 'Job waiting')
    self.assertTrue(res['OK'])
    res = self.jobDB.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Status'], 'Not received')
    self.assertEqual(res['Value']['MinorStatus'], 'Job waiting')
    res = self.jobDB.getJobOptParameters(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {})


class JobRescheduleCase(JobDBTestCase):

  def test_rescheduleJob(self):

    res = self.jobDB.insertNewJobIntoDB(jdl, 'owner', '/DN/OF/owner', 'ownerGroup', 'someSetup')
    self.assertTrue(res['OK'])
    jobID = res['JobID']

    result = self.jobDB.rescheduleJob(jobID)
    self.assertTrue(result['OK'])

    res = self.jobDB.getJobAttribute(jobID, 'Status')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'Received')
    result = self.jobDB.getJobAttribute(jobID, 'MinorStatus')
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'Job Rescheduled')


class CountJobsCase(JobDBTestCase):

  def test_getCounters(self):

    result = self.jobDB.getCounters('Jobs', ['Status', 'MinorStatus'], {}, '2007-04-22 00:00:00')
    self.assertTrue(result['OK'], 'Status after getCounters')


if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobSubmissionCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobRescheduleCase))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CountJobsCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
