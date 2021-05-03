""" This is a test of using WMSClient and several other functions in WMS

    In order to run this test we need the following DBs installed:
    - JobDB
    - JobLoggingDB
    - TaskQueueDB
    - SandboxMetadataDB

    And the following services should also be on:
    - OptimizationMind
    - JobManager
    - SandboxStore
    - JobMonitoring
    - JobStateUpdate
    - WMSAdministrator
    - Matcher

    A user proxy is also needed to submit,
    and the Framework/ProxyManager need to be running with a such user proxy already uploaded.

    Due to the nature of the DIRAC WMS, only a full chain test makes sense,
    and this also means that this test is not easy to set up.
"""


# pylint: disable=protected-access,wrong-import-position,invalid-name

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import unittest
import sys
import datetime
import time

# from mock import Mock

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all
from DIRAC.tests.Utilities.WMS import helloWorldJob, parametricJob, createFile

from DIRAC import gLogger
from DIRAC.Interfaces.API.Job import Job
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


class TestWMSTestCase(unittest.TestCase):

  def setUp(self):
    self.maxDiff = None

    gLogger.setLevel('DEBUG')

  def tearDown(self):
    """ use the JobCleaningAgent method to remove the jobs in status Deleted and Killed
    """
    jca = JobCleaningAgent('WorkloadManagement/JobCleaningAgent',
                           'WorkloadManagement/JobCleaningAgent')
    jca.initialize()
    res = jca.removeJobsByStatus({'Status': [JobStatus.KILLED, JobStatus.DELETED]})
    self.assertTrue(res['OK'], res.get('Message'))


class WMSChain(TestWMSTestCase):

  def test_FullChain(self):
    """ This test will

        - call all the WMSClient methods
          that will end up calling all the JobManager service methods
        - use the JobMonitoring to verify few properties
        - call the JobCleaningAgent to eliminate job entries from the DBs
    """
    wmsClient = WMSClient()
    jobMonitor = JobMonitoringClient()
    jobStateUpdate = JobStateUpdateClient()

    # create the job
    job = helloWorldJob()
    jobDescription = createFile(job)

    # submit the job
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertTrue(isinstance(res['Value'], int), msg="Got %s" % type(res['Value']))
    self.assertEqual(res['Value'], res['JobID'],
                     msg="Got %s, expected %s" % (str(res['Value']), res['JobID']))
    jobID = res['JobID']
    jobID = res['Value']

    # updating the status
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.RUNNING, 'Executing Minchiapp', 'source')
    self.assertTrue(res['OK'], res.get('Message'))

    # reset the job
    res = wmsClient.resetJob(jobID)
    self.assertTrue(res['OK'], res.get('Message'))

    # reschedule the job
    res = wmsClient.rescheduleJob(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], JobStatus.RECEIVED, msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobsMinorStatus([jobID])
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], {jobID: {'MinorStatus': 'Job Rescheduled', 'JobID': jobID}},
                     msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobsApplicationStatus([jobID])
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], {jobID: {'ApplicationStatus': 'Unknown', 'JobID': jobID}},
                     msg="Got %s" % str(res['Value']))

    # updating the status again
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.CHECKING, 'checking', 'source')
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.WAITING, 'waiting', 'source')
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.MATCHED, 'matched', 'source')
    self.assertTrue(res['OK'], res.get('Message'))

    # kill the job
    res = wmsClient.killJob(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], JobStatus.KILLED, msg="Got %s" % str(res['Value']))

    # delete the job - this will just set its status to "deleted"
    res = wmsClient.deleteJob(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], JobStatus.DELETED, msg="Got %s" % str(res['Value']))

  def test_ParametricChain(self):
    """ This test will submit a parametric job which should generate 3 actual jobs
    """
    wmsClient = WMSClient()
    jobStateUpdate = JobStateUpdateClient()
    jobMonitor = JobMonitoringClient()

    # create the job
    job = parametricJob()
    jobDescription = createFile(job)

    # submit the job
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    self.assertTrue(res['OK'], res.get('Message'))
    jobIDList = res['Value']
    self.assertEqual(len(jobIDList), 3, msg="Got %s" % str(jobIDList))

    res = jobMonitor.getJobsParameters(jobIDList, ['JobName'])
    self.assertTrue(res['OK'], res.get('Message'))
    jobNames = [res['Value'][jobID]['JobName'] for jobID in res['Value']]
    self.assertEqual(set(jobNames), set(['parametric_helloWorld_%s' % nJob for nJob in range(3)]))

    for jobID in jobIDList:
      res = jobStateUpdate.setJobStatus(jobID, JobStatus.CHECKING, 'checking', 'source')
      self.assertTrue(res['OK'], res.get('Message'))

    res = wmsClient.deleteJob(jobIDList)
    self.assertTrue(res['OK'], res.get('Message'))
    print(res)

    for jobID in jobIDList:
      res = jobMonitor.getJobStatus(jobID)
      self.assertTrue(res['OK'], res.get('Message'))
      self.assertEqual(res['Value'], JobStatus.DELETED, msg="Got %s" % str(res['Value']))


class JobMonitoring(TestWMSTestCase):

  def test_JobStateUpdateAndJobMonitoring(self):
    """ Verifying all JobStateUpdate and JobMonitoring functions
    """
    wmsClient = WMSClient()
    jobMonitor = JobMonitoringClient()
    jobStateUpdate = JobStateUpdateClient()

    # create a job and check stuff
    job = helloWorldJob()
    jobDescription = createFile(job)

    # submitting the job. Checking few stuff
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    self.assertTrue(res['OK'], res.get('Message'))
    jobID = int(res['Value'])
    # jobID = res['JobID']
    res = jobMonitor.getJobJDL(jobID, True)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobJDL(jobID, False)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobsParameters([jobID], [])
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], {}, msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobOwner(jobID)
    self.assertTrue(res['OK'], res.get('Message'))

    # Adding stuff
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.CHECKING, 'checking', 'source')
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.WAITING, 'waiting', 'source')
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.MATCHED, 'matched', 'source')
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.RUNNING, 'running', 'source')
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobParameters(jobID, [('par1', 'par1Value'), ('par2', 'par2Value')])
    time.sleep(5)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobApplicationStatus(jobID, 'app status', 'source')
    self.assertTrue(res['OK'], res.get('Message'))
#     res = jobStateUpdate.setJobFlag()
#     self.assertTrue(res['OK'], res.get('Message'))
#     res = jobStateUpdate.unsetJobFlag()
#     self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobSite(jobID, 'Site')
    self.assertTrue(res['OK'], res.get('Message'))

    # now checking few things
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], JobStatus.RUNNING, msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobParameter(jobID, 'par1')
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], {'par1': 'par1Value'}, msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobParameters(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], {jobID: {'par1': 'par1Value', 'par2': 'par2Value'}},
                     msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobParameters(jobID, 'par1')
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], {jobID: {'par1': 'par1Value'}},
                     msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobAttribute(jobID, 'Site')
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], 'Site', msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobAttributes(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value']['ApplicationStatus'], 'app status',
                     msg="Got %s" % str(res['Value']['ApplicationStatus']))
    self.assertEqual(res['Value']['JobName'], 'helloWorld',
                     msg="Got %s" % str(res['Value']['JobName']))
    res = jobMonitor.getJobSummary(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value']['ApplicationStatus'], 'app status',
                     msg="Got %s" % str(res['Value']['ApplicationStatus']))
    self.assertEqual(res['Value']['Status'], JobStatus.RUNNING,
                     msg="Got %s" % str(res['Value']['Status']))
    res = jobMonitor.getJobHeartBeatData(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], [], msg="Got %s" % str(res['Value']))
    res = jobMonitor.getInputData(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], [], msg="Got %s" % str(res['Value']))
    res = jobMonitor.getJobPrimarySummary(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getAtticJobParameters(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.DONE, 'MinorStatus', 'Unknown')
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobSummary(jobID)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value']['Status'], JobStatus.DONE, msg="Got %s" % str(res['Value']['Status']))
    self.assertEqual(res['Value']['MinorStatus'], 'MinorStatus',
                     msg="Got %s" % str(res['Value']['MinorStatus']))
    self.assertEqual(res['Value']['ApplicationStatus'], 'app status',
                     msg="Got %s" % str(res['Value']['ApplicationStatus']))
    res = jobStateUpdate.sendHeartBeat(jobID, {'bih': 'bih'}, {'boh': 'boh'})
    self.assertTrue(res['OK'], res.get('Message'))

    # delete the job - this will just set its status to "deleted"
    wmsClient.deleteJob(jobID)


#     # Adding a platform
#     self.getDIRACPlatformMock.return_value = {'OK': False}
#
#     job = helloWorldJob()
#     job.setPlatform( "x86_64-slc6" )
#
#     jobDescription = createFile( job )
#
#     job.setCPUTime( 17800 )
#     job.setBannedSites( ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
#                          'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'] )
#     res = WMSClient().submitJob( job._toJDL( xmlFile = jobDescription ) )
#     self.assertTrue(res['OK'], res.get('Message'))
#     self.assertEqual( type( res['Value'] ), int )


class JobMonitoringMore(TestWMSTestCase):

  def test_JobStateUpdateAndJobMonitoringMultuple(self):
    """ # Now, let's submit some jobs. Different sites, types, inputs
    """
    wmsClient = WMSClient()
    jobMonitor = JobMonitoringClient()
    jobStateUpdate = JobStateUpdateClient()

    jobIDs = []
    lfnss = [['/a/1.txt', '/a/2.txt'], ['/a/1.txt', '/a/3.txt', '/a/4.txt'], []]
    types = ['User', 'Test']
    for lfns in lfnss:
      for jobType in types:
        job = helloWorldJob()
        job.setDestination('DIRAC.Jenkins.ch')
        job.setInputData(lfns)
        job.setType(jobType)
        jobDescription = createFile(job)
        res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
        self.assertTrue(res['OK'], res.get('Message'))
        jobID = res['Value']
      jobIDs.append(jobID)

    res = jobMonitor.getSites()
    print(res)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertTrue(set(res['Value']) <= {'ANY', 'DIRAC.Jenkins.ch', 'Site'}, msg="Got %s" % res['Value'])
    res = jobMonitor.getJobTypes()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(sorted(res['Value']), sorted(types), msg="Got %s" % str(sorted(res['Value'])))
    res = jobMonitor.getApplicationStates()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], ['Unknown'], msg="Got %s" % str(res['Value']))

    res = jobMonitor.getOwners()
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getOwnerGroup()
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getProductionIds()
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobGroups()
    self.assertTrue(res['OK'], res.get('Message'))
    resJG_empty = res['Value']
    res = jobMonitor.getJobGroups(None, datetime.datetime.utcnow())
    self.assertTrue(res['OK'], res.get('Message'))
    resJG_olderThanNow = res['Value']
    self.assertEqual(resJG_empty, resJG_olderThanNow)
    res = jobMonitor.getJobGroups(None, datetime.datetime.utcnow() - datetime.timedelta(days=365))
    self.assertTrue(res['OK'], res.get('Message'))
    resJG_olderThanOneYear = res['Value']
    self.assertTrue(set(resJG_olderThanOneYear).issubset(set(resJG_olderThanNow)),
                    resJG_olderThanOneYear)
    res = jobMonitor.getStates()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertTrue(sorted(res['Value']) in [[JobStatus.RECEIVED], sorted([JobStatus.RECEIVED, JobStatus.WAITING])],
                    res['Value'])
    res = jobMonitor.getMinorStates()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertTrue(sorted(res['Value']) in [
        ['Job accepted'],
        sorted(['Job accepted', 'Job Rescheduled'])],
        res['Value'])
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobs()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertTrue(set([str(x) for x in jobIDs]) <= set(res['Value']),
                    res['Value'])
#     res = jobMonitor.getCounters(attrList)
#     self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobsSummary(jobIDs)
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobPageSummaryWeb({}, [], 0, 100)
    self.assertTrue(res['OK'], res.get('Message'))

    res = jobStateUpdate.setJobStatusBulk(
        jobID,
        {str(datetime.datetime.utcnow()): {
            'Status': JobStatus.MATCHED,
            'MinorStatus': 'MinorStatus',
            'ApplicationStatus': 'ApplicationStatus',
            'Source': 'Unknown'},
         str(datetime.datetime.utcnow() + datetime.timedelta(hours=1)): {
            'Status': JobStatus.RUNNING,
            'MinorStatus': 'MinorStatus',
            'ApplicationStatus': 'ApplicationStatus',
            'Source': 'Unknown'},
         str(datetime.datetime.utcnow() + datetime.timedelta(hours=2)): {
            'Status': 'Completed',
            'MinorStatus': 'MinorStatus',
            'ApplicationStatus': 'ApplicationStatus',
            'Source': 'Unknown'}}
    )
    self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobsParameter({jobID: ['Status', JobStatus.RUNNING]})
    self.assertTrue(res['OK'], res.get('Message'))

    res = jobMonitor.getJobSummary(int(jobID))
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value']['Status'], 'Completed')
    self.assertEqual(res['Value']['MinorStatus'], 'MinorStatus')

    # delete the jobs - this will just set its status to "deleted"
    wmsClient.deleteJob(jobIDs)


#   def test_submitFail( self ):
#
#     # Adding a platform that should not exist
#     job = helloWorldJob()
#     job.setPlatform( "notExistingPlatform" )
#     jobDescription = createFile( job )
#
#     res = WMSClient().submitJob( job._toJDL( xmlFile = jobDescription ) )
#     self.assertTrue(res['OK'], res.get('Message'))
#
#     WMSClient().deleteJob( res['Value'] )


class WMSAdministrator(TestWMSTestCase):
  """ testing WMSAdmin - for JobDB
  """

  def test_JobDBWMSAdmin(self):

    wmsAdministrator = WMSAdministratorClient()

    sitesList = ['My.Site.org', 'Your.Site.org']
    res = wmsAdministrator.setSiteMask(sitesList)
    self.assertTrue(res['OK'], res.get('Message'))
    res = wmsAdministrator.getSiteMask()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(sorted(res['Value']), sorted(sitesList), msg="Got %s" % str(sorted(res['Value'])))
    res = wmsAdministrator.banSite('My.Site.org', 'This is a comment')
    self.assertTrue(res['OK'], res.get('Message'))
    res = wmsAdministrator.getSiteMask()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(sorted(res['Value']), ['Your.Site.org'], msg="Got %s" % str(sorted(res['Value'])))
    res = wmsAdministrator.allowSite('My.Site.org', 'This is a comment')
    self.assertTrue(res['OK'], res.get('Message'))
    res = wmsAdministrator.getSiteMask()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(sorted(res['Value']), sorted(sitesList), msg="Got %s" % str(sorted(res['Value'])))

    res = wmsAdministrator.getSiteMaskLogging(sitesList)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value']['My.Site.org'][0][3], 'No comment',
                     msg="Got %s" % str(res['Value']['My.Site.org'][0][3]))
    res = wmsAdministrator.getSiteMaskSummary()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value']['My.Site.org'], 'Active', msg="Got %s" % res['Value']['My.Site.org'])

    res = wmsAdministrator.getSiteSummaryWeb({}, [], 0, 100)
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertTrue(res['Value']['TotalRecords'] in [0, 1, 2, 34])
    res = wmsAdministrator.getSiteSummarySelectors()
    self.assertTrue(res['OK'], res.get('Message'))

    res = wmsAdministrator.clearMask()
    self.assertTrue(res['OK'], res.get('Message'))
    res = wmsAdministrator.getSiteMask()
    self.assertTrue(res['OK'], res.get('Message'))
    self.assertEqual(res['Value'], [], msg="Got %s" % str(res['Value']))


class Matcher (TestWMSTestCase):
  """Testing Matcher
  """

  def test_matcher(self):
    # insert a proper DN to run the test
    resourceDescription = {
        'OwnerGroup': 'prod',
        'OwnerDN': '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser',
        'DIRACVersion': 'pippo',
        'ReleaseVersion': 'blabla',
        'VirtualOrganization': 'LHCb',
        'PilotInfoReportedFlag': 'True',
        'PilotBenchmark': 'anotherPilot',
        'Site': 'DIRAC.Jenkins.ch',
        'CPUTime': 86400}
    wmsClient = WMSClient()

    job = helloWorldJob()
    job.setDestination('DIRAC.Jenkins.ch')
    job.setInputData('/a/bbb')
    job.setType('User')
    jobDescription = createFile(job)
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    self.assertTrue(res['OK'], res.get('Message'))

    jobID = res['Value']

    res = JobStateUpdateClient().setJobStatus(jobID, JobStatus.CHECKING, 'matching', 'source')
    self.assertTrue(res['OK'], res.get('Message'))
    res = JobStateUpdateClient().setJobStatus(jobID, JobStatus.WAITING, 'matching', 'source')
    self.assertTrue(res['OK'], res.get('Message'))

    tqDB = TaskQueueDB()
    tqDefDict = {'OwnerDN': '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser',
                 'OwnerGroup': 'prod', 'Setup': 'dirac-JenkinsSetup', 'CPUTime': 86400}
    res = tqDB.insertJob(jobID, tqDefDict, 10)
    self.assertTrue(res['OK'], res.get('Message'))

    res = MatcherClient().requestJob(resourceDescription)
    print(res)
    self.assertTrue(res['OK'], res.get('Message'))
    wmsClient.deleteJob(jobID)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestWMSTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(WMSChain))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobMonitoring))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobMonitoringMore))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(WMSAdministrator))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Matcher))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
