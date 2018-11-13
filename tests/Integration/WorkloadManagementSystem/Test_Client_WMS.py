""" This is a test of using WMSClient and several other functions in WMS

    In order to run this test we need the following DBs installed:
    - JobDB
    - JobLoggingDB
    - TaskQueueDB
    - SandboxMetadataDB
    - PilotAgentsDB

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

import unittest
import datetime
import tempfile
# from mock import Mock

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from DIRAC import gLogger
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


def helloWorldJob():
  job = Job()
  job.setName("helloWorld")
  exeScriptLocation = find_all('exe-script.py', '..', '/DIRAC/tests/Integration')[0]
  job.setInputSandbox(exeScriptLocation)
  job.setExecutable(exeScriptLocation, "", "helloWorld.log")
  return job


def parametricJob():
  job = Job()
  job.setName("parametric_helloWorld_%n")
  exeScriptLocation = find_all('exe-script.py', '..', '/DIRAC/tests/Integration')[0]
  job.setInputSandbox(exeScriptLocation)
  job.setParameterSequence("args", ['one', 'two', 'three'])
  job.setParameterSequence("iargs", [1, 2, 3])
  job.setExecutable(exeScriptLocation, arguments=": testing %(args)s %(iargs)s", logFile='helloWorld_%n.log')
  return job


def createFile(job):
  tmpdir = tempfile.mkdtemp()
  jobDescription = tmpdir + '/jobDescription.xml'
  with open(jobDescription, 'w') as fd:
    fd.write(job._toXML())
  return jobDescription


class TestWMSTestCase(unittest.TestCase):

  def setUp(self):
    self.maxDiff = None

    gLogger.setLevel('VERBOSE')

  def tearDown(self):
    """ use the JobCleaningAgent method to remove the jobs in status 'deleted' and 'Killed'
    """
    jca = JobCleaningAgent('WorkloadManagement/JobCleaningAgent',
                           'WorkloadManagement/JobCleaningAgent')
    jca.initialize()
    res = jca.removeJobsByStatus({'Status': ['Killed', 'Deleted']})
    print res
    self.assertTrue(res['OK'])


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
    print res
    self.assertTrue(res['OK'])
    self.assertTrue(isinstance(res['Value'], int))
    self.assertEqual(res['Value'], res['JobID'])
    jobID = res['JobID']
    jobID = res['Value']

    # updating the status
    jobStateUpdate.setJobStatus(jobID, 'Running', 'Executing Minchiapp', 'source')

    # reset the job
    res = wmsClient.resetJob(jobID)
    self.assertTrue(res['OK'])

    # reschedule the job
    res = wmsClient.rescheduleJob(jobID)
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'Received')

    # updating the status again
    jobStateUpdate.setJobStatus(jobID, 'Matched', 'matching', 'source')

    # kill the job
    res = wmsClient.killJob(jobID)
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'Killed')

    # updating the status aaaagain
    jobStateUpdate.setJobStatus(jobID, 'Done', 'matching', 'source')

    # kill the job
    res = wmsClient.killJob(jobID)
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'Done')  # this time it won't kill... it's done!

    # delete the job - this will just set its status to "deleted"
    res = wmsClient.deleteJob(jobID)
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'Deleted')

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
    result = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    self.assertTrue(result['OK'])
    jobIDList = result['Value']
    self.assertEqual(len(jobIDList), 3)

    result = jobMonitor.getJobsParameters(jobIDList, ['JobName'])
    self.assertTrue(result['OK'])
    jobNames = [result['Value'][jobID]['JobName'] for jobID in result['Value']]
    self.assertEqual(set(jobNames), set(['parametric_helloWorld_%s' % nJob for nJob in range(3)]))

    for jobID in jobIDList:
      result = jobStateUpdate.setJobStatus(jobID, 'Done', 'matching', 'source')
      self.assertTrue(result['OK'])

    result = wmsClient.deleteJob(jobIDList)
    self.assertTrue(result['OK'])

    for jobID in jobIDList:
      result = jobMonitor.getJobStatus(jobID)
      self.assertTrue(result['OK'])
      self.assertEqual(result['Value'], 'Deleted')


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
    self.assertTrue(res['OK'])
    jobID = int(res['Value'])
    # jobID = res['JobID']
    res = jobMonitor.getJobJDL(jobID, True)
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobJDL(jobID, False)
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobsParameters([jobID], [])
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {})
    res = jobMonitor.getJobsParameters([jobID], ['Owner'])
    self.assertTrue(res['OK'])

    # Adding stuff
    res = jobStateUpdate.setJobStatus(jobID, 'Matched', 'matching', 'source')
    self.assertTrue(res['OK'])
    res = jobStateUpdate.setJobParameters(jobID, [('par1', 'par1Value'), ('par2', 'par2Value')])
    self.assertTrue(res['OK'])
    res = jobStateUpdate.setJobApplicationStatus(jobID, 'app status', 'source')
    self.assertTrue(res['OK'])
#     res = jobStateUpdate.setJobFlag()
#     self.assertTrue(res['OK'])
#     res = jobStateUpdate.unsetJobFlag()
#     self.assertTrue(res['OK'])
    res = jobStateUpdate.setJobSite(jobID, 'Site')
    self.assertTrue(res['OK'])
#     res = jobMonitor.traceJobParameter( 'Site', 1, 'Status' )
#     self.assertTrue(res['OK'])

    # now checking few things
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'Running')
    res = jobMonitor.getJobParameter(jobID, 'par1')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'par1': 'par1Value'})
    res = jobMonitor.getJobParameters(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'par1': 'par1Value', 'par2': 'par2Value'})
    res = jobMonitor.getJobAttribute(jobID, 'Site')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'Site')
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['ApplicationStatus'], 'app status')
    res = jobMonitor.getJobAttributes(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['JobName'], 'helloWorld')
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['ApplicationStatus'], 'app status')
    self.assertEqual(res['Value']['Status'], 'Running')
    res = jobMonitor.getJobHeartBeatData(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])
    res = jobMonitor.getInputData(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])
    res = jobMonitor.getJobPrimarySummary(jobID)
    self.assertTrue(res['OK'])
    res = jobMonitor.getAtticJobParameters(jobID)
    self.assertTrue(res['OK'])
    res = jobStateUpdate.setJobsStatus([jobID], 'Done', 'MinorStatus', 'Unknown')
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobStatus(jobID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Status'], 'Done')
    self.assertEqual(res['Value']['MinorStatus'], 'MinorStatus')
    self.assertEqual(res['Value']['ApplicationStatus'], 'app status')
    res = jobStateUpdate.sendHeartBeat(jobID, {'bih': 'bih'}, {'boh': 'boh'})
    self.assertTrue(res['OK'])

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
#     self.assertTrue(res['OK'])
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
        self.assertTrue(res['OK'])
        jobID = res['Value']
      jobIDs.append(jobID)

    res = jobMonitor.getSites()
    self.assertTrue(res['OK'])
    self.assertTrue(set(res['Value']) <= {'ANY', 'DIRAC.Jenkins.ch'})
    res = jobMonitor.getJobTypes()
    self.assertTrue(res['OK'])
    self.assertEqual(sorted(res['Value']), sorted(types))
    res = jobMonitor.getApplicationStates()
    self.assertTrue(res['OK'])
    self.assertEqual(sorted(res['Value']), sorted(['Unknown']))

    res = jobMonitor.getOwners()
    self.assertTrue(res['OK'])
    res = jobMonitor.getOwnerGroup()
    self.assertTrue(res['OK'])
    res = jobMonitor.getProductionIds()
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobGroups()
    self.assertTrue(res['OK'])
    res = jobMonitor.getStates()
    self.assertTrue(res['OK'])
    self.assertTrue(sorted(res['Value']) in [['Received'], sorted(['Received', 'Waiting'])])
    res = jobMonitor.getMinorStates()
    self.assertTrue(res['OK'])
    self.assertTrue(sorted(res['Value']) in [['Job accepted'], sorted(['Job accepted', 'matching'])])
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobs()
    self.assertTrue(res['OK'])
    self.assertTrue(set([str(x) for x in jobIDs]) <= set(res['Value']))
#     res = jobMonitor.getCounters(attrList)
#     self.assertTrue(res['OK'])
    res = jobMonitor.getCurrentJobCounters()
    self.assertTrue(res['OK'])
    try:
      self.assertTrue(
          res['Value'].get('Received') +
          res['Value'].get('Waiting') >= long(
              len(lfnss) *
              len(types)))
    except TypeError:
      pass
    res = jobMonitor.getJobsSummary(jobIDs)
    self.assertTrue(res['OK'])
    res = jobMonitor.getJobPageSummaryWeb({}, [], 0, 100)
    self.assertTrue(res['OK'])

    res = jobStateUpdate.setJobStatusBulk(jobID,
                                          {str(datetime.datetime.utcnow()): {'Status': 'Running',
                                                                             'MinorStatus': 'MinorStatus',
                                                                             'ApplicationStatus': 'ApplicationStatus',
                                                                             'Source': 'Unknown'}})
    self.assertTrue(res['OK'])
    res = jobStateUpdate.setJobsParameter({jobID: ['Status', 'Running']})
    self.assertTrue(res['OK'])

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
#     self.assertTrue(res['OK'])
#
#     WMSClient().deleteJob( res['Value'] )


class WMSAdministrator(TestWMSTestCase):
  """ testing WMSAdmin - for JobDB
  """

  def test_JobDBWMSAdmin(self):

    wmsAdministrator = RPCClient('WorkloadManagement/WMSAdministrator')

    sitesList = ['My.Site.org', 'Your.Site.org']
    res = wmsAdministrator.setSiteMask(sitesList)
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getSiteMask()
    self.assertTrue(res['OK'])
    self.assertEqual(sorted(res['Value']), sorted(sitesList))
    res = wmsAdministrator.banSite('My.Site.org', 'This is a comment')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getSiteMask()
    self.assertTrue(res['OK'])
    self.assertEqual(sorted(res['Value']), ['Your.Site.org'])
    res = wmsAdministrator.allowSite('My.Site.org', 'This is a comment')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getSiteMask()
    self.assertTrue(res['OK'])
    self.assertEqual(sorted(res['Value']), sorted(sitesList))

    res = wmsAdministrator.getSiteMaskLogging(sitesList)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['My.Site.org'][0][3], 'No comment')
    res = wmsAdministrator.getSiteMaskSummary()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['My.Site.org'], 'Active')

    res = wmsAdministrator.getSiteSummaryWeb({}, [], 0, 100)
    self.assertTrue(res['OK'])
    self.assertTrue(res['Value']['TotalRecords'] in [0, 1, 2, 34])
    res = wmsAdministrator.getSiteSummarySelectors()
    self.assertTrue(res['OK'])

    res = wmsAdministrator.clearMask()
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getSiteMask()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])


class WMSAdministratorPilots(TestWMSTestCase):
  """ testing WMSAdmin - for PilotAgentsDB
  """

  def test_PilotsDB(self):

    wmsAdministrator = RPCClient('WorkloadManagement/WMSAdministrator')
    pilotAgentDB = PilotAgentsDB()

    res = wmsAdministrator.addPilotTQReference(['aPilot'], 1, '/a/ownerDN', 'a/owner/Group')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getCurrentPilotCounters({})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'Submitted': 1})
    res = pilotAgentDB.deletePilot('aPilot')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getCurrentPilotCounters({})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {})

    res = wmsAdministrator.addPilotTQReference(['anotherPilot'], 1, '/a/ownerDN', 'a/owner/Group')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.storePilotOutput('anotherPilot', 'This is an output', 'this is an error')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getPilotOutput('anotherPilot')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'OwnerDN': '/a/ownerDN',
                                    'OwnerGroup': 'a/owner/Group',
                                    'StdErr': 'this is an error',
                                    'FileList': [],
                                    'StdOut': 'This is an output'})
    # need a job for the following
#     res = wmsAdministrator.getJobPilotOutput( 1 )
#     self.assertEqual( res['Value'], {'OwnerDN': '/a/ownerDN', 'OwnerGroup': 'a/owner/Group',
#                                      'StdErr': 'this is an error', 'FileList': [], 'StdOut': 'This is an output'} )
#     self.assertTrue(res['OK'])
    res = wmsAdministrator.getPilotInfo('anotherPilot')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['anotherPilot']['AccountingSent'], 'False')
    self.assertEqual(res['Value']['anotherPilot']['PilotJobReference'], 'anotherPilot')

    res = wmsAdministrator.selectPilots({})
    self.assertTrue(res['OK'])
#     res = wmsAdministrator.getPilotLoggingInfo( 'anotherPilot' )
#     self.assertTrue(res['OK'])
    res = wmsAdministrator.getPilotSummary('', '')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Total']['Submitted'], 1)
    res = wmsAdministrator.getPilotMonitorWeb({}, [], 0, 100)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['TotalRecords'], 1)
    res = wmsAdministrator.getPilotMonitorSelectors()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'GridType': ['DIRAC'],
                                    'OwnerGroup': ['a/owner/Group'],
                                    'DestinationSite': ['NotAssigned'],
                                    'Broker': ['Unknown'], 'Status': ['Submitted'],
                                    'OwnerDN': ['/a/ownerDN'],
                                    'GridSite': ['Unknown'],
                                    'Owner': []})
    res = wmsAdministrator.getPilotSummaryWeb({}, [], 0, 100)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['TotalRecords'], 1)

    res = wmsAdministrator.setAccountingFlag('anotherPilot', 'True')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.setPilotStatus('anotherPilot', 'Running')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getPilotInfo('anotherPilot')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['anotherPilot']['AccountingSent'], 'True')
    self.assertEqual(res['Value']['anotherPilot']['Status'], 'Running')

    res = wmsAdministrator.setJobForPilot(123, 'anotherPilot')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.setPilotBenchmark('anotherPilot', 12.3)
    self.assertTrue(res['OK'])
    res = wmsAdministrator.countPilots({})
    self.assertTrue(res['OK'])
#     res = wmsAdministrator.getCounters()
#     # getPilotStatistics

    res = pilotAgentDB.deletePilot('anotherPilot')
    self.assertTrue(res['OK'])
    res = wmsAdministrator.getCurrentPilotCounters({})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {})


class Matcher (TestWMSTestCase):
  "Testing Matcher"

  def test_matcher(self):
    # insert a proper DN to run the test
    resourceDescription = {
        'OwnerGroup': 'prod',
        'OwnerDN': '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
        'DIRACVersion': 'pippo',
        'ReleaseVersion': 'blabla',
        'VirtualOrganization': 'LHCb',
        'PilotInfoReportedFlag': 'True',
        'PilotBenchmark': 'anotherPilot',
        'Site': 'DIRAC.Jenkins.ch',
        'CPUTime': 86400}

    JobStateUpdate = RPCClient('WorkloadManagement/JobStateUpdate')
    wmsClient = WMSClient()

    job = helloWorldJob()
    job.setDestination('DIRAC.Jenkins.ch')
    job.setInputData('/a/bbb')
    job.setType('User')
    jobDescription = createFile(job)
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    self.assertTrue(res['OK'])

    jobID = res['Value']

    res = JobStateUpdate.setJobStatus(jobID, 'Waiting', 'matching', 'source')
    self.assertTrue(res['OK'])

    tqDB = TaskQueueDB()
    tqDefDict = {'OwnerDN': '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
                 'OwnerGroup': 'prod', 'Setup': 'dirac-JenkinsSetup', 'CPUTime': 86400}
    res = tqDB.insertJob(jobID, tqDefDict, 10)
    self.assertTrue(res['OK'])

    res = MatcherClient().requestJob(resourceDescription)
    print res
    self.assertTrue(res['OK'])
    wmsClient.deleteJob(jobID)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestWMSTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(WMSChain))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobMonitoring))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobMonitoringMore))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(WMSAdministrator))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(WMSAdministratorPilots))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Matcher))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
