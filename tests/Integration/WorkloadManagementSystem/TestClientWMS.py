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

import unittest, datetime
import os, tempfile
# from mock import Mock

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from TestDIRAC.Utilities.utils import find_all

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB

from DIRAC import gLogger

def helloWorldJob():
  job = Job()
  job.setName( "helloWorld" )
  exeScriptLocation = find_all( 'exe-script.py', '.', 'WorkloadManagementSystem' )[0]
  job.setInputSandbox( exeScriptLocation )
  job.setExecutable( exeScriptLocation, "", "helloWorld.log" )
  return job

def createFile( job ):
  tmpdir = tempfile.mkdtemp()
  jobDescription = tmpdir + '/jobDescription.xml'
  fd = os.open( jobDescription, os.O_RDWR | os.O_CREAT )
  os.write( fd, job._toXML() )
  os.close( fd )
  return jobDescription


class TestWMSTestCase( unittest.TestCase ):

  def setUp( self ):
    self.maxDiff = None

    gLogger.setLevel( 'VERBOSE' )

  def tearDown( self ):
    """ use the JobCleaningAgent method to remove the jobs in status 'deleted' and 'Killed'
    """
    jca = JobCleaningAgent( 'WorkloadManagement/JobCleaningAgent',
                            'WorkloadManagement/JobCleaningAgent' )
    jca.initialize()
    res = jca.removeJobsByStatus( { 'Status' : ['Killed', 'Deleted'] } )
    print res
    self.assert_( res['OK'] )

class WMSChain( TestWMSTestCase ):

  def test_FullChain( self ):
    """ This test will

        - call all the WMSClient methods
          that will end up calling all the JobManager service methods
        - use the JobMonitoring to verify few properties
        - call the JobCleaningAgent to eliminate job entries from the DBs
    """
    wmsClient = WMSClient()
    jobMonitor = JobMonitoringClient()
    jobStateUpdate = RPCClient( 'WorkloadManagement/JobStateUpdate' )

    # create the job
    job = helloWorldJob()
    jobDescription = createFile( job )

    # submit the job
    res = wmsClient.submitJob( job._toJDL( xmlFile = jobDescription ) )
    self.assert_( res['OK'] )
  # self.assertEqual( type( res['Value'] ), int )
  # self.assertEqual( res['Value'], res['JobID'] )
  # jobID = res['JobID']
    jobID = res['Value']

    # updating the status
    jobStateUpdate.setJobStatus( jobID, 'Running', 'Executing Minchiapp', 'source' )

    # reset the job
    res = wmsClient.resetJob( jobID )
    self.assert_( res['OK'] )

    # reschedule the job
    res = wmsClient.rescheduleJob( jobID )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobStatus( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Received' )

    # updating the status again
    jobStateUpdate.setJobStatus( jobID, 'Matched', 'matching', 'source' )

    # kill the job
    res = wmsClient.killJob( jobID )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobStatus( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Killed' )

    # updating the status aaaagain
    jobStateUpdate.setJobStatus( jobID, 'Done', 'matching', 'source' )

    # kill the job
    res = wmsClient.killJob( jobID )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobStatus( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Done' )  # this time it won't kill... it's done!

    # delete the job - this will just set its status to "deleted"
    res = wmsClient.deleteJob( jobID )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobStatus( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Deleted' )


class JobMonitoring( TestWMSTestCase ):

  def test_JobStateUpdateAndJobMonitoring( self ):
    """ Verifying all JobStateUpdate and JobMonitoring functions
    """
    wmsClient = WMSClient()
    jobMonitor = JobMonitoringClient()
    jobStateUpdate = RPCClient( 'WorkloadManagement/JobStateUpdate' )

    # create a job and check stuff
    job = helloWorldJob()
    jobDescription = createFile( job )

    # submitting the job. Checking few stuff
    res = wmsClient.submitJob( job._toJDL( xmlFile = jobDescription ) )
    self.assert_( res['OK'] )
    jobID = int ( res['Value'] )
    # jobID = res['JobID']
    res = jobMonitor.getJobJDL( jobID, True )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobJDL( jobID, False )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobsParameters( [jobID], [] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {} )
    res = jobMonitor.getJobsParameters( [jobID], ['Owner'] )
    self.assert_( res['OK'] )

    # Adding stuff
    res = jobStateUpdate.setJobStatus( jobID, 'Matched', 'matching', 'source' )
    self.assert_( res['OK'] )
    res = jobStateUpdate.setJobParameters( jobID, [( 'par1', 'par1Value' ), ( 'par2', 'par2Value' )] )
    self.assert_( res['OK'] )
    res = jobStateUpdate.setJobApplicationStatus( jobID, 'app status', 'source' )
    self.assert_( res['OK'] )
#     res = jobStateUpdate.setJobFlag()
#     self.assert_( res['OK'] )
#     res = jobStateUpdate.unsetJobFlag()
#     self.assert_( res['OK'] )
    res = jobStateUpdate.setJobSite( jobID, 'Site' )
    self.assert_( res['OK'] )
#     res = jobMonitor.traceJobParameter( 'Site', 1, 'Status' )
#     self.assert_( res['OK'] )

    # now checking few things
    res = jobMonitor.getJobStatus( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Running' )
    res = jobMonitor.getJobParameter( jobID, 'par1' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'par1': 'par1Value'} )
    res = jobMonitor.getJobParameters( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'par1': 'par1Value', 'par2': 'par2Value'} )
    res = jobMonitor.getJobAttribute( jobID, 'Site' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Site' )
    res = jobMonitor.getJobAttributes( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['ApplicationStatus'], 'app status' )
    self.assertEqual( res['Value']['JobName'], 'helloWorld' )
    res = jobMonitor.getJobSummary( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['ApplicationStatus'], 'app status' )
    self.assertEqual( res['Value']['Status'], 'Running' )
    res = jobMonitor.getJobHeartBeatData( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )
    res = jobMonitor.getInputData( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )
    res = jobMonitor.getJobPrimarySummary( jobID )
    self.assert_( res['OK'] )
    res = jobMonitor.getAtticJobParameters( jobID )
    self.assert_( res['OK'] )
    res = jobStateUpdate.setJobsStatus( [jobID], 'Done', 'MinorStatus', 'Unknown' )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobSummary( jobID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Status'], 'Done' )
    self.assertEqual( res['Value']['MinorStatus'], 'MinorStatus' )
    self.assertEqual( res['Value']['ApplicationStatus'], 'app status' )
    res = jobStateUpdate.sendHeartBeat( jobID, {'bih':'bih'}, {'boh':'boh'} )
    self.assert_( res['OK'] )


    # delete the job - this will just set its status to "deleted"
    wmsClient.deleteJob( jobID )


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
#     self.assert_( res['OK'] )
#     self.assertEqual( type( res['Value'] ), int )


class JobMonitoringMore( TestWMSTestCase ):

  def test_JobStateUpdateAndJobMonitoringMultuple( self ):
    """ # Now, let's submit some jobs. Different sites, types, inputs
    """
    wmsClient = WMSClient()
    jobMonitor = JobMonitoringClient()
    jobStateUpdate = RPCClient( 'WorkloadManagement/JobStateUpdate' )

    jobIDs = []
    dests = ['DIRAC.site1.org', 'DIRAC.site2.org']
    lfnss = [['/a/1.txt', '/a/2.txt'], ['/a/1.txt', '/a/3.txt', '/a/4.txt'], []]
    types = ['User', 'Test']
    for dest in dests:
      for lfns in lfnss:
        for jobType in types:
          job = helloWorldJob()
          job.setDestination( dest )
          job.setInputData( lfns )
          job.setType( jobType )
          jobDescription = createFile( job )
          res = wmsClient.submitJob( job._toJDL( xmlFile = jobDescription ) )
          self.assert_( res['OK'] )
          jobID = res['Value']
          jobIDs.append( jobID )

    res = jobMonitor.getSites()
    self.assert_( res['OK'] )
    self.assert_( set( res['Value'] ) <= set( dests + ['ANY', 'DIRAC.Jenkins.org'] ) )
    res = jobMonitor.getJobTypes()
    self.assert_( res['OK'] )
    self.assertEqual( sorted( res['Value'] ), sorted( types ) )
    res = jobMonitor.getApplicationStates()
    self.assert_( res['OK'] )
    self.assertEqual( sorted( res['Value'] ), sorted( ['Unknown'] ) )

    res = jobMonitor.getOwners()
    self.assert_( res['OK'] )
    res = jobMonitor.getOwnerGroup()
    self.assert_( res['OK'] )
    res = jobMonitor.getProductionIds()
    self.assert_( res['OK'] )
    res = jobMonitor.getJobGroups()
    self.assert_( res['OK'] )
    res = jobMonitor.getStates()
    self.assert_( res['OK'] )
    self.assert_( sorted( res['Value'] ) in [['Received'], sorted( ['Received', 'Waiting'] )] )
    res = jobMonitor.getMinorStates()
    self.assert_( res['OK'] )
    self.assert_( sorted( res['Value'] ) in [['Job accepted'], sorted( ['Job accepted', 'matching'] ) ] )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobs()
    self.assert_( res['OK'] )
    self.assert_( set( [str( x ) for x in jobIDs] ) <= set( res['Value'] ) )
#     res = jobMonitor.getCounters(attrList)
#     self.assert_( res['OK'] )
    res = jobMonitor.getCurrentJobCounters()
    self.assert_( res['OK'] )
    try:
      self.assert_( res['Value'].get( 'Received' ) + res['Value'].get( 'Waiting' ) >= long( len( dests ) * len( lfnss ) * len( types ) ) )
    except TypeError:
      pass
    res = jobMonitor.getJobsSummary( jobIDs )
    self.assert_( res['OK'] )
    res = jobMonitor.getJobPageSummaryWeb( {}, [], 0, 100 )
    self.assert_( res['OK'] )

    res = jobStateUpdate.setJobStatusBulk( jobID, {str( datetime.datetime.utcnow() ):{'Status': 'Running',
                                                                                      'MinorStatus': 'MinorStatus',
                                                                                      'ApplicationStatus': 'ApplicationStatus',
                                                                                      'Source': 'Unknown'}} )
    self.assert_( res['OK'] )
    res = jobStateUpdate.setJobsParameter( {jobID:['Status', 'Running']} )
    self.assert_( res['OK'] )

    # delete the jobs - this will just set its status to "deleted"
    wmsClient.deleteJob( jobIDs )


#   def test_submitFail( self ):
#
#     # Adding a platform that should not exist
#     job = helloWorldJob()
#     job.setPlatform( "notExistingPlatform" )
#     jobDescription = createFile( job )
#
#     res = WMSClient().submitJob( job._toJDL( xmlFile = jobDescription ) )
#     self.assert_( res['OK'] )
#
#     WMSClient().deleteJob( res['Value'] )


class WMSAdministrator( TestWMSTestCase ):
  """ testing WMSAdmin - for JobDB
  """
  
  def test_JobDBWMSAdmin(self):
  
    wmsAdministrator = RPCClient( 'WorkloadManagement/WMSAdministrator' )

    sitesList = ['My.Site.org', 'Your.Site.org']
    res = wmsAdministrator.setSiteMask( sitesList )
    self.assert_( res['OK'] )
    res = wmsAdministrator.getSiteMask()
    self.assert_( res['OK'] )
    self.assertEqual( sorted( res['Value'] ), sorted( sitesList ) )
    res = wmsAdministrator.banSite( 'My.Site.org', 'This is a comment' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.getSiteMask()
    self.assert_( res['OK'] )
    self.assertEqual( sorted( res['Value'] ), ['Your.Site.org'] )
    res = wmsAdministrator.allowSite( 'My.Site.org', 'This is a comment' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.getSiteMask()
    self.assert_( res['OK'] )
    self.assertEqual( sorted( res['Value'] ), sorted( sitesList ) )

    res = wmsAdministrator.getSiteMaskLogging( sitesList )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['My.Site.org'][0][3], 'No comment' )
    res = wmsAdministrator.getSiteMaskSummary()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['My.Site.org'], 'Active' )

    res = wmsAdministrator.getSiteSummaryWeb( {}, [], 0, 100 )
    self.assert_( res['OK'] )
    self.assert_( res['Value']['TotalRecords'] in [0, 1, 2, 34] )
    res = wmsAdministrator.getSiteSummarySelectors()
    self.assert_( res['OK'] )

    res = wmsAdministrator.clearMask()
    self.assert_( res['OK'] )
    res = wmsAdministrator.getSiteMask()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

class WMSAdministratorPilots( TestWMSTestCase ):
  """ testing WMSAdmin - for PilotAgentsDB
  """

  def test_PilotsDB( self ):

    wmsAdministrator = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    pilotAgentDB = PilotAgentsDB()


    res = wmsAdministrator.addPilotTQReference( ['aPilot'], 1, '/a/ownerDN', 'a/owner/Group' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.getCurrentPilotCounters( {} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'Submitted': 1L} )
    res = pilotAgentDB.deletePilot( 'aPilot' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.getCurrentPilotCounters( {} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {} )

    res = wmsAdministrator.addPilotTQReference( ['anotherPilot'], 1, '/a/ownerDN', 'a/owner/Group' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.storePilotOutput( 'anotherPilot', 'This is an output', 'this is an error' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.getPilotOutput( 'anotherPilot' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'OwnerDN': '/a/ownerDN',
                                     'OwnerGroup': 'a/owner/Group',
                                     'StdErr': 'this is an error',
                                     'FileList': [],
                                     'StdOut': 'This is an output'} )
    # need a job for the following
#     res = wmsAdministrator.getJobPilotOutput( 1 )
#     self.assertEqual( res['Value'], {'OwnerDN': '/a/ownerDN', 'OwnerGroup': 'a/owner/Group',
#                                      'StdErr': 'this is an error', 'FileList': [], 'StdOut': 'This is an output'} )
#     self.assert_( res['OK'] )
    res = wmsAdministrator.getPilotInfo( 'anotherPilot' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['anotherPilot']['AccountingSent'], 'False' )
    self.assertEqual( res['Value']['anotherPilot']['PilotJobReference'], 'anotherPilot' )

    res = wmsAdministrator.selectPilots( {} )
    self.assert_( res['OK'] )
#     res = wmsAdministrator.getPilotLoggingInfo( 'anotherPilot' )
#     self.assert_( res['OK'] )
    res = wmsAdministrator.getPilotSummary( '', '' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Total']['Submitted'], 1 )
    res = wmsAdministrator.getPilotMonitorWeb( {}, [], 0, 100 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['TotalRecords'], 1 )
    res = wmsAdministrator.getPilotMonitorSelectors()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'GridType': ['DIRAC'], 
                                     'OwnerGroup': ['a/owner/Group'], 
                                     'DestinationSite': ['NotAssigned'], 
                                     'Broker': ['Unknown'], 'Status': ['Submitted'], 
                                     'OwnerDN': ['/a/ownerDN'], 
                                     'GridSite': ['Unknown'], 
                                     'Owner': []} )
    res = wmsAdministrator.getPilotSummaryWeb( {}, [], 0, 100 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['TotalRecords'], 1 )

    res = wmsAdministrator.setAccountingFlag( 'anotherPilot', 'True' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.setPilotStatus( 'anotherPilot', 'Running' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.getPilotInfo( 'anotherPilot' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['anotherPilot']['AccountingSent'], 'True' )
    self.assertEqual( res['Value']['anotherPilot']['Status'], 'Running' )

    res = wmsAdministrator.setJobForPilot( 123, 'anotherPilot' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.setPilotBenchmark( 'anotherPilot', 12.3 )
    self.assert_( res['OK'] )
    res = wmsAdministrator.countPilots( {} )
    self.assert_( res['OK'] )
#     res = wmsAdministrator.getCounters()
#     # getPilotStatistics

    res = pilotAgentDB.deletePilot( 'anotherPilot' )
    self.assert_( res['OK'] )
    res = wmsAdministrator.getCurrentPilotCounters( {} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {} )


class Matcher ( TestWMSTestCase ):
  "Testing Matcher"

  def test_matcher( self ):
    # insert a proper DN to run the test
    resourceDescription = {'OwnerGroup': 'prod', 'OwnerDN':'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
                           'DIRACVersion': 'pippo', 'ReleaseVersion':'blabla', 'VirtualOrganization':'LHCB',
                           'PilotInfoReportedFlag':'True', 'PilotBenchmark':'anotherPilot', 'LHCbPlatform':'CERTO',
                           'Site':'DIRAC.Jenkins.org', 'CPUTime' : 86400 }
    matcher = RPCClient( 'WorkloadManagement/Matcher' )
    JobStateUpdate = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    wmsClient = WMSClient()

    job = helloWorldJob()
    job.setDestination( 'DIRAC.Jenkins.org' )
    job.setInputData( '/a/bbb' )
    job.setType( 'User' )
    jobDescription = createFile( job )
    res = wmsClient.submitJob( job._toJDL( xmlFile = jobDescription ) )
    self.assert_( res['OK'] )

    jobID = res['Value']

    res = JobStateUpdate.setJobStatus( jobID, 'Waiting', 'matching', 'source' )
    self.assert_( res['OK'] )


    tqDB = TaskQueueDB()
    tqDefDict = {'OwnerDN': '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
                 'OwnerGroup':'prod', 'Setup':'JenkinsSetup', 'CPUTime':86400}
    res = tqDB.insertJob( jobID, tqDefDict, 10 )
    self.assert_( res['OK'] )

    res = matcher.requestJob( resourceDescription )
    print res
    self.assert_( res['OK'] )
    wmsClient.deleteJob( jobID )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestWMSTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( WMSChain ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobMonitoring ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobMonitoringMore ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( WMSAdministrator ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( WMSAdministratorPilots ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Matcher ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
