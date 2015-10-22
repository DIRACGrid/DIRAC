########################################################################
# File :   HTCondorCEComputingElement.py
# Author : A.S.
########################################################################

""" HTCondorCE Computing Element

   Allows direct submission to HTCondorCE Computing Elements with a SiteDirector Agent
   Needs the condor grid middleware (condor_submit, condor_history, condor_q, condor_rm)

"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Grid                           import executeGridCommand
from DIRAC                                               import S_OK, S_ERROR

from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB     import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector   import WAITING_PILOT_STATUS
from DIRAC.Core.Utilities.File import makeGuid

import os
import tempfile
import commands
CE_NAME = 'HTCondorCE'
MANDATORY_PARAMETERS = [ 'Queue' ]

class HTCondorCEComputingElement( ComputingElement ):
  """HTCondorCE computing element class
  implementing the functions jobSubmit, getJobOutput """

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.submittedJobs = 0
    self.mandatoryParameters = MANDATORY_PARAMETERS
    self.pilotProxy = ''
    self.queue = ''
    self.outputURL = 'gsiftp://localhost'
    self.gridEnv = ''
    self.proxyRenewal = 0

  #############################################################################
  def __writeSub( self, executable, nJobs ):
    """ Create the Sub File for submission

    """

    workingDirectory = self.ceParameters['WorkingDirectory']
    initialDir = '/'.join( workingDirectory.split('/')[:-1] )
    fd, name = tempfile.mkstemp( suffix = '.sub', prefix = 'HTCondorCE_', dir = workingDirectory )
    subFile = os.fdopen( fd, 'w' )

    sub = """
executable = %(executable)s
universe = grid
use_x509userproxy = true
output = $(Cluster).$(Process).out
error = $(Cluster).$(Process).err
log = $(Cluster).$(Process).log
environment = "HTCONDOR_JOBID=$(Cluster).$(Process)"
initialdir = %(initialDir)s
grid_resource = condor %(ceName)s %(ceName)s:9619
ShouldTransferFiles = YES
WhenToTransferOutput = ON_EXIT
Queue %(nJobs)s

""" % dict( executable=executable,
            nJobs=nJobs,
            ceName=self.ceName,
            initialDir=initialDir,
          )
    subFile.write( sub )
    subFile.close()
    return name

  def _reset( self ):
    self.queue = self.ceParameters['Queue']
    self.outputURL = self.ceParameters.get( 'OutputURL', 'gsiftp://localhost' )
    self.gridEnv = self.ceParameters['GridEnv']

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job
    """

    self.log.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0755 )

    subName = self.__writeSub( executableFile, numberOfJobs )

    jobStamps = []
    for _i in range( numberOfJobs ):
      jobStamps.append( makeGuid()[:8] )

    cmd = ['condor_submit', '-terse', subName ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    self.log.verbose( result )
    os.unlink( subName )
    if not result['OK']:
      self.log.error( "Failed to submit jobs to htcondor", result['Message'] )
      return result

    if result['Value'][0]:
      # We have got a non-zero status code
      errorString = result['Value'][2] if result['Value'][2] else result['Value'][1]
      return S_ERROR( 'Pilot submission failed with error: %s ' % errorString.strip() )

    pilotJobReferences = self.__getPilotReferences( result['Value'][1].strip() )
    if not pilotJobReferences['OK']:
      return pilotJobReferences
    pilotJobReferences = pilotJobReferences['Value']

    self.log.verbose( "JobStamps: %s " % jobStamps )
    self.log.verbose( "pilotRefs: %s " % pilotJobReferences )

    result = S_OK( pilotJobReferences )
    result['PilotStampDict'] = dict( zip( pilotJobReferences, jobStamps ) )
    self.log.verbose( "Result for submission: %s " % result )
    return result

  def killJob( self, jobIDList ):
    """ Kill the specified jobs
    """
    if isinstance( jobIDList, basestring ):
      jobIDList = [ jobIDList ]

    self.log.verbose( "KillJob jobIDList: %s" % jobIDList )

    for jobRef in jobIDList:
      job,jobID = self.__condorIDFromJobRef( jobRef )
      self.log.verbose( "Killing pilot %s " % job )
      status,stdout = commands.getstatusoutput( 'condor_rm %s' % jobID )
      if status != 0:
        return S_ERROR( "Failed to kill pilot %s: %s" %( job, stdout ) )

    return S_OK()

#############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    ##getWaitingPilots
    condDict = { 'DestinationSite': self.ceName,
                 'Status': WAITING_PILOT_STATUS }
    res = PilotAgentsDB().countPilots( condDict )
    if res['OK']:
      result[ 'WaitingJobs' ] = int( res['Value'] )
    else:
      self.log.warn( "Failure getting pilot count for %s: %s " % ( self.ceName, res['Message'] ) )

    ##getRunningPilots
    condDict = { 'DestinationSite': self.ceName,
                 'Status': 'Running' }
    res = PilotAgentsDB().countPilots( condDict )
    if res['OK']:
      result[ 'RunningJobs' ] = int( res['Value'] )
    else:
      self.log.warn( "Failure getting pilot count for %s: %s " % ( self.ceName, res['Message'] ) )

    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    self.log.verbose( "Job ID List for status: %s " % jobIDList )
    if isinstance( jobIDList, basestring ):
      jobIDList = [ jobIDList ]

    resultDict = {}
    condorIDs = {}
    ##Get all condorIDs so we can just call condor_q and condor_history once
    for jobRef in jobIDList:
      job,jobID = self.__condorIDFromJobRef( jobRef )
      condorIDs[job] = jobID

    status,stdout_q = commands.getstatusoutput( 'condor_q %s' % ' '.join(condorIDs.values()) )
    if status != 0:
      return S_ERROR( stdout_q )

    status_history,stdout_history = commands.getstatusoutput( 'condor_history %s ' % ' '.join(condorIDs.values()) )
    if status_history == 0:
      stdout_q = '\n'.join( [stdout_q,stdout_history] )

    lines = stdout_q.split( '\n' )

    for job,jobID in condorIDs.iteritems():

      pilotStatus = self.__parseCondorStatus( lines, jobID )
      resultDict[job] = pilotStatus

    if len( resultDict ) != len( jobIDList ):
      for jobRef in jobIDList:
        job = jobRef.split( ":::" )[0]
        if job not in resultDict:
          resultDict[job] = 'Unknown'
    self.log.verbose( "Pilot Statuses: %s " % resultDict )
    return S_OK( resultDict )

  def getJobOutput( self, jobID, _localDir = None ):
    """ TODO: condor can copy the output automatically back to the
    submission, so we just need to pick it up from the proper folder
    FIXME: When do we clean the log and out/err files?
    """
    self.log.verbose( "Getting job output for jobID: %s " % jobID )
    _job,condorID = self.__condorIDFromJobRef( jobID )
    ## FIXME: the WMSAdministrator does not know about the
    ## SiteDirector WorkingDirectory, it might not even run on the
    ## same machine
    workingDirectory = self.ceParameters.get( 'WorkingDirectory',
                                              '/opt/dirac/pro/runit/WorkloadManagement/SiteDirectorHT' )

    output = ''
    error = ''

    outputfilename = os.path.join( workingDirectory, '%s.out' % condorID )
    errorfilename = os.path.join( workingDirectory, '%s.err' % condorID )
    try:
      with open( outputfilename ) as outputfile:
        output = outputfile.read()
    except IOError as e:
      self.log.error( "Failed to open outputfile", str(e) )
      return S_ERROR( "Failed to get pilot output" )
    try:
      with open( errorfilename ) as errorfile:
        error = errorfile.read()
    except IOError as e:
      self.log.error( "Failed to open errorfile", str(e) )
      return S_ERROR( "Failed to get pilot error" )

    return S_OK( ( output, error ) )


  def __getPilotReferences( self, jobString ):
    """get the references from the condor_submit output
    cluster ids look like " 107.0 - 107.0 " or " 107.0 - 107.4 "
    """
    self.log.verbose( "getPilotReferences: %s" % jobString )
    clusterIDs = jobString.split( '-' )
    if len(clusterIDs) != 2:
      return S_ERROR( "Something wrong with the condor_submit output: %s" % jobString )
    clusterIDs = [ clu.strip() for clu in clusterIDs ]
    self.log.verbose( "Cluster IDs parsed: %s " % clusterIDs )
    try:
      clusterID = clusterIDs[0].split( '.' )[0]
      numJobs = clusterIDs[1].split( '.' )[1]
    except IndexError:
      return S_ERROR( "Something wrong with the condor_submit output: %s" % jobString )
    cePrefix = "htcondorce://%s/" % self.ceName
    jobReferences = [ "%s%s.%s" % ( cePrefix, clusterID, i ) for i in range( int( numJobs ) + 1 ) ]
    return S_OK( jobReferences )

  def __condorIDFromJobRef( self, jobRef ):
    """return tuple of "jobURL" and condorID from the jobRef string"""
    jobURL = jobRef.split(":::")[0]
    condorID = jobURL.split("/")[-1]
    return jobURL,condorID

  def __parseCondorStatus( self, lines, jobID ):
    """parse the condor_q or condor_history output for the job status"""
    for line in lines:
      if line.strip().startswith( jobID ):
        if " I " in line:
          return 'Waiting'
        elif " R " in line:
          return 'Running'
        elif " C " in line:
          return 'Done'
        elif " X " in line:
          return 'Aborted'
        elif " H " in line:
          return 'Aborted'
    return 'Unknown'

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
