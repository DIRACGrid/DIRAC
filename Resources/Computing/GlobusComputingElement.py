########################################################################
# File :   GlobusComputingElement.py
# Author : A.S.
########################################################################

""" Globus Computing Element

   Allows direct submission to Globus Computing Elements with a SiteDirector Agent

   Needs the globus grid middleware. On needs open ports GLOBUS_TCP_PORT_RANGE
   to be set or open ports 20000 to 25000 (needs to be confirmed)

"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Grid                           import executeGridCommand
from DIRAC                                               import S_OK, S_ERROR

from DIRAC.ConfigurationSystem.Client.Helpers.Registry   import getGroupOption
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB     import PilotAgentsDB


import os
import tempfile

CE_NAME = 'Globus'
MANDATORY_PARAMETERS = [ 'Queue' ]

class GlobusComputingElement( ComputingElement ):
  """Globus computing element class
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
  def __writeRSL( self, executableFile ):
    """ Create the RSL for submission

    This function is not used except to get the diracStamp
    """

    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, name = tempfile.mkstemp( suffix = '.rsl', prefix = 'Globus_', dir = workingDirectory )
    diracStamp = os.path.basename( name ).replace( '.rsl', '' ).replace( 'Globus_', '' )
    jdlFile = os.fdopen( fd, 'w' )

    jdl = """
[
  JobType = "Normal";
  Executable = "%(executable)s";
  StdOutput="%(diracStamp)s.out";
  StdError="%(diracStamp)s.err";
  InputSandbox={"%(executableFile)s"};
  OutputSandbox={"%(diracStamp)s.out", "%(diracStamp)s.err"};
  OutputSandboxBaseDestUri="%(outputURL)s";
]
    """ % { 'executableFile':executableFile,
            'executable':os.path.basename( executableFile ),
            'outputURL':self.outputURL,
            'diracStamp':diracStamp
          }

    jdlFile.write( jdl )
    jdlFile.close()
    return name, diracStamp

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

    batchIDList = []
    stampDict = {}
    for _i in xrange(numberOfJobs):
      _jdlName, diracStamp = self.__writeRSL( executableFile )
      queueName = '%s/%s' % ( self.ceName, self.queue )
      cmd = ['globus-job-submit', queueName, "-s", executableFile ]
      #cmd = ['globus-job-submit', '-r %s' % queueName, '-f %s' % jdlName ]
      result = executeGridCommand( self.proxy, cmd, self.gridEnv )
      self.log.verbose(result)
      #os.unlink( jdlName )
      if result['OK']:
        if result['Value'][0]:
          # We have got a non-zero status code
          errorString = result['Value'][2] if result['Value'][2] else result['Value'][1]
          return S_ERROR( 'Pilot submission failed with error: %s ' % errorString.strip() )
        pilotJobReference = result['Value'][1].strip()
        if not pilotJobReference:
          return S_ERROR( 'No pilot reference returned from the glite job submission command' )
        if not pilotJobReference.startswith( 'https' ):
          return S_ERROR( 'Invalid pilot reference %s' % pilotJobReference )
        batchIDList.append( pilotJobReference )
        stampDict[pilotJobReference] = diracStamp

    if batchIDList:
      result = S_OK( batchIDList )
      result['PilotStampDict'] = stampDict
    else:
      result = S_ERROR( 'No pilot references obtained from the glite job submission' )
    return result

  def killJob( self, jobIDList ):
    """ Kill the specified jobs
    #FIXME: Needs to be tested
    """
    jobList = list( jobIDList )
    if isinstance(jobIDList, basestring):
      jobList = [ jobIDList ]
    for jobID in jobList:
      cmd = ['globus-job-clean', jobID]
      result = executeGridCommand( self.proxy, cmd, self.gridEnv )
      if not result['OK']:
        return result
      if result['Value'][0] != 0:
        return S_ERROR( 'Failed kill job: %s' % result['Value'][0][1] )

    return S_OK()

#############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """

    resultDict = {}
    self.log.verbose("JobIDList: %s" % jobIDList)
    for jobInfo in jobIDList:
      jobID = jobInfo.split(":::")[0]
      #jobRef = jobInfo.split(":::")[1]

      cmd = ['globus-job-status', jobID ]
      result = executeGridCommand( self.proxy, cmd, self.gridEnv )

      self.log.info("Result from globus-job-status %s " % str(result) )

      if not result['OK']:
        self.log.error( 'Failed to get job status for jobID', jobID  )
        continue
      if result['Value'][0]:
        if result['Value'][2]:
          return S_ERROR( result['Value'][2] )
        else:
          return S_ERROR( 'Error while interrogating job statuses' )

      if result['Value'][1]:
        resultDict[jobID] = self.__parseJobStatus( result['Value'][1] )

    if not resultDict:
      return  S_ERROR( 'No job statuses returned' )

    # If CE does not know about a job, set the status to Unknown
    for jobInfo in jobIDList:
      jobID = jobInfo.split(":::")[0]
      if not resultDict.has_key( jobID ):
        resultDict[jobInfo] = 'Unknown'

    return S_OK( resultDict )

  def __parseJobStatus( self, output ):
    """ Parse the output of the globus-job-status
    """
    self.log.verbose("Output %s " % output)
    output = output.strip()
    self.log.verbose("Output Stripped %s " % output)
    if output in ['DONE']:
      return 'Done'
    elif output in ['FAILED', 'SUSPENDED']:
      return 'Failed'
    elif output in ['PENDING', 'UNSUBMITTED']:
      return 'Scheduled'
    elif output in ['CANCELLED']:
      return 'Killed'
    elif output in ['RUNNING', 'ACTIVE', 'STAGE_IN', 'STAGE_OUT']:
      return 'Running'
    elif output == 'N/A':
      return 'Unknown'

    return 'Unknown'

  def getJobOutput( self, jobID, _localDir = None ):
    """ Get the specified job standard output and error files. The output is returned
        as strings. 
    """

    if jobID.find( ':::' ) != -1:
      pilotRef, stamp = jobID.split( ':::' )
    else:
      pilotRef = jobID
      stamp = ''
    if not stamp:
      return S_ERROR( 'Pilot stamp not defined for %s' % pilotRef )

    ## somehow when this is called from the WMSAdministrator we don't
    ## get the right proxy, so we do all this stuff here now. Probably
    ## should be fixed in the WMSAdministrator? 

    ## Because this function is called from the WMSAdminsitrator, the
    ## gridEnv that is picked up is not the one from the SiteDirector
    ## Definition, but from Computing/CEDefaults
    result = PilotAgentsDB().getPilotInfo(pilotRef)
    if not result['OK'] or not result[ 'Value' ]:
      return S_ERROR('Failed to determine owner for pilot ' + pilotRef)
    pilotDict = result['Value'][pilotRef]
    owner = pilotDict['OwnerDN']
    group = getGroupOption(pilotDict['OwnerGroup'],'VOMSRole',pilotDict['OwnerGroup'])
    ret = gProxyManager.getPilotProxyFromVOMSGroup( owner, group )
    if not ret['OK']:
      self.log.error( ret['Message'] )
      self.log.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( owner, group ) )
      return S_ERROR("Failed to get the pilot's owner proxy")
    self.proxy = ret['Value']


    self.log.verbose("Getting output for: %s " % pilotRef)
    cmd = ['globus-job-get-output', '-out', pilotRef ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    output = ''
    if result['OK']:
      if not result['Value'][0]:
        output = result['Value'][1]
      elif result['Value'][0] == 1 and "No such file or directory" in result['Value'][2]:
        output = "Standard Output is not available on the Globus service"
      else:
        error = '\n'.join( result['Value'][1:] )
        return S_ERROR( error )
    else:
      return S_ERROR( 'Failed to retrieve output for %s' % jobID )


    cmd = ['globus-job-get-output', '-err', pilotRef ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    error = ''
    if result['OK']:
      if not result['Value'][0]:
        error = result['Value'][1]
      elif result['Value'][0] == 1 and "No such file or directory" in result['Value'][2]:
        error = "Standard Error is not available on the Globus service"
      else:
        error = '\n'.join( result['Value'][1:] )
        return S_ERROR( error )
    else:
      return S_ERROR( 'Failed to retrieve error for %s' % jobID )

    return S_OK( ( output, error ) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

