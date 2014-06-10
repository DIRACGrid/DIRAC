########################################################################
# $HeadURL$
# File :   SSHLSFComputingElement.py
# Author : S.P.
########################################################################

""" LSF Computing Element with remote job submission via ssh/scp and using site
    shared area for the job proxy placement

    To get the proxy right, don't forget to add in the CS that for this kind of CE, 
    the BundleProxy must be True (default False)

    Do not use yet as some cleanup of submission scripts is not done, as well as the cleanup of outputs.

"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.SSHComputingElement       import SSH, SSHComputingElement 
from DIRAC.Core.Utilities.List                           import breakListIntoChunks
from DIRAC.Core.Utilities.Pfn                            import pfnparse 
from DIRAC                                               import S_OK, S_ERROR

__RCSID__ = "$Id$"

CE_NAME = 'SSHLSF'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHLSFComputingElement( SSHComputingElement ):
  """ For LSF submission via SSH
  """
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'lsfce'
    self.mandatoryParameters = MANDATORY_PARAMETERS

  #############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """

    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs

    ssh = SSH( parameters = self.ceParameters )
    cmd = ["bjobs", "-q" , self.execQueue , "-a" ]
    ret = ssh.sshCall( 100, cmd )

    if not ret['OK']:
      self.log.error( 'Timeout', ret['Message'] )
      return ret

    status = ret['Value'][0]
    stdout = ret['Value'][1]
    stderr = ret['Value'][2]

    self.log.debug( "status:", status )
    self.log.debug( "stdout:", stdout )
    self.log.debug( "stderr:", stderr )

    if status:
      self.log.error( 'Failed bjobs execution:', stderr )
      return S_ERROR( stderr )

    waitingJobs = 0
    runningJobs = 0
    lines = stdout.split( "\n" )
    for line in lines:
      if line.count( "PEND" ) or line.count( 'PSUSP' ):
        waitingJobs += 1
      if line.count( "RUN" ) or line.count( 'USUSP' ):
        runningJobs += 1

    result['WaitingJobs'] = waitingJobs
    result['RunningJobs'] = runningJobs

    self.log.verbose( 'Waiting Jobs: ', waitingJobs )
    self.log.verbose( 'Running Jobs: ', runningJobs )

    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """

    resultDict = {}
    ssh = SSH( parameters = self.ceParameters )

    for jobList in breakListIntoChunks( jobIDList, 100 ):
      jobDict = {}
      for job in jobList:
        result = pfnparse( job )
        jobNumber = result['Value']['FileName']
        if jobNumber:
          jobDict[jobNumber] = job

      jobStamps = jobDict.keys()
      cmd = [ 'bjobs', ' '.join( jobStamps ) ]
      result = ssh.sshCall( 100, cmd )
      if not result['OK']:
        return result
      output = result['Value'][1].replace( '\r', '' )
      lines = output.split( '\n' )
      for job in jobDict:
        resultDict[jobDict[job]] = 'Unknown'
        for line in lines:
          if line.find( job ) != -1:
            if line.find( 'UNKWN' ) != -1:
              resultDict[jobDict[job]] = 'Unknown'
            else:
              lsfStatus = line.split()[2]
              if lsfStatus in ['DONE', 'EXIT']:
                resultDict[jobDict[job]] = 'Done'
              elif lsfStatus in ['RUN', 'SSUSP']:
                resultDict[jobDict[job]] = 'Running'
              elif lsfStatus in ['PEND', 'PSUSP']:
                resultDict[jobDict[job]] = 'Waiting'

    return S_OK( resultDict )
  
  def _getJobOutputFiles( self, jobID ):
    """ Get output file names for the specific CE 
    """
    result = pfnparse( jobID )
    if not result['OK']:
      return result
    jobStamp = result['Value']['FileName']
    host = result['Value']['Host']

    output = '%s/DIRACPilot.o%s' % ( self.batchOutput, jobStamp )
    error = '%s/DIRACPilot.e%s' % ( self.batchError, jobStamp )

    return S_OK( (jobStamp, host, output, error) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
