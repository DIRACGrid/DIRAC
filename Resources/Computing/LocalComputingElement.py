########################################################################
# $HeadURL$
# File :   LocalComputingElement.py
# Author : Ricardo Graciani, A.T.
########################################################################

""" Local (Virtual) Computing Element: it will send jobs directly
"""

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Resources.Computing.PilotBundle               import bundleProxy, writeScript
from DIRAC.Core.Utilities.List                           import uniqueElements
from DIRAC.Core.Utilities.File                           import makeGuid
from DIRAC.Core.Utilities.Pfn                            import pfnparse
from DIRAC.Core.Utilities.Subprocess                     import systemCall
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import rootPath
from DIRAC                                               import gLogger

import os, urllib
import shutil, tempfile
import getpass
from types import StringTypes

CE_NAME = 'Local'
MANDATORY_PARAMETERS = [ 'Queue' ]

class LocalComputingElement( ComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = ''
    self.finalScript = ''
    self.submittedJobs = 0
    self.userName = getpass.getuser()
    self.mandatoryParameters = MANDATORY_PARAMETERS

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )
    # Now batch system specific ones
    if 'ExecQueue' not in self.ceParameters:
      self.ceParameters['ExecQueue'] = self.ceParameters.get( 'Queue', '' )

    if 'SharedArea' not in self.ceParameters:
      self.ceParameters['SharedArea'] = '.'

    if 'BatchOutput' not in self.ceParameters:
      self.ceParameters['BatchOutput'] = 'data'

    if 'BatchError' not in self.ceParameters:
      self.ceParameters['BatchError'] = 'data'

    if 'ExecutableArea' not in self.ceParameters:
      self.ceParameters['ExecutableArea'] = 'data'

    if 'InfoArea' not in self.ceParameters:
      self.ceParameters['InfoArea'] = 'info'

    if 'WorkArea' not in self.ceParameters:
      self.ceParameters['WorkArea'] = 'work'

    if 'SubmitOptions' not in self.ceParameters:
      self.ceParameters['SubmitOptions'] = '-'

  def _reset( self ):
    """ Process CE parameters and make necessary adjustments
    """

    self.queue = self.ceParameters['Queue']
    if 'ExecQueue' not in self.ceParameters or not self.ceParameters['ExecQueue']:
      self.ceParameters['ExecQueue'] = self.ceParameters.get( 'Queue', '' )
    self.execQueue = self.ceParameters['ExecQueue']
    self.log.info( "Using queue: ", self.queue )

    self.sharedArea = self.ceParameters['SharedArea']
    self.batchOutput = self.ceParameters['BatchOutput']
    if not self.batchOutput.startswith( '/' ):
      self.batchOutput = os.path.join( self.sharedArea, self.batchOutput )
    self.batchError = self.ceParameters['BatchError']
    if not self.batchError.startswith( '/' ):
      self.batchError = os.path.join( self.sharedArea, self.batchError )
    self.infoArea = self.ceParameters['InfoArea']
    if not self.infoArea.startswith( '/' ):
      self.infoArea = os.path.join( self.sharedArea, self.infoArea )
    self.executableArea = self.ceParameters['ExecutableArea']
    if not self.executableArea.startswith( '/' ):
      self.executableArea = os.path.join( self.sharedArea, self.executableArea )
    self.workArea = self.ceParameters['WorkArea']
    if not self.workArea.startswith( '/' ):
      self.workArea = os.path.join( self.sharedArea, self.workArea )

    result = self._prepareHost()

    self.submitOptions = ''
    if 'SubmitOptions' in self.ceParameters:
      self.submitOptions = self.ceParameters['SubmitOptions']
    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower()  in ['no', 'false', '0']:
        self.removeOutput = False

  def _prepareHost( self ):
    """ Prepare directories and copy control script 
    """

    # Make remote directories
    dirTuple = uniqueElements( [ self.sharedArea,
                                 self.executableArea,
                                 self.infoArea,
                                 self.batchOutput,
                                 self.batchError,
                                 self.workArea] )
    nDirs = len( dirTuple )
    cmdTuple = [ 'mkdir', '-p' ] + dirTuple
    self.log.verbose( 'Creating working directories' )
    result = systemCall( 30, cmdTuple )
    if not result['OK']:
      self.log.warn( 'Failed creating working directories: %s' % result['Message'][1] )
      return result
    status, output, error = result['Value']
    if status != 0:
      self.log.warn( 'Failed to create directories: %s' % output )
      return S_ERROR( 'Failed to create directories: %s' % output )

    # copy the control script now
    localScript = os.path.join( rootPath, "DIRAC", "Resources", "Computing", "remote_scripts", self.controlScript )
    self.log.verbose( 'Copying %s script' % self.controlScript )
    try:
      shutil.copy( localScript, self.sharedArea )
      # Chmod the control scripts
      self.finalScript = os.path.join( self.sharedArea, self.controlScript )
      os.chmod( self.finalScript, 0o755 )
    except Exception, x:
      self.log.warn( 'Failed copying control script', x )
      return S_ERROR( x )

    return S_OK()

  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):

    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0o755 )

    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
    if proxy:
      self.log.verbose( 'Setting up proxy for payload' )
      wrapperContent = bundleProxy( executableFile, proxy )
      name = writeScript( wrapperContent, os.getcwd() )
      submitFile = name
    else:  # no proxy
      submitFile = executableFile

    result = self._submitJob( submitFile, numberOfJobs )
    if proxy:
      os.remove( submitFile )

    return result

  def _submitJob( self, executableFile, numberOfJobs ):
    """  Submit prepared executable
    """
    # Copy the executable
    executable = os.path.basename( executableFile )
    executable = os.path.join( self.executableArea, executable )
    try:
      shutil.copy( executableFile, executable )
    except Exception, x:
      self.log.warn( 'Failed copying executable', x )
      return S_ERROR( x )

    jobStamps = []
    for i in range( numberOfJobs ):
      jobStamps.append( makeGuid()[:8] )
    jobStamp = '#'.join( jobStamps )

    subOptions = urllib.quote( self.submitOptions )

    cmdTuple = [ self.finalScript, 'submit_job', executable, self.batchOutput, self.batchError,
                 self.workArea, str( numberOfJobs ), self.infoArea, jobStamp, self.execQueue, subOptions ]

    self.log.verbose( 'CE submission command: %s' % ' '.join( cmdTuple ) )

    result = systemCall( 120, cmdTuple )

    if not result['OK']:
      self.log.error( '%s CE job submission failed' % self.ceType, result['Message'] )
      return result

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]

    # Examine results of the job submission
    if status == 0:
      outputLines = stdout.strip().replace( '\r', '' ).split( '\n' )
      try:
        index = outputLines.index( '============= Start output ===============' )
        outputLines = outputLines[index + 1:]
      except:
        return S_ERROR( "Invalid output from submit Job: %s" % outputLines[0] )
      try:
        status = int( outputLines[0] )
      except:
        return S_ERROR( "Failed to submit Job: %s" % outputLines[0] )
      if status != 0:
        message = "Unknown reason"
        if len( outputLines ) > 1:
          message = outputLines[1]
        return S_ERROR( 'Failed to submit Job, reason: %s' % message )
      return S_ERROR( '\n'.join( [stdout, stderr] ) )
      batchIDs = outputLines[1:]
      jobIDs = [ self.ceType.lower() + '://' + self.ceName + '/' + id for id in batchIDs ]
    else:
      return S_ERROR( '\n'.join( [stdout, stderr] ) )

    result = S_OK ( jobIDs )
    self.submittedJobs += len( batchIDs )

    return result

  def killJob( self, jobIDList ):
    """ Kill a bunch of jobs
    """
    if type( jobIDList ) in StringTypes:
      jobIDList = [jobIDList]
    return self._killJobs( jobIDList )

  def _killJobs( self, jobIDList, host = None ):
    """ Kill the jobs for the given list of job IDs
    """
    resultDict = {}
    jobDict = {}
    for job in jobIDList:
      result = pfnparse( job )
      if result['OK']:
        stamp = result['Value']['FileName']
      else:
        self.log.error( 'Invalid job id', job )
        continue
      jobDict[stamp] = job
    stampList = jobDict.keys()

    cmdTuple = [ self.finalScript, 'kill_job', '#'.join( stampList ), self.infoArea ]
    result = systemCall( 10, cmdTuple )

    if not result['OK']:
      return result

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]

    # Examine results of the job submission
    if status != 0:
      outputLines = stdout.strip().replace( '\r', '' ).split( '\n' )
      try:
        index = outputLines.index( '============= Start output ===============' )
        outputLines = outputLines[index + 1:]
      except:
        return S_ERROR( "Invalid output from kill Job: %s" % outputLines[0] )
      try:
        status = int( outputLines[0] )
      except:
        return S_ERROR( "Failed to kill Job: %s" % outputLines[0] )
      if status != 0:
        message = "Unknown reason"
        if len( outputLines ) > 1:
          message = outputLines[1]
        return S_ERROR( 'Failed to kill Job, reason: %s' % message )
      return S_ERROR( '\n'.join( [stdout, stderr] ) )

    return S_OK()

  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    resultHost = self._getStatus()
    if not resultHost['OK']:
      return resultHost

    result['RunningJobs'] = resultHost['Value'].get( 'Running', 0 )
    result['WaitingJobs'] = resultHost['Value'].get( 'Waiting', 0 )
    self.log.verbose( 'Waiting Jobs: ', result['WaitingJobs'] )
    self.log.verbose( 'Running Jobs: ', result['RunningJobs'] )

    return result

  def _getStatus( self ):
    """ Get jobs running
    """
    cmdTuple = [ self.finalScript, 'status_info', self.infoArea, self.workArea, self.userName, self.execQueue ]

    result = systemCall( 10, cmdTuple )
    if not result['OK']:
      return result

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]

    # Examine results of the job status
    resultDict = {}
    if status == 0:
      outputLines = stdout.strip().replace( '\r', '' ).split( '\n' )
      try:
        index = outputLines.index( '============= Start output ===============' )
        outputLines = outputLines[index + 1:]
      except:
        return S_ERROR( "Invalid output from CE get status: %s" % outputLines[0] )
      try:
        status = int( outputLines[0] )
      except:
        return S_ERROR( "Failed to get CE status: %s" % outputLines[0] )
      if status != 0:
        message = "Unknown reason"
        if len( outputLines ) > 1:
          message = outputLines[1]
        return S_ERROR( 'Failed to get CE status, reason: %s' % message )
      for line in outputLines[1:]:
        if ':::' in line:
          jobStatus, nJobs = line.split( ':::' )
          resultDict[jobStatus] = int( nJobs )
    else:
      return S_ERROR( '\n'.join( [stdout, stderr] ) )

    return S_OK( resultDict )

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    return self._getJobStatus( jobIDList )

  def _getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """

    resultDict = {}
    jobDict = {}
    for job in jobIDList:
      result = pfnparse( job )
      if result['OK']:
        stamp = result['Value']['FileName']
      else:
        self.log.error( 'Invalid job id', job )
        continue
      jobDict[stamp] = job
    stampList = jobDict.keys()

    cmdTuple = [ self.finalScript, 'job_status', '#'.join( stampList ), self.infoArea, self.userName ]

    result = systemCall( 10, cmdTuple )
    if not result['OK']:
      return result

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]

    # Examine results of the job status
    if status == 0:
      outputLines = stdout.strip().replace( '\r', '' ).split( '\n' )
      try:
        index = outputLines.index( '============= Start output ===============' )
        outputLines = outputLines[index + 1:]
      except:
        return S_ERROR( "Invalid output from CE get status: %s" % outputLines[0] )
      try:
        status = int( outputLines[0] )
      except:
        return S_ERROR( "Failed to get CE status: %s" % outputLines[0] )
      if status != 0:
        message = "Unknown reason"
        if len( outputLines ) > 1:
          message = outputLines[1]
        return S_ERROR( 'Failed to get CE status, reason: %s' % message )
      for line in outputLines[1:]:
          if ':::' in line:
            jbundle = line.split( ':::' )
            if ( len( jbundle ) == 2 ):
              resultDict[jobDict[jbundle[0]]] = jbundle[1]
    else:
      return S_ERROR( '\n'.join( [stdout, stderr] ) )

    return S_OK( resultDict )

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    result = self._getJobOutputFiles( jobID )
    if not result['OK']:
      return result

    jobStamp, host, outputFile, errorFile = result['Value']

    self.log.verbose( 'Getting output for jobID %s' % jobID )

    if not localDir:
      tempDir = tempfile.mkdtemp()
    else:
      tempDir = localDir

    try:
      localOut = os.path.join( tempDir, '%s.out' % jobStamp )
      localErr = os.path.join( tempDir, '%s.err' % jobStamp )
      if os.path.exists( outputFile ):
        shutil.copy( outputFile, localOut )
      if os.path.exists( errorFile ):
        shutil.copy( errorFile, localErr )
    except Exception, x:
      return S_ERROR( 'Failed to get output files: %s' % str( x ) )

    open( localOut, 'a' ).close()
    open( localErr, 'a' ).close()

    # The result is OK, we can remove the output
    if self.removeOutput and os.path.exists( outputFile ):
      os.remove( outputFile )
    if self.removeOutput and os.path.exists( errorFile ):
      os.remove( errorFile )

    if localDir:
      return S_OK( ( localOut, localErr ) )
    else:
      # Return the output as a string
      outputFile = open( localOut, 'r' )
      output = outputFile.read()
      outputFile.close()
      outputFile = open( localErr, 'r' )
      error = outputFile.read()
      outputFile.close()
      shutil.rmtree( tempDir )
      return S_OK( ( output, error ) )

  def _getJobOutputFiles( self, jobID ):
    """ Get output file names for the specific CE 
    """
    result = pfnparse( jobID )
    if not result['OK']:
      return result
    jobStamp = result['Value']['FileName']
    host = result['Value']['Host']

    output = '%s/%s.out' % ( self.batchOutput, jobStamp )
    error = '%s/%s.out' % ( self.batchError, jobStamp )

    return S_OK( ( jobStamp, host, output, error ) )


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
