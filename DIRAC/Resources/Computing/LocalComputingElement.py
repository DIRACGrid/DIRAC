########################################################################
# File :   BatchComputingElement.py
# Author : Ricardo Graciani, A.T.
########################################################################

""" BatchComputingElement is a class to handle non-grid computing clusters
"""

import os
import stat
import shutil, tempfile
import getpass
from urlparse import urlparse

from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import gConfig

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Resources.Computing.PilotBundle               import bundleProxy, writeScript
from DIRAC.Core.Utilities.List                           import uniqueElements
from DIRAC.Core.Utilities.File                           import makeGuid
from DIRAC.Core.Utilities.Subprocess                     import systemCall

class LocalComputingElement( ComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )

    self.ceType = ''
    self.execution = "Local"
    self.batchSystem = self.ceParameters.get( 'BatchSystem', 'Host' )
    self.batchModuleFile = None
    self.submittedJobs = 0
    self.userName = getpass.getuser()

  def _reset( self ):
    """ Process CE parameters and make necessary adjustments
    """
    self.batchSystem = self.ceParameters.get( 'BatchSystem', 'Host' )
    self.loadBatchSystem()

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
    if not result['OK']:
      return result

    self.submitOptions = ''
    if 'SubmitOptions' in self.ceParameters:
      self.submitOptions = self.ceParameters['SubmitOptions']
    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower()  in ['no', 'false', '0']:
        self.removeOutput = False
        
    return S_OK()    

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
      defaultPath = os.environ.get( 'HOME', '.' )
      self.ceParameters['SharedArea'] = gConfig.getValue( '/LocalSite/InstancePath', defaultPath )

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
    cmdTuple = [ 'mkdir', '-p' ] + dirTuple
    self.log.verbose( 'Creating working directories' )
    result = systemCall( 30, cmdTuple )
    if not result['OK']:
      self.log.warn( 'Failed creating working directories: %s' % result['Message'][1] )
      return result
    status, output, _error = result['Value']
    if status != 0:
      self.log.warn( 'Failed to create directories: %s' % output )
      return S_ERROR( 'Failed to create directories: %s' % output )

    return S_OK()

  def submitJob( self, executableFile, proxy = None, numberOfJobs = 1 ):

    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )

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

    jobStamps = []
    for _i in range( numberOfJobs ):
      jobStamps.append( makeGuid()[:8] )

    batchDict = { 'Executable': submitFile,
                  'NJobs': numberOfJobs,
                  'OutputDir': self.batchOutput,
                  'ErrorDir': self.batchError,
                  'SubmitOptions': self.submitOptions,
                  'ExecutionContext': self.execution,
                  'JobStamps': jobStamps }
    resultSubmit = self.batch.submitJob( **batchDict )
    if proxy:
      os.remove( submitFile )

    if resultSubmit['Status'] == 0:
      self.submittedJobs += len( resultSubmit['Jobs'] )
      jobIDs = [ self.ceType.lower()+'://'+self.ceName+'/'+_id for _id in resultSubmit['Jobs'] ]  
      result = S_OK( jobIDs )
    else:
      result = S_ERROR( resultSubmit['Message'] )

    return result

  def killJob( self, jobIDList ):
    """ Kill a bunch of jobs
    """

    batchDict = { 'JobIDList': jobIDList }
    resultKill = self.batch.killJob( **batchDict )
    if resultKill['Status'] == 0:
      return S_OK()
    else:
      return S_ERROR( resultKill['Message'] )

  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    batchDict = { 'User': self.userName }
    resultGet = self.batch.getCEStatus( **batchDict )
    if resultGet['Status'] == 0:
      result['RunningJobs'] = resultGet.get( 'Running', 0 )
      result['WaitingJobs'] = resultGet.get( 'Waiting', 0 )
    else:
      result = S_ERROR( resultGet['Message'] )

    self.log.verbose( 'Waiting Jobs: ', result['WaitingJobs'] )
    self.log.verbose( 'Running Jobs: ', result['RunningJobs'] )

    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    batchDict = { 'JobIDList': jobIDList,
                  'User': self.userName }
    resultGet = self.batch.getJobStatus( **batchDict )
    if resultGet['Status'] == 0:
      result = S_OK( resultGet['Jobs'] )
    else:
      result = S_ERROR( resultGet['Message'] )

    return result

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned
        as strings.
    """
    result = self._getJobOutputFiles( jobID )
    if not result['OK']:
      return result

    jobStamp, _host, outputFile, errorFile = result['Value']

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
    except Exception as x:
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
    jobStamp = os.path.basename( urlparse( jobID ).path )
    host = urlparse( jobID ).hostname
    
    if hasattr( self.batch, 'getOutputFiles' ):
      output, error = self.batch.getOutputFiles( jobStamp, 
                                                 self.batchOutput,
                                                 self.batchError )
    else:
      output = '%s/%s.out' % ( self.batchOutput, jobStamp )
      error = '%s/%s.out' % ( self.batchError, jobStamp )
  
    return S_OK( ( jobStamp, host, output, error ) )


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
