########################################################################
# $HeadURL$
# File :   SSHComputingElement.py
# Author : Dumitru Laurentiu, A.T.
########################################################################

""" SSH (Virtual) Computing Element: For a given IP/host it will send jobs directly through ssh
"""

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Resources.Computing.PilotBundle               import bundleProxy, writeScript    
from DIRAC.Core.Utilities.List                           import uniqueElements
from DIRAC.Core.Utilities.File                           import makeGuid
from DIRAC.Core.Utilities.Pfn                            import pfnparse 
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import rootPath
from DIRAC                                               import gLogger

import os, urllib
import shutil, tempfile
from types import StringTypes

__RCSID__ = "$Id$"

CE_NAME = 'SSH'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSH( object ):
  """ The SSH interface
  """
  def __init__( self, user = None, host = None, password = None, key = None, parameters = {} ):

    self.user = user
    if not user:
      self.user = parameters.get( 'SSHUser', '' )
    self.host = host
    if not host:
      self.host = parameters.get( 'SSHHost', '' )
    self.password = password
    if not password:
      self.password = parameters.get( 'SSHPassword', '' )
    self.key = key
    if not key:
      self.key = parameters.get( 'SSHKey', '' )
    self.log = gLogger.getSubLogger( 'SSH' )  

  def __ssh_call( self, command, timeout ):

    try:
      from DIRAC.Resources.Computing import pexpect
      expectFlag = True
    except:
      from DIRAC.Core.Utilities.Subprocess import shellCall
      expectFlag = False

    if not timeout:
      timeout = 999

    if expectFlag:
      ssh_newkey = 'Are you sure you want to continue connecting'
      try:
        child = pexpect.spawn( command, timeout = timeout )
  
        i = child.expect( [pexpect.TIMEOUT, ssh_newkey, pexpect.EOF, 'assword: '] )
        if i == 0: # Timeout        
          return S_OK( ( -1, child.before, 'SSH login failed' ) )
        elif i == 1: # SSH does not have the public key. Just accept it.
          child.sendline ( 'yes' )
          child.expect ( 'assword: ' )
          i = child.expect( [pexpect.TIMEOUT, 'assword: '] )
          if i == 0: # Timeout
            return S_OK( ( -1, str( child.before ) + str( child.after ), 'SSH login failed' ) )
          elif i == 1:
            child.sendline( self.password )
            child.expect( pexpect.EOF )
            return S_OK( ( 0, child.before, '' ) )
        elif i == 2:
          # Passwordless login, get the output
          return S_OK( ( 0, child.before, '' ) )
  
  
        if self.password:
          child.sendline( self.password )
          child.expect( pexpect.EOF )
          return S_OK( ( 0, child.before, '' ) )
        else:
          return S_ERROR( ( -2, child.before, '' ) )
      except Exception, x:
        res = ( -1 , 'Encountered exception %s: %s' % ( Exception, str( x ) ) )
        return S_ERROR( res )  
    else:
      # Try passwordless login
      result = shellCall( timeout, command )
#      print ( "!!! SSH command: %s returned %s\n" % (command, result) )
      if result['Value'][0] == 255:
        return S_ERROR ( ( -1, 'Cannot connect to host %s' % self.host, '' ) )
      return result

  def sshCall( self, timeout, cmdSeq ):
    """ Execute remote command via a ssh remote call
    """

    command = cmdSeq
    if type( cmdSeq ) == type( [] ):
      command = ' '.join( cmdSeq )

    key = ''
    if self.key:
      key = ' -i %s ' % self.key

    pattern = "'===><==='"
    command = 'ssh -q %s -l %s %s "echo %s;%s"' % ( key, self.user, self.host, pattern, command )    
    self.log.debug( "SSH command %s" % command )
    result = self.__ssh_call( command, timeout )    
    self.log.debug( "SSH command result %s" % str( result ) )
    if not result['OK']:
      return result
    
    # Take the output only after the predefined pattern
    ind = result['Value'][1].find('===><===')
    if ind == -1:
      return result

    status, output, error = result['Value']
    output = output[ind+8:]
    if output.startswith('\r'):
      output = output[1:]
    if output.startswith('\n'):
      output = output[1:]  
      
    result['Value'] = ( status, output, error )
    return result

  def scpCall( self, timeout, localFile, destinationPath, upload = True ):
    """ Execute scp copy
    """
    key = ''
    if self.key:
      key = ' -i %s ' % self.key

    if upload:
      command = "scp %s %s %s@%s:%s" % ( key, localFile, self.user, self.host, destinationPath )
    else:
      command = "scp %s %s@%s:%s %s" % ( key, self.user, self.host, destinationPath, localFile )
    self.log.debug( "SCP command %s" % command )
    return self.__ssh_call( command, timeout )

class SSHComputingElement( ComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'sshce'
    self.submittedJobs = 0
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
      
    result = self._prepareRemoteHost()

    self.submitOptions = ''
    if 'SubmitOptions' in self.ceParameters:
      self.submitOptions = self.ceParameters['SubmitOptions']
    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower()  in ['no', 'false', '0']:
        self.removeOutput = False

  def _prepareRemoteHost(self, host=None ):
    """ Prepare remote directories and upload control script 
    """
    
    ssh = SSH( host = host, parameters = self.ceParameters )
    
    # Make remote directories
    dirTuple = tuple ( uniqueElements( [self.sharedArea, 
                                        self.executableArea, 
                                        self.infoArea, 
                                        self.batchOutput, 
                                        self.batchError,
                                        self.workArea] ) )
    nDirs = len( dirTuple )
    cmd = 'mkdir -p %s; '*nDirs % dirTuple
    self.log.verbose( 'Creating working directories on %s' % self.ceParameters['SSHHost'] )
    result = ssh.sshCall( 30, cmd )
    if not result['OK']:
      self.log.warn( 'Failed creating working directories: %s' % result['Message'][1] )
      return result
    status, output, error = result['Value']
    if status == -1:
      self.log.warn( 'TImeout while creating directories' )
      return S_ERROR( 'TImeout while creating directories' )
    if "cannot" in output:
      self.log.warn( 'Failed to create directories: %s' % output )
      return S_ERROR( 'Failed to create directories: %s' % output )
    
    # Upload the control script now
    sshScript = os.path.join( rootPath, "DIRAC", "Resources", "Computing", "remote_scripts", self.controlScript )
    self.log.verbose( 'Uploading %s script to %s' % ( self.controlScript, self.ceParameters['SSHHost'] ) )
    result = ssh.scpCall( 30, sshScript, self.sharedArea )
    if not result['OK']:
      self.log.warn( 'Failed uploading control script: %s' % result['Message'][1] )
      return result
    status, output, error = result['Value']
    if status != 0:
      if status == -1:
        self.log.warn( 'Timeout while uploading control script' )
        return S_ERROR( 'Timeout while uploading control script' )
      else:  
        self.log.warn( 'Failed uploading control script: %s' % output )
        return S_ERROR( 'Failed uploading control script' )
      
    # Chmod the control scripts
    self.log.verbose( 'Chmod +x control script' )
    result = ssh.sshCall( 10, "chmod +x %s/%s" % ( self.sharedArea, self.controlScript ) )
    if not result['OK']:
      self.log.warn( 'Failed chmod control script: %s' % result['Message'][1] )
      return result
    status, output, error = result['Value']
    if status != 0:
      if status == -1:
        self.log.warn( 'Timeout while chmod control script' )
        return S_ERROR( 'Timeout while chmod control script' )
      else:  
        self.log.warn( 'Failed uploading chmod script: %s' % output )
        return S_ERROR( 'Failed uploading chmod script' )
    
    return S_OK()

  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):

#    self.log.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0755 )

    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
    if proxy:
      self.log.verbose( 'Setting up proxy for payload' )
      wrapperContent = bundleProxy( executableFile, proxy )
      name = writeScript( wrapperContent, os.getcwd() )
      submitFile = name
    else: # no proxy
      submitFile = executableFile

    result = self._submitJobToHost( submitFile, numberOfJobs )
    if proxy:
      os.remove( submitFile )
      
    return result  
  
  def _submitJobToHost( self, executableFile, numberOfJobs, host = None ):
    """  Submit prepared executable to the given host
    """
    ssh = SSH( host = host, parameters = self.ceParameters )
    # Copy the executable
    sFile = os.path.basename( executableFile )
    result = ssh.scpCall( 10, executableFile, '%s/%s' % ( self.executableArea, os.path.basename( executableFile ) ) )
    if not result['OK']:
      return result  
    
    jobStamps = []
    for i in range( numberOfJobs ):
      jobStamps.append( makeGuid()[:8] )
    jobStamp = '#'.join( jobStamps )
    
    subOptions = urllib.quote( self.submitOptions )
    
    cmd = "bash --login -c '%s/%s submit_job %s/%s %s %s %s %d %s %s %s %s'" % ( self.sharedArea, 
                                                                                 self.controlScript, 
                                                                                 self.executableArea, 
                                                                                 os.path.basename( executableFile ),  
                                                                                 self.batchOutput, 
                                                                                 self.batchError,
                                                                                 self.workArea,
                                                                                 numberOfJobs,
                                                                                 self.infoArea,
                                                                                 jobStamp,
                                                                                 self.execQueue,
                                                                                 subOptions )

    self.log.verbose( 'CE submission command: %s' %  cmd )

    result = ssh.sshCall( 120, cmd )
    if not result['OK']:
      self.log.error( '%s CE job submission failed' % self.ceType, result['Message'] )
      return result

    sshStatus = result['Value'][0]
    sshStdout = result['Value'][1]
    sshStderr = result['Value'][2]
    
    # Examine results of the job submission
    submitHost = host
    if host is None:
      submitHost = self.ceParameters['SSHHost'].split('/')[0]
        
    if sshStatus == 0:
      outputLines = sshStdout.strip().replace('\r','').split('\n')
      try:
        index = outputLines.index('============= Start output ===============')
        outputLines = outputLines[index+1:]
      except:
        return S_ERROR( "Invalid output from job submission: %s" % outputLines[0] )  
      try:
        status = int( outputLines[0] )
      except:
        return S_ERROR( "Failed local batch job submission: %s" % outputLines[0] )
      if status != 0:
        message = "Unknown reason"
        if len( outputLines ) > 1:
          message = outputLines[1]
        return S_ERROR( 'Failed job submission, reason: %s' % message )   
      else:
        batchIDs = outputLines[1:]
        jobIDs = [ self.ceType.lower()+'://'+self.ceName+'/'+id for id in batchIDs ]    
    else:
      return S_ERROR( '\n'.join( [sshStdout, sshStderr] ) )

    result = S_OK ( jobIDs )
    self.submittedJobs += len( batchIDs )

    return result
    
  def killJob( self, jobIDList ):
    """ Kill a bunch of jobs
    """
    if type( jobIDList ) in StringTypes:
      jobIDList = [jobIDList]
    return self._killJobOnHost( jobIDList )

  def _killJobOnHost( self, jobIDList, host = None ):
    """ Kill the jobs for the given list of job IDs
    """ 
    resultDict = {}
    ssh = SSH( host = host, parameters = self.ceParameters )
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

    cmd = "bash --login -c '%s/%s kill_job %s %s'" % ( self.sharedArea, self.controlScript, '#'.join( stampList ), 
                                                       self.infoArea )
    result = ssh.sshCall( 10, cmd )
    if not result['OK']:
      return result
    
    sshStatus = result['Value'][0]
    sshStdout = result['Value'][1]
    sshStderr = result['Value'][2]
    
    # Examine results of the job submission
    if sshStatus == 0:
      outputLines = sshStdout.strip().replace('\r','').split('\n')
      try:
        index = outputLines.index('============= Start output ===============')
        outputLines = outputLines[index+1:]
      except:
        return S_ERROR( "Invalid output from job kill: %s" % outputLines[0] )  
      try:
        status = int( outputLines[0] )
      except:
        return S_ERROR( "Failed local batch job kill: %s" % outputLines[0] )
      if status != 0:
        message = "Unknown reason"
        if len( outputLines ) > 1:
          message = outputLines[1]
        return S_ERROR( 'Failed job kill, reason: %s' % message )      
    else:
      return S_ERROR( '\n'.join( [sshStdout, sshStderr] ) )
    
    return S_OK()

  def _getHostStatus( self, host = None ):
    """ Get jobs running at a given host
    """
    ssh = SSH( host = host, parameters = self.ceParameters ) 
    cmd = "bash --login -c '%s/%s status_info %s %s %s %s'" % ( self.sharedArea, 
                                                             self.controlScript, 
                                                             self.infoArea,
                                                             self.workArea,
                                                             self.ceParameters['SSHUser'],
                                                             self.execQueue )

    result = ssh.sshCall( 10, cmd )
    if not result['OK']:
      return result
    
    sshStatus = result['Value'][0]
    sshStdout = result['Value'][1]
    sshStderr = result['Value'][2]
    
    # Examine results of the job submission
    resultDict = {}
    if sshStatus == 0:
      outputLines = sshStdout.strip().replace('\r','').split('\n')
      try:
        index = outputLines.index('============= Start output ===============')
        outputLines = outputLines[index+1:]
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
      else:
        for line in outputLines[1:]:          
          if ':::' in line:
            jobStatus, nJobs = line.split( ':::' )
            resultDict[jobStatus] = int( nJobs )    
    else:
      return S_ERROR( '\n'.join( [sshStdout, sshStderr] ) )
    
    return S_OK( resultDict )

  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    resultHost = self._getHostStatus()
    if not resultHost['OK']:
      return resultHost
    
    result['RunningJobs'] = resultHost['Value'].get( 'Running', 0 )
    result['WaitingJobs'] = resultHost['Value'].get( 'Waiting', 0 )
    self.log.verbose( 'Waiting Jobs: ', result['WaitingJobs'] )
    self.log.verbose( 'Running Jobs: ', result['RunningJobs'] )

    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    return self._getJobStatusOnHost( jobIDList )

  def _getJobStatusOnHost( self, jobIDList, host = None ):
    """ Get the status information for the given list of jobs
    """
#    self.log.verbose( '*** getUnitJobStatus %s - %s\n' % ( jobIDList, host) )

    resultDict = {}
    ssh = SSH( host = host, parameters = self.ceParameters )
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

    cmd = "bash --login -c '%s/%s job_status %s %s %s'" % ( self.sharedArea, 
                                                         self.controlScript, 
                                                         '#'.join( stampList ), 
                                                         self.infoArea,
                                                         self.ceParameters['SSHUser'] )    
    result = ssh.sshCall( 30, cmd )
    if not result['OK']:
      return result
    sshStatus = result['Value'][0]
    sshStdout = result['Value'][1]
    sshStderr = result['Value'][2]

    if sshStatus == 0:
      outputLines = sshStdout.strip().replace('\r','').split('\n')
      try:
        index = outputLines.index('============= Start output ===============')
        outputLines = outputLines[index+1:]
      except:
        return S_ERROR( "Invalid output from job get status: %s" % outputLines[0] )  
      try:
        status = int( outputLines[0] )
      except:
        return S_ERROR( "Failed local batch job status: %s" % outputLines[0] )
      if status != 0:
        message = "Unknown reason"
        if len( outputLines ) > 1:
          message = outputLines[1]
        return S_ERROR( 'Failed job kill, reason: %s' % message )      
      else:
        for line in outputLines[1:]:
          jbundle = line.split( ':::' )
          if ( len( jbundle ) == 2 ):
            resultDict[jobDict[jbundle[0]]] = jbundle[1]
    else:
      return S_ERROR( '\n'.join( [sshStdout, sshStderr] ) )

#    self.log.verbose( ' !!! getUnitJobStatus will return : %s\n' % resultDict )
    return S_OK( resultDict )

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

    return S_OK( (jobStamp, host, output, error) )

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    result = self._getJobOutputFiles(jobID)
    if not result['OK']:
      return result
    
    jobStamp, host, outputFile, errorFile = result['Value']
    
    self.log.verbose( 'Getting output for jobID %s' % jobID )

    if not localDir:
      tempDir = tempfile.mkdtemp()
    else:
      tempDir = localDir

    ssh = SSH( parameters = self.ceParameters )
    result = ssh.scpCall( 20, '%s/%s.out' % ( tempDir, jobStamp ), '%s' % outputFile, upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.out' % ( tempDir, jobStamp ) ):
      os.system( 'touch %s/%s.out' % ( tempDir, jobStamp ) )
    result = ssh.scpCall( 20, '%s/%s.err' % ( tempDir, jobStamp ), '%s' % errorFile, upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.err' % ( tempDir, jobStamp ) ):
      os.system( 'touch %s/%s.err' % ( tempDir, jobStamp ) )

    # The result is OK, we can remove the output
    if self.removeOutput:
      result = ssh.sshCall( 10, 'rm -f %s/%s.out %s/%s.err' % ( self.batchOutput, jobStamp, self.batchError, jobStamp ) )

    if localDir:
      return S_OK( ( '%s/%s.out' % ( tempDir, jobStamp ), '%s/%s.err' % ( tempDir, jobStamp ) ) )
    else:
      # Return the output as a string
      outputFile = open( '%s/%s.out' % ( tempDir, jobStamp ), 'r' )
      output = outputFile.read()
      outputFile.close()
      outputFile = open( '%s/%s.err' % ( tempDir, jobStamp ), 'r' )
      error = outputFile.read()
      outputFile.close()
      shutil.rmtree( tempDir )
      return S_OK( ( output, error ) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
