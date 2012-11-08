########################################################################
# $HeadURL$
# File : CondorComputingElement.py
# Author : A.Tsaregorodtsev
########################################################################

import os, stat, tempfile, shutil

from DIRAC.Resources.Computing.ComputingElement     import ComputingElement
from DIRAC.Resources.Computing.SSHComputingElement  import SSH 
from DIRAC import rootPath, S_OK, S_ERROR

CE_NAME = 'SSHCondor'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHCondorComputingElement(ComputingElement):
       
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.submittedJobs = 0
    self.mandatoryParameters = MANDATORY_PARAMETERS     
       
  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )

    if 'ExecQueue' not in self.ceParameters:
      self.ceParameters['ExecQueue'] = self.ceParameters.get( 'Queue', '' )

    if 'SharedArea' not in self.ceParameters:
      self.ceParameters['SharedArea'] = ''

    if 'BatchOutput' not in self.ceParameters:
      self.ceParameters['BatchOutput'] = 'data' 

    if 'BatchError' not in self.ceParameters:
      self.ceParameters['BatchError'] = 'data' 

    if 'ExecutableArea' not in self.ceParameters:
      self.ceParameters['ExecutableArea'] = 'data' 

    if 'InfoArea' not in self.ceParameters:
      self.ceParameters['InfoArea'] = 'info'

  def reset( self ):

    self.queue = self.ceParameters['Queue']
    self.sshScript = os.path.join( rootPath, "DIRAC", "Resources", "Computing", "remote_scripts", "condorce" )
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
      
    self.sshHost = self.ceParameters['SSHHost']
    self.log.verbose( 'Uploading condorce script to %s' % self.sshHost )
    ssh = SSH( host = self.sshHost, parameters = self.ceParameters )
    result = ssh.scpCall( 10, self.sshScript, self.sharedArea )
    if not result['OK']:
      self.log.warn( 'Failed uploading script: %s' % result['Message'][1] )
      
    self.log.verbose( 'Creating working directories on %s' % self.sshHost )
    ssh.sshCall( 10, "chmod +x %s/condorce; mkdir -p %s" % ( self.sharedArea, self.executableArea ) )
    if not result['OK']:
      self.log.warn( 'Failed creating working directories: %s' % result['Message'][1] )

    self.submitOptions = ''
    if 'SubmitOptions' in self.ceParameters:
      self.submitOptions = self.ceParameters['SubmitOptions']
    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower()  in ['no', 'false', '0']:
        self.removeOutput = False     
       
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

      compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace( '\n', '' )
      compressedAndEncodedExecutable = base64.encodestring( bz2.compress( open( executableFile, "rb" ).read(), 9 ) ).replace( '\n', '' )

      wrapperContent = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os, tempfile, sys, base64, bz2, shutil
try:
  workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'TORQUE_' )
  os.chdir( workingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedProxy)s" ) ) )
  open( '%(executable)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedExecutable)s" ) ) )
  os.chmod('proxy',0600)
  os.chmod('%(executable)s',0700)
  os.environ["X509_USER_PROXY"]=os.path.join(workingDirectory, 'proxy')
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "./%(executable)s"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )

shutil.rmtree( workingDirectory )

""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy, \
        'compressedAndEncodedExecutable': compressedAndEncodedExecutable, \
        'executable': os.path.basename( executableFile ) }

      fd, name = tempfile.mkstemp( suffix = '_wrapper.py', prefix = 'NOTORQUE_', dir = os.getcwd() )
      wrapper = os.fdopen( fd, 'w' )
      wrapper.write( wrapperContent )
      wrapper.close()

      submitFile = name

    else: # no proxy
      submitFile = executableFile

    ssh = SSH( parameters = self.ceParameters )
    # Copy the executable
    os.chmod( submitFile, stat.S_IRUSR | stat.S_IXUSR )
    sFile = os.path.basename( submitFile )
    result = ssh.scpCall( 10, submitFile, '%s/%s' % ( self.executableArea, os.path.basename( submitFile ) ) )

    # Submit jobs
    cmd = "bash --login -c '%s/condorce %s/%s %s %s'" % ( self.sharedArea, self.executableArea, os.path.basename( submitFile ), numberOfJobs, self.batchOutput )
    self.log.verbose( 'CE submission command: %s\n' %  cmd )
    ret = ssh.sshCall( 10, cmd )

    if not ret['OK']:
      self.log.error( 'SSHCondor CE job submission failed', result['Message'] )
      return result
    else:
      self.log.debug( 'SSHCondor CE job submission OK' )

    status = ret['Value'][0]
    stdout = ret['Value'][1]
    stderr = ret['Value'][2]
    if status == 0:
      nJobs,clusterID = stdout.strip().split()
      jobIDs = [ clusterID + '.' + str(j) for j in range( int(nJobs) ) ]    
      return S_OK( jobIDs )
    else:
      return S_ERROR( '\n'.join( [stdout,stderr] ) )
   
  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    jobIDs = list ( jobIDList )
    resultDict = {}
    ssh = SSH( parameters = self.ceParameters )
    cmd = ["bash --login -c 'condor_q -submitter %s'" % self.ceParameters["SSHUser"] ]
    ret = ssh.sshCall( 10, cmd )

    if not ret['OK']:
      self.log.error( 'Timeout', ret['Message'] )
      return ret

    status_q = ret['Value'][0]
    stdout_q = ret['Value'][1]
    stderr_q = ret['Value'][2]

    if status_q:
      self.log.error( 'Failed condor_q execution:', stderr )
      return S_ERROR( stderr )
    
    cmd = ["bash --login -c 'condor_history | grep %s'" % self.ceParameters["SSHUser"] ]
    ret = ssh.sshCall( 10, cmd )

    if not ret['OK']:
      self.log.error( 'Timeout', ret['Message'] )
      return ret

    status_history = ret['Value'][0]
    stdout_history = ret['Value'][1]
    stderr_history = ret['Value'][2]
    
    stdout = stdout_q
    if status_history == 0:
      stdout = '\n'.join( [stdout_q,stdout_history] )

    if len( stdout ):
      lines = stdout.split( '\n' )
      for line in lines:
        l = line.strip()
        for job in jobIDList:
          if l.startswith( job ):
            if " I " in line:
              resultDict[job] = 'Waiting'
            elif " R " in line:
              resultDict[job] = 'Running'
            elif " C " in line:
              resultDict[job] = 'Done'
            elif " X " in line:
              resultDict[job] = 'Aborted'    

    if len( resultDict ) == len( jobIDList ):
      return S_OK( resultDict )

    for job in jobIDList:
      if not job in resultDict:
        resultDict[job] = 'Unknown'
    return S_OK( resultDict )    
                
  def getDynamicInfo( self ):  
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    ssh = SSH( parameters = self.ceParameters )
    cmd = ["bash --login -c 'condor_q -submitter %s'" % self.ceParameters["SSHUser"] ]
    ret = ssh.sshCall( 10, cmd )

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
      self.log.error( 'Failed condor_q execution:', stderr )
      return S_ERROR( stderr )
    waitingJobs = 0
    runningJobs = 0

    if len( stdout ):
      lines = stdout.split( '\n' )
      for line in lines:
        if not line.strip():
          continue
        if " I " in line:
          waitingJobs += 1
        elif " R " in line:
          runningJobs += 1  

    result['SubmittedJobs'] = self.submittedJobs
    result['WaitingJobs'] = waitingJobs
    result['RunningJobs'] = runningJobs

    self.log.verbose( 'Waiting Jobs: ', waitingJobs )
    self.log.verbose( 'Running Jobs: ', runningJobs )

    return result
   
  def getJobOutput( self, jobID, localDir = None ):   
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    jobNumber = jobID

    if not localDir:
      tempDir = tempfile.mkdtemp()
    else:
      tempDir = localDir
    ssh = SSH( parameters = self.ceParameters )
    result = ssh.scpCall( 20, '%s/%s.out' % ( tempDir, jobID ), '%s/%s.out' % ( self.batchOutput, jobNumber ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.out' % ( tempDir, jobID ) ):
      os.system( 'touch %s/%s.out' % ( tempDir, jobID ) )
    result = ssh.scpCall( 20, '%s/%s.err' % ( tempDir, jobID ), '%s/%s.err' % ( self.batchOutput, jobNumber ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.err' % ( tempDir, jobID ) ):
      os.system( 'touch %s/%s.err' % ( tempDir, jobID ) )
    # The result is OK, we can remove the output
    if self.removeOutput:
      result = ssh.sshCall( 10, 'rm -f %s/%s*' % ( self.batchOutput, jobNumber ) )
    if localDir:
      return S_OK( ( '%s/%s.out' % ( tempDir, jobID ), '%s/%s.err' % ( tempDir, jobID ) ) )
    else:
      # Return the output as a string
      outputFile = open( '%s/%s.out' % ( tempDir, jobID ), 'r' )
      output = outputFile.read()
      outputFile.close()
      outputFile = open( '%s/%s.err' % ( tempDir, jobID ), 'r' )
      error = outputFile.read()
      outputFile.close()
      shutil.rmtree( tempDir )

      return S_OK( ( output, error ) )   
