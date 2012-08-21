########################################################################
# $HeadURL$
# File :   SSHGEComputingElement.py
# Author : A.T. V.H.
########################################################################

""" Grid Engine Computing Element with remote job submission via ssh/scp and using site
    shared area for the job proxy placement
"""

__RCSID__ = "092c1d9 (2011-06-02 15:20:46 +0200) atsareg <atsareg@in2p3.fr>"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Utilities.List                           import breakListIntoChunks
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC                                               import gConfig
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
from DIRAC.Resources.Computing.SSHComputingElement       import SSH 

import os, sys, time, re, socket, stat, shutil
import string, shutil, bz2, base64, tempfile

CE_NAME = 'SSHGE'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHGEComputingElement( ComputingElement ):

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
    # Now Torque specific ones
    if 'ExecQueue' not in self.ceParameters:
      self.ceParameters['ExecQueue'] = self.ceParameters.get( 'Queue', '' )

    if 'SharedArea' not in self.ceParameters:
      self.ceParameters['SharedArea'] = ''

    if 'BatchOutput' not in self.ceParameters:
      self.ceParameters['BatchOutput'] = os.path.join( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), 'data' )

    if 'BatchError' not in self.ceParameters:
      self.ceParameters['BatchError'] = os.path.join( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), 'data' )

    if 'ExecutableArea' not in self.ceParameters:
      self.ceParameters['ExecutableArea'] = os.path.join( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), 'data' )

    if 'GEEnv' not in self.ceParameters:
      self.ceParameters['GEEnv'] = self.ceParameters.get( 'GEEnv', '' )

  def reset( self ):
    self.queue = self.ceParameters['Queue']
    if 'ExecQueue' not in self.ceParameters or not self.ceParameters['ExecQueue']:
      self.ceParameters['ExecQueue'] = self.ceParameters.get( 'Queue', '' )
    self.execQueue = self.ceParameters['ExecQueue']
    if 'GEEnv' not in self.ceParameters or not self.ceParameters['GEEnv']:
      self.ceParameters['GEEnv'] = self.ceParameters.get( 'GEEnv', '' )
    self.geEnv = self.ceParameters['GEEnv']
    self.log.info( "Using queue: ", self.queue )
    self.hostname = socket.gethostname()
    self.sharedArea = self.ceParameters['SharedArea']
    self.batchOutput = self.ceParameters['BatchOutput']
    self.batchError = self.ceParameters['BatchError']
    self.executableArea = self.ceParameters['ExecutableArea']
    self.sshUser = self.ceParameters['SSHUser']
    self.sshHost = self.ceParameters['SSHHost']
    self.sshPassword = ''
    if 'SSHPassword' in self.ceParameters:
      self.sshPassword = self.ceParameters['SSHPassword']
    self.submitOptions = ''
    if 'SubmitOptions' in self.ceParameters:
      self.submitOptions = self.ceParameters['SubmitOptions']
    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower()  in ['no', 'false', '0']:
        self.removeOutput = False

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job
    """
    self.log.verbose( "Executable file path: %s" % executableFile )
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

      fd, name = tempfile.mkstemp( suffix = '_wrapper.py', prefix = 'SGE_', dir = os.getcwd() )
      wrapper = os.fdopen( fd, 'w' )
      wrapper.write( wrapperContent )
      wrapper.close()

      submitFile = name

    else: # no proxy
      submitFile = executableFile

    ssh = SSH( self.sshUser, self.sshHost, self.sshPassword )
    # Copy the executable
    os.chmod( submitFile, stat.S_IRUSR | stat.S_IXUSR )
    sFile = os.path.basename( submitFile )
    result = ssh.scpCall( 10, submitFile, '%s/%s' % ( self.executableArea, os.path.basename( submitFile ) ) )
    # submit submitFile to the batch system
    cmd = "source %(geEnv)s;i=0; while [ $i -lt %(numberOfJobs)d ]; do qsub -o %(output)s -e %(error)s -q %(queue)s -N DIRACPilot %(submitOptions)s %(executable)s; let i=i+1; done; rm -f %(executable)s" % \
      { 'geEnv': self.geEnv, \
       'numberOfJobs': numberOfJobs, \
       'output': self.batchOutput, \
       'error': self.batchError, \
       'queue': self.queue, \
       'submitOptions': self.submitOptions, \
       'executable': '%s/%s' % ( self.executableArea, os.path.basename( submitFile ) ) }

    self.log.verbose( 'CE submission command: %s' % ( cmd ) )

    result = ssh.sshCall( 10, cmd )

    if not result['OK'] or result['Value'][0] != 0:
      self.log.warn( '===========> SSHSGE CE result NOT OK' )
      self.log.debug( result )
      return S_ERROR( result['Value'] )
    else:
      self.log.debug( 'SGE CE result OK' )

    ### VANESSA 
    listJobs = []
    batchIDList = result['Value'][1].strip().replace( '\r', '' ).split( '\n' )
    for i in batchIDList:
      jobNum = string.split( i )[2]
      listJobs.append( jobNum )
    self.submittedJobs += 1
    self.log.debug( "************************ List of Jobs submitted: ************************" )
    self.log.debug( listJobs )
    return S_OK( listJobs )

  #############################################################################
  def getDynamicInfo( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs

    ssh = SSH( self.sshUser, self.sshHost, self.sshPassword )
    cmd1 = ( "source %s" ) % ( self.geEnv )
    cmd = [cmd1, ";" , "qstat"]
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
      self.log.error( 'Failed qstat execution:', stderr )
      return S_ERROR( stderr )
    waitingJobs = 0
    runningJobs = 0

    if len( stdout ):
      lines = stdout.split( '\n' )
      for line in lines:
        if not line.strip():
          continue
        sub = '--------------------'
        if sub in line:
          self.log.debug( "Line ---" )
        else:
          jobStatus = line.split()[4]
          if jobStatus in ['Tt', 'Tr']:
            doneJobs = 'Done'
          elif jobStatus in ['Rr', 'r']:
            runningJobs = runningJobs + 1
          elif jobStatus in ['qw', 'h']:
            waitingJobs = waitingJobs + 1


    result['WaitingJobs'] = waitingJobs
    result['RunningJobs'] = runningJobs

    self.log.verbose( 'Waiting Jobs: ', waitingJobs )
    self.log.verbose( 'Running Jobs: ', runningJobs )


    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    resultDict = {}
    ssh = SSH( self.sshUser, self.sshHost, self.sshPassword )
    for jobList in breakListIntoChunks( jobIDList, 100 ):
      jobDict = {}
      for job in jobList:
        jobNumber = job.split( '.' )[0]
        if jobNumber:
          jobDict[jobNumber] = job
      cmd = ( "source %s; qstat" ) % ( self.geEnv )
      result = ssh.sshCall( 10, cmd )
      if not result['OK']:
        return result

      output = result['Value'][1].replace( '\r', '' )
      lines = output.split( '\n' )
      for job in jobDict:
        resultDict[jobDict[job]] = 'Unknown'
        for line in lines:

          if line.find( job ) != -1:
            if line.find( 'Unknown' ) != -1:
              resultDict[jobDict[job]] = 'Unknown'
            else:
              torqueStatus = line.split()[4]
              if torqueStatus in ['Tt', 'Tr']:
                resultDict[jobDict[job]] = 'Done'
              elif torqueStatus in ['Rr', 'r']:
                resultDict[jobDict[job]] = 'Running'
              elif torqueStatus in ['qw', 'h']:
                resultDict[jobDict[job]] = 'Waiting'
          else:
            if resultDict[jobDict[job]] == 'Unknown':
              cmd = ( "ls -la  %s/*%s*" ) % ( self.batchOutput, job )
              result = ssh.sshCall( 10, cmd )
              subS = ( "No such file or directory" )
              if subS in result['Value']:
                self.log.debug ( "Output no ready" )
              else:
                resultDict[jobDict[job]] = 'Done'
            else:
              continue

    self.log.debug( "Result dict: " )
    self.log.debug( resultDict )
    return S_OK( resultDict )

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    jobNumber = jobID.split( '.' )[0]

    if not localDir:
      tempDir = tempfile.mkdtemp()
    else:
      tempDir = localDir
    ssh = SSH( self.sshUser, self.sshHost, self.sshPassword )
    result = ssh.scpCall( 20, '%s/%s.out' % ( tempDir, jobID ), '%s/*%s*' % ( self.batchOutput, jobNumber ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.out' % ( tempDir, jobID ) ):
      os.system( 'touch %s/%s.out' % ( tempDir, jobID ) )
    result = ssh.scpCall( 20, '%s/%s.err' % ( tempDir, jobID ), '%s/*%s*' % ( self.batchError, jobNumber ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.err' % ( tempDir, jobID ) ):
      os.system( 'touch %s/%s.err' % ( tempDir, jobID ) )
    # The result is OK, we can remove the output
    if self.removeOutput:
      result = ssh.sshCall( 10, 'rm -f %s/*%s* %s/*%s*' % ( self.batchOutput, jobNumber, self.batchError, jobNumber ) )
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

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
