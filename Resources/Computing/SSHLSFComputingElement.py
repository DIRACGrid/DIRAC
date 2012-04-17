########################################################################
# $HeadURL$
# File :   SSHLSFComputingElement.py
# Author : S.P.
########################################################################

""" LSF Computing Element with remote job submission via ssh/scp and using site
    shared area for the job proxy placement    

    To get the proxy right, don't forget to add in the CS that for this kind of CE, the BundleProxy must be True (default False)

    Do not use yet as some cleanup of submission scripts is not done, as well as the cleanup of outputs.

"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Utilities.List                           import breakListIntoChunks
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC                                               import gConfig
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo

import os, sys, time, re, socket, stat, shutil
import string, shutil, bz2, base64, tempfile

CE_NAME = 'SSHLSF'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSH:

  def __init__( self, user, host, password = None ):

    self.user = user
    self.host = host
    self.password = password

  def __ssh_call( self, command, timeout ):

    try:
      import pexpect
      expectFlag = True
    except:
      from DIRAC import shellCall
      expectFlag = False

    if not timeout:
      timeout = 999

    if expectFlag:
      ssh_newkey = 'Are you sure you want to continue connecting'
      try:
        child = pexpect.spawn( command, timeout = timeout )

        i = child.expect( [pexpect.TIMEOUT, ssh_newkey, pexpect.EOF, 'password: '] )
        if i == 0: # Timeout
            return S_OK( ( -1, child.before, 'SSH login failed' ) )
        elif i == 1: # SSH does not have the public key. Just accept it.
          child.sendline ( 'yes' )
          child.expect ( 'password: ' )
          i = child.expect( [pexpect.TIMEOUT, 'password: '] )
          if i == 0: # Timeout
            return S_OK( ( -1, child.before + child.after, 'SSH login failed' ) )
          elif i == 1:
            child.sendline( password )
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
          return S_ERROR( ( -1, child.before, '' ) )
      except Exception,x:
        res = (-1 ,'Encountered exception %s: %s'%(Exception,str(x)))
        return S_ERROR(res)
    else:
      # Try passwordless login
      result = shellCall( timeout, command )
      return result


  def sshCall( self, timeout, cmdSeq ):
    """ Execute remote command via a ssh remote call
    """

    command = cmdSeq
    if type( cmdSeq ) == type( [] ):
      command = ' '.join( cmdSeq )

    command = "ssh -l %s %s '%s'" % ( self.user, self.host, command )
    return self.__ssh_call( command, timeout )

  def scpCall( self, timeout, localFile, destinationPath, upload = True ):
    """ Execute scp copy
    """

    if upload:
      command = "scp %s %s@%s:%s" % ( localFile, self.user, self.host, destinationPath )
    else:
      command = "scp %s@%s:%s %s" % ( self.user, self.host, destinationPath, localFile )
    return self.__ssh_call( command, timeout )


class SSHLSFComputingElement( ComputingElement ):

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


  def reset( self ):

    self.queue = self.ceParameters['Queue']
    if 'ExecQueue' not in self.ceParameters or not self.ceParameters['ExecQueue']:
      self.ceParameters['ExecQueue'] = self.ceParameters.get( 'Queue', '' )
    self.execQueue = self.ceParameters['ExecQueue']
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
  workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'LSF_' )
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

      fd, name = tempfile.mkstemp( suffix = '_wrapper.py', prefix = 'LSF_', dir = os.getcwd() )
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
    result = ssh.scpCall( 100, submitFile, '%s/%s' % ( self.executableArea, os.path.basename( submitFile ) ) )
    # submit submitFile to the batch system
    cmd = "i=0; while [ $i -lt %(numberOfJobs)d ]; do bsub -o %(output)s -e %(error)s -q %(queue)s -J DIRACPilot %(submitOptions)s %(executable)s; let i=i+1; done;" % \
      {'numberOfJobs': numberOfJobs, \
       'output': self.batchOutput, \
       'error': self.batchError, \
       'queue': self.queue, \
       'submitOptions': self.submitOptions, \
       'executable': '%s/%s' % ( self.executableArea, os.path.basename( submitFile ) ) }

    self.log.verbose( 'CE submission command: %s' % ( cmd ) )

    result = ssh.sshCall( 100, cmd )
<<<<<<< HEAD
=======

>>>>>>> Working version of the LSF CE module. Some cleanup or the submitted scripts needed
    if not result['OK'] or result['Value'][0] != 0:
      self.log.warn( '===========> SSHLSF CE result NOT OK' )
      self.log.debug( result )
      return S_ERROR( result['Value'] )
    else:
      self.log.debug( 'LSF CE result OK' )

    batchIDList = []
    lines = result['Value'][1].strip().replace( '\r', '' ).split( '\n' )
    for line in lines:
      batchIDList.append(line.split("<")[1].split(">")[0])
<<<<<<< HEAD
=======
      

>>>>>>> Working version of the LSF CE module. Some cleanup or the submitted scripts needed
    self.submittedJobs += 1

    return S_OK( batchIDList )

  #############################################################################
  def getDynamicInfo( self ):
    """ Method to return information on running and pending jobs.
    """

    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs

    ssh = SSH( self.sshUser, self.sshHost, self.sshPassword )
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
    lines = stdout.split("\n")
    for line in lines:
      if line.count("PEND") or line.count('PSUSP'):
        waitingJobs += 1
      if line.count("RUN") or line.count('USUSP'):
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
    ssh = SSH( self.sshUser, self.sshHost, self.sshPassword )
    
    for jobList in breakListIntoChunks(jobIDList,100):
      jobDict = {}
      for job in jobList:
        jobNumber = job.split('.')[0]
        if jobNumber:
          jobDict[jobNumber] = job
      
      cmd = [ 'bjobs', ' '.join( jobList ) ]
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
              elif lsfStatus in ['RUN','SSUSP']:
                resultDict[jobDict[job]] = 'Running'
              elif lsfStatus in ['PEND','PSUSP']:
                resultDict[jobDict[job]] = 'Waiting'

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
    result = ssh.scpCall( 200, '%s/%s.out' % ( tempDir, jobID ), '%s/*%s*' % ( self.batchOutput, jobNumber ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.out' % ( tempDir, jobID ) ):
      os.system( 'touch %s/%s.out' % ( tempDir, jobID ) )
    result = ssh.scpCall( 200, '%s/%s.err' % ( tempDir, jobID ), '%s/*%s*' % ( self.batchError, jobNumber ), upload = False )
<<<<<<< HEAD

=======
>>>>>>> Working version of the LSF CE module. Some cleanup or the submitted scripts needed
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.err' % ( tempDir, jobID ) ):
      os.system( 'touch %s/%s.err' % ( tempDir, jobID ) )

    # The result is OK, we can remove the output
    if self.removeOutput:
      result = ssh.sshCall( 100, 'rm -f %s/*%s* %s/*%s*' % ( self.batchOutput, jobNumber, self.batchError, jobNumber ) )

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
