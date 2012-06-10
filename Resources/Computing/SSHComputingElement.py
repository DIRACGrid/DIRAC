########################################################################
# $HeadURL$
# File :   SSHComputingElement.py
# Author : Dumitru Laurentiu
########################################################################

""" SSH (Virtual) Computing Element: For a given list of ip/cores pair it will send jobs
    directly through ssh
""" 

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Utilities.List                           import breakListIntoChunks
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC                                               import gConfig
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo

import os, sys, time, re, socket, stat, shutil
import string, shutil, bz2, base64, tempfile, random

CE_NAME = 'SSH'
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
    else:
      # Try passwordless login
      result = shellCall( timeout, command )
#      print ( "!!! SSH command: %s returned %s\n" % (command, result) )
      if result['Value'][0] == 255:
        return S_ERROR ( (-1, 'Cannot connect to host %s' % self.host, '') )
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


class SSHComputingElement( ComputingElement ):

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
    self.sshScript = os.path.join( rootPath,"DIRAC", "Resources", "Computing", "scripts", "sshce" )
    if 'ExecQueue' not in self.ceParameters or not self.ceParameters['ExecQueue']:
      self.ceParameters['ExecQueue'] = self.ceParameters.get( 'Queue', '' )
    self.execQueue = self.ceParameters['ExecQueue']
    self.log.info( "Using queue: ", self.queue )
    self.hostname = socket.gethostname()
    self.sharedArea = self.ceParameters['SharedArea']
    self.batchOutput = self.ceParameters['BatchOutput']
    self.batchError = self.ceParameters['BatchError']
    self.infoArea = self.ceParameters['InfoArea']
    self.executableArea = self.ceParameters['ExecutableArea']
    self.sshUser = self.ceParameters['SSHUser']
    self.sshPassword = ''
    if 'SSHPassword' in self.ceParameters:
      self.sshPassword = self.ceParameters['SSHPassword']
    self.sshHost = []
    
    for host in self.ceParameters['SSHHost'].strip().split(','):
      self.sshHost.append(host.strip())
      self.log.verbose('Registered host:%s; uploading script.' % host.strip())
      ssh = SSH( self.sshUser, host.strip().split("/")[0], self.sshPassword )
      result = ssh.scpCall( 10, self.sshScript, '' )      
      if ( result['OK'] ):
        ssh.sshCall(10, "chmod +x ~/sshce; mkdir -p %s; mkdir -p %s" % ( self.infoArea, self.executableArea ) )
    

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
    max=0;    
    best="N/A";
    
    for host in self.sshHost:
      thost=host.split("/")
      runningJobs=self.getUnitDynamicInfo(thost[0])
      
      if (runningJobs >= 0):        
        if ( max <= int(thost[1]) - int(runningJobs) ):
          max=int(thost[1]) - int(runningJobs)
          best=thost[0];     

    if best == "N/A":
      return S_ERROR( "No online node found on queue" )
    
    return self.submitJobReal ( best, executableFile, proxy, numberOfJobs )
    
    
  def submitJobReal( self, host, executableFile, proxy, numberOfJobs = 1 ):
    
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

    ssh = SSH( self.sshUser, host, self.sshPassword )
    # Copy the executable
    os.chmod( submitFile, stat.S_IRUSR | stat.S_IXUSR )
    sFile = os.path.basename( submitFile )
    result = ssh.scpCall( 10, submitFile, '%s/%s' % ( self.executableArea, os.path.basename( submitFile ) ) )
    
#    self.log.verbose( '***%s %s %s SCP: %s, %s/%s : %s\n' % (  self.sshUser, host, self.sshPassword, submitFile, self.executableArea, os.path.basename( submitFile ), result ) )
    
    # submit submitFile directly to host
    # rm la final
    rnd = random.randint(200,5000)
    cmd = "~/sshce run_job %s/%s %s_%s %s" % \
	(  self.executableArea, os.path.basename( submitFile ), host, rnd, self.infoArea )

#    self.log.verbose( '*** CE submission command: %s\n' %  cmd )

    result = ssh.sshCall( 10, cmd )
    
#    if not result['OK'] or result['Value'][0] != 0:
    if not result['OK']:
      self.log.warn( '===========> SSH CE result NOT OK' )
      self.log.debug( result )
      return S_ERROR( result['Value'] )
    else:
      self.log.debug( 'SSH CE result OK' )
      
#    self.log.verbose ( ' **** ce.submitjob, sshCall returned : %s\n' % result )

    stampDict = {}

    batchID = result['Value'][1].strip().replace( '\r', '' ).split( '\n' )
    stampDict[batchID[0]] = '%s_%s' % (host, rnd) 
    
    result = S_OK ( batchID )
    result['PilotStampDict'] = stampDict

    self.submittedJobs += 1

    return result

  #############################################################################
  def getUnitDynamicInfo( self, hostAddress ):
    ssh = SSH( self.sshUser, hostAddress, self.sshPassword )
    cmd = ["~/sshce dynamic_info %s"  % self.infoArea ]
    ret = ssh.sshCall( 10, cmd )

    if not ret['OK']:
      self.log.error( 'Timeout', ret['Message'] )
      return -1

    return ret['Value'][1]
    
    
  def getDynamicInfo( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    for host in self.sshHost:
      thost=host.split("/")

      runningJobs = self.getUnitDynamicInfo(thost[0])
      if (runningJobs > -1):
        result['RunningJobs'] += int(runningJobs)

    self.log.verbose( 'Waiting Jobs: ', 0 )
    self.log.verbose( 'Running Jobs: ', result['RunningJobs'] )

    return result

  def getJobStatus (self, jobIDList):
    wnDict={}
    
    self.log.verbose ( 'getJobStatus (jobIDList) ; jobIDList = %s' % jobIDList )
    for job in jobIDList:
      jobStamp=job.split(':::')[1]
      if not jobStamp.split('_')[0] in wnDict:
        wnDict[jobStamp.split('_')[0]]=[]
      wnDict[jobStamp.split('_')[0]].append(jobStamp)

#    print (' !! wnDict : %s \n    ' % wnDict )
    resultDict={}
        
    for elem in wnDict:        
 
      tmpDict=self.getUnitJobStatus( wnDict[elem], elem )['Value']
    #  self.log.verbose(' !!!! getUnitJobStatus(%s, %s) => %s\n' % (wnDict[elem],elem, tmpDict) )
      for item in tmpDict:
        resultDict[item]=tmpDict[item]
    #    self.log.verbose(' !!!! resultDict[%s]= tmpDict[%s] ; tmpdict value: (%s)\n' % ( item, item,tmpDict[item]) )
      
#    self.log.verbose(' !!! getJobStatus will return: %s\n' % S_OK ( resultDict ) )  
    return S_OK( resultDict )
    
  def getUnitJobStatus( self, jobIDList, host ):
    """ Get the status information for the given list of jobs
    """
#    self.log.verbose( '*** getUnitJobStatus %s - %s\n' % ( jobIDList, host) )
    
    resultDict = {}
    ssh = SSH( self.sshUser, host, self.sshPassword )
    
    cmd = [ '~/sshce job_status %s' % self.infoArea, '#'.join(jobIDList) ]
    result = ssh.sshCall( 10, cmd )

    if not result['OK']:
      return result

    output = result['Value'][1].strip().replace( '\r', '' )
#    self.log.verbose ( ' **** ce.getUnitJobStatus, output : %s\n' % output )  

    lines = output.split( '#' )
    for line in lines:
      jbundle=line.split('-')
      if ( len(jbundle) == 2 ):
        if jbundle[1] in ['D']:
          resultDict[jbundle[0]] = 'Done'
        elif jbundle[1] in ['R']:
          resultDict[jbundle[0]] = 'Running'
      #self.log.verbose ( ' **** ce.getUnitJobStatus %s, job %s status %s\n %s' % ( host, jbundle[0], resultDict[jbundle[0]], resultDict ) )                  
    
#    self.log.verbose( ' !!! getUnitJobStatus will return : %s\n' % resultDict )
    return S_OK( resultDict )

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    jobPid = jobID.split( ':::' )[0]
    jobStamp = jobID.split( ':::' )[1]
    self.log.verbose( '!! getJobOutput, jobStamp=%s  jobID %s\n' % (jobStamp, jobID) )
    
    host = jobStamp.split( '_' )[0]
    
    if not localDir:
      tempDir = tempfile.mkdtemp()
    else:
      tempDir = localDir

    ssh = SSH( self.sshUser, host, self.sshPassword )
    result = ssh.scpCall( 20, '%s/%s.out' % ( tempDir, jobStamp ), '%s/%s/std.out' % ( self.infoArea, jobStamp ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.out' % ( tempDir, jobStamp ) ):
      os.system( 'touch %s/%s.out' % ( tempDir, jobStamp ) )
    result = ssh.scpCall( 20, '%s/%s.err' % ( tempDir, jobStamp ), '%s/%s/std.err' % ( self.infoArea, jobStamp ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.err' % ( tempDir, jobStamp ) ):
      os.system( 'touch %s/%s.err' % ( tempDir, jobStamp ) )

    # The result is OK, we can remove the output
    if self.removeOutput:
      result = ssh.sshCall( 10, 'rm -f %s/*%s* %s/*%s*' % ( self.batchOutput, jobNumber, self.batchError, jobNumber ) )

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
