########################################################################
# $HeadURL$
# File :   SSHTorqueComputingElement.py
# Author : A.T.
########################################################################

""" Torque Computing Element with remote job submission via ssh/scp and using site
    shared area for the job proxy placement
"""

__RCSID__ = "$Id$"
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Resources.Computing.SSHComputingElement       import SSH, SSHComputingElement 
from DIRAC.Core.Utilities.List                           import breakListIntoChunks
from DIRAC.Core.Utilities.Pfn                            import pfnparse
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import rootPath
from DIRAC                                               import gConfig

import os, re, socket, stat
import bz2, base64, tempfile

CE_NAME = 'SSHTorque'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHTorqueComputingElement( SSHComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'torquece'
    self.mandatoryParameters = MANDATORY_PARAMETERS

  #############################################################################
  def _addCEConfigDefaults_old( self ):
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


  def reset_old( self ):

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
    self.submitOptions = ''
    if 'SubmitOptions' in self.ceParameters:
      self.submitOptions = self.ceParameters['SubmitOptions']
    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower()  in ['no', 'false', '0']:
        self.removeOutput = False

  #############################################################################
  def submitJob_old( self, executableFile, proxy, numberOfJobs = 1 ):
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

      fd, name = tempfile.mkstemp( suffix = '_wrapper.py', prefix = 'TORQUE_', dir = os.getcwd() )
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
    # submit submitFile to the batch system
    cmd = "i=0; while [ $i -lt %(numberOfJobs)d ]; do qsub -o %(output)s -e %(error)s -q %(queue)s -N DIRACPilot %(submitOptions)s %(executable)s; let i=i+1; done; rm -f %(executable)s" % \
      {'numberOfJobs': numberOfJobs, \
       'output': self.batchOutput, \
       'error': self.batchError, \
       'queue': self.queue, \
       'submitOptions': self.submitOptions, \
       'executable': '%s/%s' % ( self.executableArea, os.path.basename( submitFile ) ) }

    self.log.verbose( 'CE submission command: %s' % ( cmd ) )

    result = ssh.sshCall( 10, cmd )

    if not result['OK'] or result['Value'][0] != 0:
      self.log.warn( '===========> SSHTorque CE result NOT OK' )
      self.log.debug( result )
      return S_ERROR( result['Value'] )
    else:
      self.log.debug( 'Torque CE result OK' )

    batchIDList = result['Value'][1].strip().replace( '\r', '' ).split( '\n' )

    self.submittedJobs += 1

    return S_OK( batchIDList )

  #############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """

    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs

    ssh = SSH( parameters = self.ceParameters )
    cmd = ["qstat", "-Q" , self.execQueue ]
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

    matched = re.search( self.queue + "\D+(\d+)\D+(\d+)\W+(\w+)\W+(\w+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\W+(\w+)", stdout )

    if matched.groups < 6:
      return S_ERROR( "Error retrieving information from qstat:" + stdout + stderr )

    try:
      waitingJobs = int( matched.group( 5 ) )
      runningJobs = int( matched.group( 6 ) )
    except:
      return S_ERROR( "Error retrieving information from qstat:" + stdout + stderr )

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
        if result['OK']:
          stamp = result['Value']['FileName'].split('.')[0] 
        else:
          self.log.error( 'Invalid job id', job )
          continue  
        jobDict[stamp] = job
      stampList = jobDict.keys() 

      cmd = [ 'qstat', ' '.join( stampList ) ]
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
              if torqueStatus in ['E', 'C']:
                resultDict[jobDict[job]] = 'Done'
              elif torqueStatus in ['R']:
                resultDict[jobDict[job]] = 'Running'
              elif torqueStatus in ['S', 'W', 'Q', 'H', 'T']:
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

    return S_OK( (jobStamp,host,output,error) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
