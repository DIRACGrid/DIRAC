########################################################################
# $Id$
# File :   TorqueComputingElement.py
# Author : Stuart Paterson, Paul Szczypka
########################################################################

""" The simplest Computing Element instance that submits jobs locally.
"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall, systemCall
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import rootPath
from DIRAC                                               import gConfig

import os, re, socket
import shutil, bz2, base64, tempfile

CE_NAME = 'Torque'

UsedParameters = [ 'ExecQueue', 'SharedArea', 'BatchOutput', 'BatchError', 'UserName' ]
MandatoryParameters = [ 'Queue' ]

class TorqueComputingElement( ComputingElement ):
  """ Direct Torque submission
  """
  mandatoryParameters = MandatoryParameters

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )
    self.submittedJobs = 0

    self.queue = self.ceConfigDict['Queue']
    self.execQueue = self.ceConfigDict['ExecQueue']
    self.log.info( "Using queue: ", self.queue )
    self.hostname = socket.gethostname()
    self.sharedArea = self.ceConfigDict['SharedArea']
    self.batchOutput = self.ceConfigDict['BatchOutput']
    self.batchError = self.ceConfigDict['BatchError']
    self.userName = self.ceConfigDict['UserName']
    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower()  in ['no', 'false', '0']:
        self.removeOutput = False


  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )
    # Now Torque specific ones
    if 'ExecQueue' not in self.ceConfigDict:
      self.ceConfigDict['ExecQueue'] = self.ceConfigDict['Queue']

    if 'SharedArea' not in self.ceConfigDict:
      self.ceConfigDict['SharedArea'] = ''

    if 'UserName' not in self.ceConfigDict:
      self.ceConfigDict['UserName'] = ''

    if 'BatchOutput' not in self.ceConfigDict:
      self.ceConfigDict['BatchOutput'] = os.path.join( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), 'data' )

    if 'BatchError' not in self.ceConfigDict:
      self.ceConfigDict['BatchError'] = os.path.join( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), 'data' )

  #############################################################################
  def makeProxyExecutableFile( self, executableFile, proxy ):
    """ Make a single executable bundling together executableFile and proxy
    """
    compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy ) ).replace( '\n', '' )
    compressedAndEncodedExecutable = base64.encodestring( bz2.compress( open( executableFile, "rb" ).read(), 9 ) ).replace( '\n', '' )

    wrapperContent = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os, tempfile, sys, base64, bz2
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
cmd = "%(executable)s"
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

    return name


  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job, should be overridden in sub-class.
    """

    self.log.info( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0755 )

    #Perform any other actions from the site admin
    if self.ceParameters.has_key( 'AdminCommands' ):
      commands = self.ceParameters['AdminCommands'].split( ';' )
      for command in commands:
        self.log.verbose( 'Executing site admin command: %s' % command )
        result = shellCall( 30, command, callbackFunction = self.sendOutput )
        if not result['OK'] or result['Value'][0]:
          self.log.error( 'Error during "%s":' % command, result )
          return S_ERROR( 'Error executing %s CE AdminCommands' % CE_NAME )

    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
    if proxy:
      self.log.verbose( 'Setting up proxy for payload' )
      submitFile = self.makeProxyExecutableFile( executableFile, proxy )

    else: # no proxy
      submitFile = executableFile

    # submit submitFile to the batch system
    cmd = "qsub -o %(output)s -e %(error)s -q %(queue)s -N DIRACPilot %(executable)s" % \
      {'output': self.batchOutput, \
       'error': self.batchError, \
       'queue': self.queue, \
       'executable': os.path.abspath( submitFile ) }

    self.log.verbose( 'CE submission command: %s' % ( cmd ) )

    batchIDList = []
    for i in range( numberOfJobs ):

      result = shellCall( 30, cmd )
      if not result['OK'] or result['Value'][0]:
        self.log.warn( '===========>Torque CE result NOT OK' )
        self.log.debug( result )
        return S_ERROR( result['Value'] )
      else:
        self.log.debug( 'Torque CE result OK' )

      batchID = result['Value'][1].strip()
      batchIDList.append( batchID )

      self.submittedJobs += 1

    return S_OK( batchIDList )

  #############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs

    cmd = ["qstat", "-Q" , self.execQueue ]
    if self.userName:
      cmd = [ "qstat", "-u", self.userName, self.execQueue ]

    ret = systemCall( 10, cmd )

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

    if self.userName:
      # Parse qstat -u userName queueName
      runningJobs = 0
      waitingJobs = 0
      lines = stdout.replace( '\r', '' ).split( '\n' )
      for line in lines:
        if not line:
          continue
        if line.find( self.userName ) != -1:
          if 'R' == line.split( ' ' )[-2]:
            runningJobs += 1
          else:
            # every other status to assimilate to Waiting
            waitingJobs += 1
    else:
      # parse qstat -Q queueName
      matched = re.search( self.queue + "\D+(\d+)\D+(\d+)\W+(\w+)\W+(\w+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\W+(\w+)", stdout )
      if matched.groups < 6:
        return S_ERROR( "Error retrieving information from qstat:" + stdout + stderr )

      try:
        waitingJobs = int( matched.group( 5 ) )
        runningJobs = int( matched.group( 6 ) )
      except  ValueError:
        return S_ERROR( "Error retrieving information from qstat:" + stdout + stderr )

    result['WaitingJobs'] = waitingJobs
    result['RunningJobs'] = runningJobs

    self.log.verbose( 'Waiting Jobs: ', waitingJobs )
    self.log.verbose( 'Running Jobs: ', runningJobs )

    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    jobDict = {}
    for job in jobIDList:
      if not job:
        continue
      jobNumber = job.split( '.' )[0]
      jobDict[jobNumber] = job

    cmd = [ 'qstat' ] + jobIDList
    result = systemCall( 10, cmd )
    if not result['OK']:
      return result

    resultDict = {}
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

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    jobNumber = jobID.split( '.' )[0]
    # Find the output files
    outFile = ''
    outNames = os.listdir( self.batchOutput )
    for outName in outNames:
      if outName.find( jobNumber ) != -1:
        outFile = os.path.join( self.batchOutput, outName )
        break
    errFile = ''
    errNames = os.listdir( self.batchError )
    for errName in errNames:
      if errName.find( jobNumber ) != -1:
        errFile = os.path.join( self.batchError, errName )
        break

    if localDir:
      if outFile:
        doutFile = os.path.join( localDir, os.path.basename( outFile ) )
        shutil.copyfile( outFile, doutFile )
      if errFile:
        derrFile = os.path.join( localDir, os.path.basename( errFile ) )
        shutil.copyfile( errFile, derrFile )

    # The result is OK, we can remove the output
    if self.removeOutput:
      result = os.system( 'rm -f %s/*%s* %s/*%s*' % ( self.batchOutput, jobNumber, self.batchError, jobNumber ) )

    if localDir:
      if outFile and errFile:
        return S_OK( ( doutFile, derrFile ) )
      else:
        return S_ERROR( 'Output files not found' )
    else:
      # Return the output as a string
      output = ''
      error = ''
      if outFile:
        outputFile = open( outFile, 'r' )
        output = outputFile.read()
        outputFile.close()
      if errFile:
        outputFile = open( errFile, 'r' )
        error = outputFile.read()
        outputFile.close()

      return S_OK( ( output, error ) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
