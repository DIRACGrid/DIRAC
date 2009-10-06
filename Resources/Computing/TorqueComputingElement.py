########################################################################
# $Id: TorqueComputingElement.py,v 1.25 2009/10/06 16:12:07 ffeldhau Exp $
# File :   TorqueComputingElement.py
# Author : Stuart Paterson, Paul Szczypka
########################################################################

""" The simplest Computing Element instance that submits jobs locally.
"""

__RCSID__ = "$Id: TorqueComputingElement.py,v 1.25 2009/10/06 16:12:07 ffeldhau Exp $"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK,S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC                                               import gConfig
from DIRAC.Core.Security.Misc                            import getProxyInfo

import os,sys, time, re, socket
import string, shutil, bz2, base64, tempfile

CE_NAME = 'Torque'

UsedParameters      = [ 'ExecQueue', 'SharedArea', 'BatchOutput', 'BatchError' ]
MandatoryParameters = [ 'Queue' ]

class TorqueComputingElement( ComputingElement ):

  mandatoryParameters = MandatoryParameters

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )
    self.submittedJobs = 0

    self.queue = self.ceConfigDict['Queue']
    self.execQueue = self.ceConfigDict['ExecQueue']
    self.log.info("Using queue: ", self.queue)
    self.hostname = socket.gethostname()
    self.sharedArea = self.ceConfigDict['SharedArea']
    self.batchOutput = self.ceConfigDict['BatchOutput']
    self.batchError = self.ceConfigDict['BatchError']

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

    if 'BatchOutput' not in self.ceConfigDict:
      self.ceConfigDict['BatchOutput'] = os.path.join(rootPath, 'data' )

    if 'BatchError' not in self.ceConfigDict:
      self.ceConfigDict['BatchError'] = os.path.join(rootPath, 'data' )


  #############################################################################
  def submitJob(self,executableFile,jdl,proxy,localID):
    """ Method to submit job, should be overridden in sub-class.
    """

    self.log.info("Executable file path: %s" %executableFile)
    if not os.access(executableFile, 5):
      os.chmod(executableFile,0755)

    #Perform any other actions from the site admin
    if self.ceParameters.has_key('AdminCommands'):
      commands = self.ceParameters['AdminCommands'].split(';')
      for command in commands:
        self.log.verbose('Executing site admin command: %s' %command)
        result = shellCall(0,command,callbackFunction=self.sendOutput)
        if not result['OK'] or result['Value'][0]:
          self.log.error('Error during "%s":' %command,result)
          return S_ERROR('Error executing %s CE AdminCommands' %CE_NAME)

    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
    if proxy:
      self.log.verbose('Setting up proxy for payload')

      compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy ) ).replace('\n','')
      compressedAndEncodedExecutable = base64.encodestring( bz2.compress( open( executableFile, "rb" ).read(), 9 ) ).replace('\n','')

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
        'executable': os.path.basename(executableFile) }

      fd, name = tempfile.mkstemp( suffix = '_wrapper.py', prefix = 'TORQUE_', dir=os.getcwd())
      wrapper = os.fdopen(fd, 'w')
      wrapper.write( wrapperContent )
      wrapper.close()

      submitFile = name

    else: # no proxy
      submitFile = executableFile

    # submit submitFile to the batch system
    cmd = "qsub -o %(output)s -e %(error)s -q %(queue)s -N DIRACPilot %(executable)s" % \
      {'output': self.batchOutput, \
       'error': self.batchError, \
       'queue': self.queue, \
       'executable': os.path.abspath( submitFile ) }

    self.log.verbose('CE submission command: %s' %(cmd))

    result = shellCall(0,cmd, callbackFunction = self.sendOutput)
    if not result['OK'] or result['Value'][0]:
      self.log.warn('===========>Torque CE result NOT OK')
      self.log.debug(result)
      return S_ERROR(result['Value'])
    else:
      self.log.debug('Torque CE result OK')

    self.submittedJobs += 1
    return S_OK(localID)

  #############################################################################
  def getDynamicInfo(self):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs

    cmd = ["qstat", "-Q" , self.execQueue ]

    ret = systemCall( 10, cmd )

    if not ret['OK']:
      self.log.error( 'Timeout', ret['Message'])
      return ret

    status = ret['Value'][0]
    stdout = ret['Value'][1]
    stderr = ret['Value'][2]

    self.log.debug("status:", status)
    self.log.debug("stdout:", stdout)
    self.log.debug("stderr:", stderr)

    if status:
      self.log.error( 'Failed qstat execution:', stderr )
      return S_ERROR( stderr )

    matched = re.search(self.queue + "\D+(\d+)\D+(\d+)\W+(\w+)\W+(\w+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\W+(\w+)", stdout)

    if matched.groups < 6:
      return S_ERROR("Error retrieving information from qstat:" + stdout + stderr)

    try:
      waitingJobs = int(matched.group(5))
      runningJobs = int(matched.group(6))
    except:
      return S_ERROR("Error retrieving information from qstat:" + stdout + stderr)

    result['WaitingJobs'] = waitingJobs
    result['RunningJobs'] = runningJobs

    self.log.verbose('Waiting Jobs: ', waitingJobs )
    self.log.verbose('Running Jobs: ', runningJobs )

    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
