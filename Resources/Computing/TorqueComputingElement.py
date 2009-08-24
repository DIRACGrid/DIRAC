########################################################################
# $Id: TorqueComputingElement.py,v 1.20 2009/08/24 14:47:19 ffeldhau Exp $
# File :   TorqueComputingElement.py
# Author : Stuart Paterson, Paul Szczypka
########################################################################

""" The simplest Computing Element instance that submits jobs locally.
"""

__RCSID__ = "$Id: TorqueComputingElement.py,v 1.20 2009/08/24 14:47:19 ffeldhau Exp $"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK,S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC                                               import gConfig
from DIRAC.Core.Security.Misc                            import getProxyInfo

import os,sys, time, re, socket
import string, shutil, bz2, base64, tempfile

DIRAC_PILOT   = os.path.join( rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot' )
DIRAC_INSTALL = os.path.join( rootPath, 'scripts', 'dirac-install' )

CE_NAME = 'Torque'
QUEUE = 'batch'

class TorqueComputingElement(ComputingElement):

  #############################################################################
  def __init__(self):
    """ Standard constructor.
    """
    ComputingElement.__init__(self,CE_NAME)
    self.submittedJobs = 0
    self.queue = gConfig.getValue('/LocalSite/Queue', QUEUE)
    self.log.info("Using queue: ", self.queue)
    self.pilot = DIRAC_PILOT
    self.install = DIRAC_INSTALL
    self.hostname = socket.gethostname()
    self.sharedArea = gConfig.getValue('/LocalSite/SharedArea')
    self.batchOutput = gConfig.getValue('/LocalSite/BatchOutput', \
                                         os.path.join(rootPath, 'data' ))
    self.batchError = gConfig.getValue('/LocalSite/BatchError', \
                                         os.path.join(rootPath, 'data' ))
    
    self.hostname = socket.gethostname()

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
      
      compressedAndEncodedProxy = base64.encode( bz2.compress( proxy ) )
      compressedAndEncodedExecutable = base64.encode( bz2.compress( open( executableFile, "rb" ).read(), 9 ) )
      
      wrapperContent = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os, tempfile, sys, base64, bz2
try:
  workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'TORQUE_' )
  os.chdir( workingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.decode( "%(compressedAndEncodedProxy)s" ) ) )
  open( '%(executable)s', "w" ).write(bz2.decompress( base64.decode( "%(compressedAndEncodedExecutable)s" ) ) )
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
    cmd = "qsub -o %(output)s -e %(error)s -q %(queue)s %(executable)s" % \
      {'output': self.batchOutput, \
       'error': self.batchError, \
       'queue': self.queue, \
       'executable': os.path.abspath( submitFile ) }
    
    self.log.verbose('CE submission command: %s' %(cmd))

    result = shellCall(0,cmd, callbackFunction = self.sendOutput)
    if not result['OK'] or result['Value'][0]:
      self.log.warn('===========>Torque CE result NOT OK')
      self.log.debug(result)
      return result
    else:
      self.log.debug('Torque CE result OK')

    self.submittedJobs += 1
    return S_OK(localID)

  #############################################################################
  def getDynamicInfo(self):
    """ Method to return information on running and pending jobs.
    """
    result = {}
    result['SubmittedJobs'] = self.submittedJobs
    
    cmd = ["qstat", "-Q" , self.queue ]
    
    ret = systemCall( 120, cmd )
    
    stdout = ret['Value'][1]
    stderr = ret['Value'][2]
    
    self.log.debug("stdout", stdout)
    
    matched = re.search(self.queue + "\D+(\d+)\D+(\d+)\W+(\w+)\W+(\w+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\W+(\w+)", stdout)
    
    if matched.groups < 6:
      return S_ERROR("Error retrieving information from qstat:" + stdout + stderr)
    
    result['WaitingJobs'] = matched.group(5)
    result['RunningJobs'] = matched.group(6)
    self.log.verbose('Waiting Jobs: ', matched.group(5))
    self.log.verbose('Running Jobs: ', matched.group(6))
    return S_OK(result)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
