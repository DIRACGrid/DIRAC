########################################################################
# $Id: TorqueComputingElement.py,v 1.9 2009/07/31 20:14:53 ffeldhau Exp $
# File :   TorqueComputingElement.py
# Author : Stuart Paterson, Paul Szczypka
########################################################################

""" The simplest Computing Element instance that submits jobs locally.
"""

__RCSID__ = "$Id: TorqueComputingElement.py,v 1.9 2009/07/31 20:14:53 ffeldhau Exp $"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK,S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC.Core.Security.Misc                            import getProxyInfo

import os,sys, time, re, socket
import string

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
    self.queue = QUEUE
    self.pilot = DIRAC_PILOT
    self.install = DIRAC_INSTALL
    self.hostname = socket.gethostname()
    self.sharedArea = gConfig.getValue('/LocalSite/SharedArea')

  #############################################################################
  def submitJob(self,executableFile,jdl,proxy,localID):
    """ Method to submit job, should be overridden in sub-class.
    """
    self.log.verbose('Setting up proxy for payload')
    result = self.writeProxyToFile(proxy)
    if not result['OK']:
      return result

    proxyLocation = result['Value']
    self.log.info("Executable file path: %s" %executableFile)
    if not os.access(executableFile, 5):
      os.chmod(executableFile,0755)

    # The little script which sets the dirac-python version explicitly poses some problems.
    # specifically with newline characters which need to be stripped
    # hence the strip
    fopen = open(executableFile,'r')
    contents = fopen.read()
    fopen.close()

    # Get the proxy of the user who submitted the job:
    fopen = open(proxyLocation,'r')
    proxyString = fopen.read()
    fopen.close()

    # create and write the executable file run###.py
    executableFileBaseName=os.path.basename(executableFile)
    fopen = open('run%s.py' %executableFileBaseName,'w')
    fopen.write('#!/usr/bin/env python\n')
    fopen.write('#PBS -W stagein=%s@%s:%s\n' % (os.path.basename(self.pilot), self.hostname, self.pilot ) )
    fopen.write('#PBS -W stagein=%s@%s:%s\n' % (os.path.basename(self.install), self.hostname, self.install ) )
    fopen.write('import os\n')
    fopen.write('fopen = open("%s","w")\n' %executableFileBaseName)
    fopen.write('fopen.write("""%s""")\n' %contents)
    fopen.write("fopen.close()\n")
    fopen.write('os.chmod("%s",0755)\n'%executableFileBaseName)
    fopen.write('fopen = open("%s","w")\n' %proxyLocation)
#    fopen.write('fopen.write("""%s""")\n' %proxy)
    fopen.write('fopen.write("""%s""")\n' %proxyString)
    fopen.write('fopen.close()\n')
    fopen.write('os.chmod("%s",0600)\n' %proxyLocation)
    fopen.write('os.environ["X509_USER_PROXY"]="%s"\n' %proxyLocation)
    # temporary fix for CAs
    fopen.write('os.environ["X509_CERT_DIR"]="%s/certificates"\n' %self.sharedArea)
    fopen.write('os.environ["X509_VOMS_DIR"]="%s/vomsdir"\n' %self.sharedArea)
    fopen.write('print "submitting wrapper"\n')
    fopen.write('os.system("./%s")\n' %executableFileBaseName)
    fopen.write('os.remove("%s")' % executableFileBaseName)
    fopen.close()
    
    fopen = open('run%s.py' %executableFileBaseName,'r')
    executableFileContent = fopen.read()
    fopen.close()
    
    self.log.debug("Executable File contents:\n", executableFileContent)
    
    #time.sleep(120)

    #Perform any other actions from the site admin
    if self.ceParameters.has_key('AdminCommands'):
      commands = self.ceParameters['AdminCommands'].split(';')
      for command in commands:
        self.log.verbose('Executing site admin command: %s' %command)
        result = shellCall(0,command,callbackFunction=self.sendOutput)
        if not result['OK'] or result['Value'][0]:
          self.log.error('Error during "%s":' %command,result)
          return S_ERROR('Error executing %s CE AdminCommands' %CE_NAME)

    # change the permissions of run###.py to 0755
    os.chmod('run%s.py' %executableFileBaseName,0755)
#    time.sleep(120)
    # submit run###.py to the torque batch system keeping the local env
    cmd = "qsub -q %s %s" %(self.queue, os.path.abspath('run%s.py' % executableFileBaseName ))
    self.log.verbose('CE submission command: %s' %(cmd))

    result = shellCall(0,cmd, callbackFunction = self.sendOutput)
    if not result['OK'] or result['Value'][0]:
      self.log.warn('===========>Torque CE result NOT OK')
      self.log.debug(result)
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
    
    matched = re.search("batch\D+(\d+)\D+(\d+)\W+(\w+)\W+(\w+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\W+(\w+)", stdout)
    
    result['WaitingJobs'] = matched.group(5)
    result['RunningJobs'] = matched.group(6)
    self.log.verbose('Waiting Jobs: ', matched.group(5))
    self.log.verbose('Running Jobs: ', matched.group(6))
    return S_OK(result)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
