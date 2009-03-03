########################################################################
# $Id: TorqueComputingElement.py,v 1.1 2009/03/03 15:33:31 szczypka Exp $
# File :   TorqueComputingElement.py
# Author : Stuart Paterson
########################################################################

""" The simplest Computing Element instance that submits jobs locally.
"""

__RCSID__ = "$Id: TorqueComputingElement.py,v 1.1 2009/03/03 15:33:31 szczypka Exp $"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK, S_ERROR
from DIRAC.Core.Security.Misc                            import getProxyInfo

import os,sys
import string

CE_NAME = 'Torque'

class TorqueComputingElement(ComputingElement):

  #############################################################################
  def __init__(self):
    """ Standard constructor.
    """
    ComputingElement.__init__(self,CE_NAME)
    self.submittedJobs = 0

  #############################################################################
  def submitJob(self,executableFile,jdl,localID):
    """ Method to submit job, should be overridden in sub-class.
    """

    ret = getProxyInfo( disableVOMS = True )
    if not ret['OK']:
      return S_ERROR("Could not get Proxy info")
    
    proxyLocation = ret['Value']['path']

    print "First path: %s" %executableFile

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
    proxy = fopen.read()
    fopen.close()

    # create and write the executable file run###.py
    executableFileBaseName=os.path.basename(executableFile)    
    fopen = open('run%s.py' %executableFileBaseName,'w')
    fopen.write('#!/usr/bin/env python\n')
    fopen.write('import os\n')
    fopen.write('fopen = open("%s","w")\n' %executableFileBaseName)
    fopen.write('fopen.write("%s")\n' %contents)
    fopen.write("fopen.close()\n")
    fopen.write('os.chmod("%s",0755)\n'%executableFileBaseName)
    fopen.write('fopen = open("%s","w")\n' %proxyLocation)
    fopen.write('fopen.write("%s")\n' %proxy)
    fopen.write('fopen.close()\n')
    fopen.write('os.chmod("%s",0600)\n' %proxyLocation)
    fopen.write('os.environ["X509_USER_PROXY"]="%s"\n' %proxyLocation)
    fopen.write('print "submitting wrapper"\n')     
    fopen.write('os.system("./%s")\n' %executableFileBaseName)
    fopen.close()

    # change the permissions of run###.py to 0755
    os.chmod('run%s.py' %executableFileBaseName,0755)
    # submit run###.py to the torque batch system keeping the local env
    cmd = "qsub -V %s" %(os.path.abspath('run%s.py' %executableFileBaseName))
    self.log.verbose('CE submission command: %s' %(cmd))

    result = shellCall(0,cmd, callbackFunction = self.sendOutput)
    if not result['OK']:
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
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return S_OK(result)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
