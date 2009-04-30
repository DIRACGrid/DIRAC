########################################################################
# $Id: InProcessComputingElement.py,v 1.10 2009/04/30 13:20:49 rgracian Exp $
# File :   InProcessComputingElement.py
# Author : Stuart Paterson
########################################################################

""" The simplest Computing Element instance that submits jobs locally.
"""

__RCSID__ = "$Id: InProcessComputingElement.py,v 1.10 2009/04/30 13:20:49 rgracian Exp $"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.Core.Utilities.ThreadScheduler                import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess                     import systemCall
from DIRAC.Core.Security.Misc                            import getProxyInfoAsString
from DIRAC                                               import gConfig,S_OK,S_ERROR

import os,sys

CE_NAME = 'InProcess'

class InProcessComputingElement(ComputingElement):

  #############################################################################
  def __init__(self):
    """ Standard constructor.
    """
    ComputingElement.__init__(self,CE_NAME)
    self.minProxyTime = gConfig.getValue( '/Security/MinProxyLifeTime', 10800 ) #secs
    self.defaultProxyTime = gConfig.getValue( '/Security/DefaultProxyLifeTime', 86400 ) #secs
    self.proxyCheckPeriod = gConfig.getValue('/Security/ProxyCheckingPeriod',3600) #secs
    self.submittedJobs = 0

  #############################################################################
  def submitJob(self,executableFile,jdl,proxy,localID):
    """ Method to submit job, should be overridden in sub-class.
    """
    self.log.verbose('Setting up proxy for payload')
    result = self.writeProxyToFile(proxy)
    if not result['OK']:
      return result

    payloadProxy = result['Value']
    pilotProxy = os.environ['X509_USER_PROXY']
    os.environ[ 'X509_USER_PROXY' ] = payloadProxy
    self.log.verbose('Starting process for monitoring payload proxy')
    gThreadScheduler.addPeriodicTask(self.proxyCheckPeriod,self.monitorProxy,taskArgs=(pilotProxy,payloadProxy),executions=0,elapsedTime=0)

    if not os.access(executableFile, 5):
      os.chmod(executableFile,0755)
    cmd = os.path.abspath(executableFile)
    self.log.verbose('CE submission command: %s' %(cmd))
    result = systemCall(0,cmd,callbackFunction = self.sendOutput)

    ret = S_OK(localID)

    if not result['OK']:
      self.log.error('Fail to run InProcess',result['Message'])
    elif result['Value'][0] < 0:
      self.log.error('InProcess Job Execution Failed')
      self.log.info('Exit status:',result['Value'][0])
      return S_ERROR('InProcess Job Execution Failed')
    elif result['Value'][0] > 0:
      self.log.error('Fail in payload execution')
      self.log.info('Exit status:',result['Value'][0])
      ret['PayloadFailed'] = result['Value'][0]
    else:
      self.log.debug('InProcess CE result OK')

    self.submittedJobs += 1
    return ret

  #############################################################################
  def getDynamicInfo(self):
    """ Method to return information on running and pending jobs.
    """
    result = {}
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return S_OK(result)

  #############################################################################
  def monitorProxy(self,pilotProxy,payloadProxy):
    """ Monitor the payload proxy and renew as necessary.
    """
    if not os.path.exists(pilotProxy):
      return S_ERROR('Pilot proxy not found at %s' %pilotProxy)
    if not os.path.exists(payloadProxy):
      return S_ERROR('Payload proxy not found at %s' %payloadProxy)

    result = getProxyInfoAsString(payloadProxy)
    if not result['OK']:
      self.log.error('Could not get payload proxy info',result)
      return result

    self.log.verbose('Payload proxy information:\n%s' %result['Value'])
    gProxyManager.renewProxy(minLifeTime=self.minProxyTime,
                             newProxyLifeTime=self.defaultProxyTime,
                             proxyToConnect=pilotProxy)

    return S_OK('Proxy checked')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
