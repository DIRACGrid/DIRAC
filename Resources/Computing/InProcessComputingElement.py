########################################################################
# File :   InProcessComputingElement.py
# Author : Stuart Paterson
########################################################################

""" The simplest of the "inner" CEs (meaning it's used by a jobAgent inside a pilot)

    A "InProcess" CE instance submits jobs in the current process.
    This is the standard "inner CE" invoked from the JobAgent, main alternative being the PoolCE
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import stat

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Security.ProxyInfo import getProxyInfo


class InProcessComputingElement(ComputingElement):

  #############################################################################
  def __init__(self, ceUniqueID):
    """ Standard constructor.
    """
    super(InProcessComputingElement, self).__init__(ceUniqueID)
    self.submittedJobs = 0

    self.log.debug("CE parameters", self.ceParameters)

    self.processors = int(self.ceParameters.get('NumberOfProcessors', 1))
    self.ceParameters['MaxTotalJobs'] = 1

  #############################################################################
  def submitJob(self, executableFile, proxy=None, **kwargs):
    """ Method to submit job (overriding base method).

    :param str executableFile: file to execute via systemCall.
                               Normally the JobWrapperTemplate when invoked by the JobAgent.
    :param str proxy: the proxy used for running the job (the payload). It will be dumped to a file.
    """

    # This will get the pilot proxy
    ret = getProxyInfo()
    if not ret['OK']:
      pilotProxy = None
    else:
      pilotProxy = ret['Value']['path']

    self.log.notice('Pilot Proxy:', pilotProxy)

    payloadEnv = dict(os.environ)
    payloadProxy = ''
    if proxy:
      self.log.verbose('Setting up proxy for payload')
      result = self.writeProxyToFile(proxy)
      if not result['OK']:
        return result

      payloadProxy = result['Value']  # proxy file location
      payloadEnv['X509_USER_PROXY'] = payloadProxy

      self.log.verbose('Starting process for monitoring payload proxy')

      result = gThreadScheduler.addPeriodicTask(self.proxyCheckPeriod, self._monitorProxy,
                                                taskArgs=(pilotProxy, payloadProxy),
                                                executions=0, elapsedTime=0)
      if result['OK']:
        renewTask = result['Value']
      else:
	self.log.warn('Failed to start proxy renewal task')
	renewTask = None

    if not os.access(executableFile, 5):
      os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    cmd = os.path.abspath(executableFile)
    self.log.verbose('CE submission command: %s' % (cmd))
    result = systemCall(0, cmd, callbackFunction=self.sendOutput, env=payloadEnv)
    if payloadProxy:
      os.unlink(payloadProxy)

    if renewTask:
      gThreadScheduler.removeTask(renewTask)

    ret = S_OK()

    if not result['OK']:
      self.log.error('Fail to run InProcess', result['Message'])
    elif result['Value'][0] > 128:
      # negative exit values are returned as 256 - exit
      self.log.warn('InProcess Job Execution Failed')
      self.log.info('Exit status:', result['Value'][0] - 256)
      if result['Value'][0] - 256 == -2:
        error = 'JobWrapper initialization error'
      elif result['Value'][0] - 256 == -1:
        error = 'JobWrapper execution error'
      else:
        error = 'InProcess Job Execution Failed'
      res = S_ERROR(error)
      res['Value'] = result['Value'][0] - 256
      return res
    elif result['Value'][0] > 0:
      self.log.warn('Fail in payload execution')
      self.log.info('Exit status:', result['Value'][0])
      ret['PayloadFailed'] = result['Value'][0]
    else:
      self.log.debug('InProcess CE result OK')

    self.submittedJobs += 1
    return ret

  #############################################################################
  def getCEStatus(self):
    """ Method to return information on running and waiting jobs,
        as well as number of available processors
    """
    result = S_OK()

    # FIXME: this is broken?
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    # processors
    result['AvailableProcessors'] = self.processors
    return result
