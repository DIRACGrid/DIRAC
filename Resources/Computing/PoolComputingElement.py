########################################################################
# $Id$
# File :   PoolComputingElement.py
# Author : A.T.
########################################################################

""" The Computing Element to run several jobs simultaneously
"""

__RCSID__ = "$Id$"

import os
import multiprocessing

from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement
from DIRAC.Resources.Computing.SudoComputingElement import SudoComputingElement
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

MandatoryParameters = []
# Number of unix users to run job payloads with sudo
MAX_NUMBER_OF_SUDO_UNIX_USERS = 32


def executeJob(executableFile, proxy, taskID, **kwargs):
  """ wrapper around ce.submitJob: decides which CE to use (Sudo or InProcess)
  """

  useSudo = kwargs.pop('UseSudo', False)
  if useSudo:
    ce = SudoComputingElement("Task-" + str(taskID))
    payloadUser = kwargs.get('PayloadUser')
    if payloadUser:
      ce.setParameters({'PayloadUser': payloadUser})
  else:
    ce = InProcessComputingElement("Task-" + str(taskID))

  return ce.submitJob(executableFile, proxy)


class PoolComputingElement(ComputingElement):

  mandatoryParameters = MandatoryParameters

  #############################################################################
  def __init__(self, ceUniqueID):
    """ Standard constructor.
    """
    ComputingElement.__init__(self, ceUniqueID)
    self.ceType = "Pool"
    self.log = gLogger.getSubLogger('Pool')
    self.submittedJobs = 0
    self.processors = 1
    self.pPool = None
    self.taskID = 0
    self.processorsPerTask = {}
    self.userNumberPerTask = {}
    self.useSudo = False

  #############################################################################
  def _addCEConfigDefaults(self):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults(self)

  def _reset(self):

    self.processors = int(self.ceParameters.get('NumberOfProcessors', self.processors))
    self.ceParameters['MaxTotalJobs'] = self.processors
    self.useSudo = self.ceParameters.get('SudoExecution', False)

  def getProcessorsInUse(self):
    """
    """
    processorsInUse = 0
    for task in self.processorsPerTask:
      processorsInUse += self.processorsPerTask[task]
    return processorsInUse

  #############################################################################
  def submitJob(self, executableFile, proxy, **kwargs):
    """ Method to submit job.
    """

    if self.pPool is None:
      self.pPool = ProcessPool(minSize=self.processors,
                               maxSize=self.processors,
                               poolCallback=self.finalizeJob)

    self.pPool.processResults()

    processorsInUse = self.getProcessorsInUse()
    if kwargs.get('wholeNode'):
      if processorsInUse > 0:
        return S_ERROR('Can not take WholeNode job')  # , %d/%d slots used' % (self.slotsInUse,self.slots) )
      else:
        requestedProcessors = self.processors
    elif "numberOfProcessors" in kwargs:
      requestedProcessors = int(kwargs['numberOfProcessors'])
      if requestedProcessors > 0:
        if (processorsInUse + requestedProcessors) > self.processors:
          return S_ERROR('Not enough slots: requested %d, available %d' % (requestedProcessors,
                                                                           self.processors - processorsInUse))
    else:
      requestedProcessors = 1
    if self.processors - processorsInUse < requestedProcessors:
      return S_ERROR('Not enough slots: requested %d, available %d' % (requestedProcessors,
                                                                       self.processors - processorsInUse))

    ret = getProxyInfo()
    if not ret['OK']:
      pilotProxy = None
    else:
      pilotProxy = ret['Value']['path']
    self.log.notice('Pilot Proxy:', pilotProxy)

    kwargs = {'UseSudo': False}
    if self.useSudo:
      for nUser in range(MAX_NUMBER_OF_SUDO_UNIX_USERS):
        if nUser not in self.userNumberPerTask.values():
          break
      kwargs['NUser'] = nUser
      kwargs['PayloadUser'] = os.environ['USER'] + 'p%s' % str(nUser).zfill(2)
      kwargs['UseSudo'] = True

    result = self.pPool.createAndQueueTask(executeJob,
                                           args=(executableFile, proxy, self.taskID),
                                           kwargs=kwargs,
                                           taskID=self.taskID,
                                           usePoolCallbacks=True)
    self.processorsPerTask[self.taskID] = requestedProcessors
    self.taskID += 1

    self.pPool.processResults()

    return result

  def finalizeJob(self, taskID, result):
    """ Finalize the job
    """
    nProc = self.processorsPerTask.pop(taskID)
    if result['OK']:
      self.log.info('Task %d finished successfully, %d processor(s) freed' % (taskID, nProc))
    else:
      self.log.error("Task failed submission", "%d, message: %s" % (taskID, result['Message']))

  #############################################################################
  def getCEStatus(self, jobIDList=None):
    """ Method to return information on running and pending jobs.
    """
    self.pPool.processResults()
    result = S_OK()
    result['SubmittedJobs'] = 0
    nJobs = 0
    for _j, value in self.processorsPerTask.iteritems():
      if value > 0:
        nJobs += 1
    result['RunningJobs'] = nJobs
    result['WaitingJobs'] = 0
    processorsInUse = self.getProcessorsInUse()
    result['UsedProcessors'] = processorsInUse
    result['AvailableProcessors'] = self.processors - processorsInUse
    return result

  #############################################################################
  def monitorProxy(self, pilotProxy, payloadProxy):
    """ Monitor the payload proxy and renew as necessary.
    """
    return self._monitorProxy(pilotProxy, payloadProxy)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
