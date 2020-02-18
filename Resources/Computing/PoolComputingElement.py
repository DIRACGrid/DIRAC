########################################################################
# File :   PoolComputingElement.py
# Author : A.T.
########################################################################

""" The Pool Computing Element is an "inner" CE (meaning it's used by a jobAgent inside a pilot)

    It's used running several jobs simultaneously in separate processes, managed by a ProcessPool
"""

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.private.ConfigurationData import ConfigurationData
from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement
from DIRAC.Resources.Computing.SudoComputingElement import SudoComputingElement
from DIRAC.Resources.Computing.ComputingElement import ComputingElement

MandatoryParameters = []
# Number of unix users to run job payloads with sudo
MAX_NUMBER_OF_SUDO_UNIX_USERS = 32


def executeJob(executableFile, proxy, taskID, **kwargs):
  """ wrapper around ce.submitJob: decides which CE to use (Sudo or InProcess)

  :param str executableFile: location of the executable file
  :param str proxy: proxy file location to be used for job submission
  :param int taskID: local task ID of the PoolCE

  :return: the result of the job submission
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
    super(PoolComputingElement, self).__init__(ceUniqueID)

    self.ceType = "Pool"
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
    """ Update internal variables after some extra parameters are added

    :return: None
    """

    self.processors = int(self.ceParameters.get('NumberOfProcessors', self.processors))
    self.ceParameters['MaxTotalJobs'] = self.processors
    self.useSudo = self.ceParameters.get('SudoExecution', False)

  def getProcessorsInUse(self):
    """ Get the number of currently allocated processor cores

    :return: number of processor cores
    """
    processorsInUse = 0
    for task in self.processorsPerTask:
      processorsInUse += self.processorsPerTask[task]
    return processorsInUse

  #############################################################################
  def submitJob(self, executableFile, proxy, **kwargs):
    """ Method to submit job.

    :param str executableFile: location of the executable file
    :param str proxy: payload proxy

    :return: S_OK/S_ERROR of the result of the job submission
    """

    if self.pPool is None:
      self.pPool = ProcessPool(minSize=self.processors,
                               maxSize=self.processors,
                               poolCallback=self.finalizeJob)

    self.pPool.processResults()

    processorsForJob = self._getProcessorsForJobs(kwargs)
    if not processorsForJob:
      return S_ERROR('Not enough processors for the job')

    # Now persisiting the job limits for later use in pilot.cfg file (pilot 3 default)
    cd = ConfigurationData(loadDefaultCFG=False)
    res = cd.loadFile('pilot.cfg')
    if not res['OK']:
      self.log.error("Could not load pilot.cfg", res['Message'])
    # only NumberOfProcessors for now, but RAM (or other stuff) can also be added
    jobID = int(kwargs.get('jobDesc', {}).get('jobID', 0))
    cd.setOptionInCFG('/Resources/Computing/JobLimits/%d/NumberOfProcessors' % jobID, processorsForJob)
    res = cd.dumpLocalCFGToFile('pilot.cfg')
    if not res['OK']:
      self.log.error("Could not dump cfg to pilot.cfg", res['Message'])

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
    self.processorsPerTask[self.taskID] = processorsForJob
    self.taskID += 1

    self.pPool.processResults()

    return result

  def _getProcessorsForJobs(self, kwargs):
    """ helper function
    """
    processorsInUse = self.getProcessorsInUse()
    availableProcessors = self.processors - processorsInUse

    # Does this ask for MP?
    if not kwargs.get('mpTag', False):
      if availableProcessors:
        return 1
      else:
        return 0

    # From here we assume the job is asking for MP
    if kwargs.get('wholeNode', False):
      if processorsInUse > 0:
        return 0
      else:
        requestedProcessors = self.processors

    if "numberOfProcessors" in kwargs:
      requestedProcessors = int(kwargs['numberOfProcessors'])
    else:
      requestedProcessors = 1

    if availableProcessors < requestedProcessors:
      return 0

    # If there's a maximum number of processors allowed for the job, use that as maximum,
    # otherwise it will use all the remaining processors
    if 'maxNumberOfProcessors' in kwargs and kwargs['maxNumberOfProcessors']:
      maxNumberOfProcessors = min(int(kwargs['maxNumberOfProcessors']), availableProcessors)
    else:
      maxNumberOfProcessors = availableProcessors

    return maxNumberOfProcessors

  def finalizeJob(self, taskID, result):
    """ Finalize the job by updating the process utilisation counters

    :param int taskID: local PoolCE task ID
    :param dict result: result of the job execution

    """
    nProc = self.processorsPerTask.pop(taskID)
    if result['OK']:
      self.log.info('Task %d finished successfully, %d processor(s) freed' % (taskID, nProc))
    else:
      self.log.error("Task failed submission", "%d, message: %s" % (taskID, result['Message']))

  #############################################################################
  def getCEStatus(self):
    """ Method to return information on running and waiting jobs,
        as well as the number of processors (used, and available).

    :return: dictionary of numbers of jobs per status and processors (used, and available)
    """

    if self.pPool is None:
      self.pPool = ProcessPool(minSize=self.processors,
                               maxSize=self.processors,
                               poolCallback=self.finalizeJob)

    self.pPool.processResults()
    result = S_OK()
    nJobs = 0
    for _j, value in self.processorsPerTask.iteritems():
      if value > 0:
        nJobs += 1
    result['SubmittedJobs'] = nJobs
    result['RunningJobs'] = nJobs
    result['WaitingJobs'] = 0

    # dealing with processors
    processorsInUse = self.getProcessorsInUse()
    result['UsedProcessors'] = processorsInUse
    result['AvailableProcessors'] = self.processors - processorsInUse
    return result

  def getDescription(self):
    """ Get a list of CEs descriptions (each is a dict)

        This is called by the JobAgent.
    """
    result = super(PoolComputingElement, self).getDescription()
    if not result['OK']:
      return result
    ceDict = result['Value']

    ceDictList = []
    if self.ceParameters.get('MultiProcessorStrategy'):
      strategyRequiredTags = []
      if not ceDict.get("ProcessorsInUse", 0):
        # We are starting from a clean page, try to get the most demanding
        # jobs first
        strategyRequiredTags.append(['WholeNode'])
      processors = ceDict.get('NumberOfProcessors', 0)
      if processors > 1:
        # We have several processors at hand, try to use most of them
        strategyRequiredTags.append(['%dProcessors' % processors])
        # Well, at least jobs with some processors requirement
        strategyRequiredTags.append(['MultiProcessor'])

      for strat in strategyRequiredTags:
        newCEDict = dict(ceDict)
        newCEDict.setdefault("RequiredTag", []).extend(strat)
        ceDictList.append(newCEDict)

    # Do not require anything special if nothing else was lucky
    ceDictList.append(dict(ceDict))

    return S_OK(ceDictList)

  #############################################################################
  def monitorProxy(self, pilotProxy, payloadProxy):
    """ Monitor the payload proxy and renew as necessary.

    :param str pilotProxy: location of the pilotProxy
    :param str payloadProxy: location of the payloadProxy
    """
    return self._monitorProxy(pilotProxy, payloadProxy)
