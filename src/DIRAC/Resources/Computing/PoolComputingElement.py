########################################################################
# File :   PoolComputingElement.py
# Author : A.T.
########################################################################

""" The Pool Computing Element is an "inner" CE (meaning it's used by a jobAgent inside a pilot)

    It's used running several jobs simultaneously in separate processes, managed by a ProcessPool
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import concurrent.futures

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.private.ConfigurationData import ConfigurationData

from DIRAC.Resources.Computing.ComputingElement import ComputingElement

from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement
from DIRAC.Resources.Computing.SudoComputingElement import SudoComputingElement
from DIRAC.Resources.Computing.SingularityComputingElement import SingularityComputingElement

# Number of unix users to run job payloads with sudo
MAX_NUMBER_OF_SUDO_UNIX_USERS = 32


def executeJob(executableFile, proxy, taskID, **kwargs):
  """ wrapper around ce.submitJob: decides which CE to use (Sudo or InProcess or Singularity)

  :param str executableFile: location of the executable file
  :param str proxy: proxy file location to be used for job submission
  :param int taskID: local task ID of the PoolCE

  :return: the result of the job submission (S_OK/S_ERROR)
  """

  innerCESubmissionType = kwargs.pop('InnerCESubmissionType')

  if innerCESubmissionType == 'Sudo':
    ce = SudoComputingElement("Task-" + str(taskID))
    payloadUser = kwargs.get('PayloadUser')
    if payloadUser:
      ce.setParameters({'PayloadUser': payloadUser})
  elif innerCESubmissionType == 'Singularity':
    ce = SingularityComputingElement("Task-" + str(taskID))
  else:
    ce = InProcessComputingElement("Task-" + str(taskID))

  return ce.submitJob(executableFile, proxy)


class PoolComputingElement(ComputingElement):

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

    # This CE will effectively submit to another "Inner"CE
    # (by default to the InProcess CE)
    self.innerCESubmissionType = 'InProcess'

  def _reset(self):
    """ Update internal variables after some extra parameters are added

    :return: None
    """

    self.processors = int(self.ceParameters.get('NumberOfProcessors', self.processors))
    self.ceParameters['MaxTotalJobs'] = self.processors
    self.innerCESubmissionType = self.ceParameters.get('InnerCESubmissionType', self.innerCESubmissionType)
    return S_OK()

  def getProcessorsInUse(self):
    """ Get the number of currently allocated processor cores

    :return: number of processors in use
    """
    processorsInUse = 0
    for future in self.processorsPerTask:
      processorsInUse += self.processorsPerTask[future]
    return processorsInUse

  #############################################################################
  def submitJob(self, executableFile, proxy=None, **kwargs):
    """ Method to submit job.
    This method will submit to a ProcessPoolExecutor, which returns Future objects.

    :param str executableFile: location of the executable file
    :param str proxy: payload proxy

    :return: S_OK/S_ERROR of the result of the job submission
    """

    if self.pPool is None:
      self.pPool = concurrent.futures.ProcessPoolExecutor(max_workers=self.processors)

    processorsForJob = self._getProcessorsForJobs(kwargs)
    if not processorsForJob:
      return S_ERROR('Not enough processors for the job')

    # Now persisting the job limits for later use in pilot.cfg file (pilot 3 default)
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

    # Here we define task kwargs: adding complex objects like thread.Lock can trigger errors in the task
    taskKwargs = {'InnerCESubmissionType': self.innerCESubmissionType}
    if self.innerCESubmissionType == 'Sudo':
      for nUser in range(MAX_NUMBER_OF_SUDO_UNIX_USERS):
        if nUser not in self.userNumberPerTask.values():
          break
      taskKwargs['NUser'] = nUser
      if 'USER' in os.environ:
        taskKwargs['PayloadUser'] = os.environ['USER'] + 'p%s' % str(nUser).zfill(2)

    future = self.pPool.submit(
        executeJob, executableFile, proxy, self.taskID, **taskKwargs
    )
    self.processorsPerTask[future] = processorsForJob
    self.taskID += 1
    future.add_done_callback(self.finalizeJob)

    return S_OK()  # returning S_OK as callback will do the rest

  def _getProcessorsForJobs(self, kwargs):
    """ helper function
    """
    processorsInUse = self.getProcessorsInUse()
    availableProcessors = self.processors - processorsInUse

    self.log.verbose("Processors (total, in use, available)",
                     "(%d, %d, %d)" % (self.processors, processorsInUse, availableProcessors))

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
        return self.processors

    if "numberOfProcessors" in kwargs:
      requestedProcessors = int(kwargs['numberOfProcessors'])
    else:
      requestedProcessors = 1

    if availableProcessors < requestedProcessors:
      return 0

    # If there's a maximum number of processors allowed for the job, use that as maximum,
    # otherwise it will use all the remaining processors
    if 'maxNumberOfProcessors' in kwargs and kwargs['maxNumberOfProcessors']:
      requestedProcessors = min(int(kwargs['maxNumberOfProcessors']), availableProcessors)

    return requestedProcessors

  def finalizeJob(self, future):
    """ Finalize the job by updating the process utilisation counters

        :param future: evaluating the future result
    """
    nProc = self.processorsPerTask.pop(future)

    result = future.result()  # This would be the result of the e.g. InProcess.submitJob()
    if result['OK']:
      self.log.info('Task %s finished successfully, %d processor(s) freed' % (future, nProc))
    else:
      self.log.error("Task failed submission", "%d, message: %s" % (future, result['Message']))
    self.taskResults[future] = result

  def getCEStatus(self):
    """ Method to return information on running and waiting jobs,
        as well as the number of processors (used, and available).

    :return: dictionary of numbers of jobs per status and processors (used, and available)
    """

    result = S_OK()
    nJobs = 0
    for _j, value in self.processorsPerTask.items():
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

  def shutdown(self):
    """ Wait for all futures (jobs) to complete
    """
    self.pPool.shutdown()  # blocking
    return S_OK(self.taskResults)
