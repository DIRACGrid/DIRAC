""" The Matcher service provides an XMLRPC interface for matching jobs to pilots

    It uses a Matcher and a Limiter object that encapsulated the matching logic.
    It connects to JobDB, TaskQueueDB, and PilotAgentsDB.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from DIRAC import gLogger, S_OK, S_ERROR

from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning

from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor

from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB

from DIRAC.WorkloadManagementSystem.Client.Matcher import Matcher
from DIRAC.WorkloadManagementSystem.Client.Limiter import Limiter
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


class MatcherHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    cls.gJobDB = JobDB()
    cls.gJobLoggingDB = JobLoggingDB()
    cls.gTaskQueueDB = TaskQueueDB()
    cls.gPilotAgentsDB = PilotAgentsDB()
    cls.limiter = Limiter(jobDB=cls.gJobDB)

    cls.gTaskQueueDB.recalculateTQSharesForAll()

    gMonitor.registerActivity('matchTime', "Job matching time",
                              'Matching', "secs", gMonitor.OP_MEAN, 300)
    gMonitor.registerActivity('matchesDone', "Job Match Request",
                              'Matching', "matches", gMonitor.OP_RATE, 300)
    gMonitor.registerActivity('matchesOK', "Matched jobs",
                              'Matching', "matches", gMonitor.OP_RATE, 300)
    gMonitor.registerActivity('numTQs', "Number of Task Queues",
                              'Matching', "tqsk queues", gMonitor.OP_MEAN, 300)

    gThreadScheduler.addPeriodicTask(120, cls.gTaskQueueDB.recalculateTQSharesForAll)
    gThreadScheduler.addPeriodicTask(60, cls.sendNumTaskQueues)

    cls.sendNumTaskQueues()
    return S_OK()

  @classmethod
  def sendNumTaskQueues(cls):
    result = cls.gTaskQueueDB.getNumTaskQueues()
    if result['OK']:
      gMonitor.addMark('numTQs', result['Value'])
    else:
      gLogger.error("Cannot get the number of task queues", result['Message'])


##############################################################################
  types_requestJob = [six.string_types + (dict,)]

  def export_requestJob(self, resourceDescription):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """

    resourceDescription['Setup'] = self.serviceInfoDict['clientSetup']
    credDict = self.getRemoteCredentials()

    try:
      opsHelper = Operations(group=credDict['group'])
      matcher = Matcher(pilotAgentsDB=self.gPilotAgentsDB,
                        jobDB=self.gJobDB,
                        tqDB=self.gTaskQueueDB,
                        jlDB=self.gJobLoggingDB,
                        opsHelper=opsHelper)
      result = matcher.selectJob(resourceDescription, credDict)
    except RuntimeError as rte:
      self.log.error("Error requesting job: ", rte)
      return S_ERROR("Error requesting job")

    # result can be empty, meaning that no job matched
    if result:
      gMonitor.addMark("matchesDone")
      gMonitor.addMark("matchesOK")
      return S_OK(result)
    # FIXME: This is correctly interpreted by the JobAgent, but DErrno should be used instead
    return S_ERROR("No match found")

##############################################################################
  types_getActiveTaskQueues = []

  @classmethod
  @ignoreEncodeWarning
  def export_getActiveTaskQueues(cls):
    """ Return all task queues
    """
    return cls.gTaskQueueDB.retrieveTaskQueues()

##############################################################################
  types_getMatchingTaskQueues = [dict]
  # int keys are cast into str

  @classmethod
  @ignoreEncodeWarning
  def export_getMatchingTaskQueues(cls, resourceDict):
    """ Return all task queues that match the resourceDict
    """
    if 'Site' in resourceDict and isinstance(resourceDict['Site'], six.string_types):
      negativeCond = cls.limiter.getNegativeCondForSite(resourceDict['Site'])
    else:
      negativeCond = cls.limiter.getNegativeCond()
    matcher = Matcher(pilotAgentsDB=cls.gPilotAgentsDB,
                      jobDB=cls.gJobDB,
                      tqDB=cls.gTaskQueueDB,
                      jlDB=cls.gJobLoggingDB)
    resourceDescriptionDict = matcher._processResourceDescription(resourceDict)
    return cls.gTaskQueueDB.getMatchingTaskQueues(resourceDescriptionDict,
                                                  negativeCond=negativeCond)
