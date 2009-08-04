# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/RequestFinalization.py,v 1.1 2009/08/04 14:55:21 acsmith Exp $

__RCSID__ = "$Id: RequestFinalization.py,v 1.1 2009/08/04 14:55:21 acsmith Exp $"

from DIRAC import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath

from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
 
import time,os,sys,re
from types import *

AGENT_NAME = 'Stager/RequestFinalization'

class RequestFinalization(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.stagerClient = RPCClient('dips://volhcb08.cern.ch:9149/Stager/Stager')
    return S_OK()

  def execute(self):
    res = self.clearFailedTasks()
    res = self.clearStagedTasks()
    res = self.removeUnlinkedReplicas()
    return res

  def clearFailedTasks(self):
    """ This obtains the tasks which are marked as Failed and remove all the associated records
    """ 
    res = self.stagerClient.getTasksWithStatus('Failed')
    if not res['OK']:
      gLogger.fatal("RequestFinalization.clearFailedTasks: Failed to get Failed Tasks from StagerDB.", res['Message'])
      return res
    failedTasks = res['Value']
    gLogger.info("RequestFinalization.clearFailedTasks: Obtained %s tasks in the 'Failed' status." % len(failedTasks))
    for taskID,(source,callback,sourceTask) in failedTasks.items():
      if (callback and sourceTask):
        res = self.__performCallback(callback,sourceTask)
        if not res['OK']:
          gLogger.error("RequestFinalization.clearFailedTasks: Failed to perform callback for task.", res['Message'])
          failedTasks.pop(taskID)
    if not failedTasks:
      gLogger.info("RequestFinalization.clearFailedTasks: No tasks to remove.")
      return S_OK()
    gLogger.info("RequestFinalization.clearFailedTasks: Removing %s tasks..." % len(failedTasks))
    res = self.stagerClient.removeTasks(failedTasks.keys())
    if not res['OK']:
      gLogger.error("RequestFinalization.clearFailedTasks: Failed to remove tasks.",res['Message'])
      return res
    gLogger.info("RequestFinalization.clearFailedTasks: ...removed.") 
    return S_OK()

  def clearStagedTasks(self):
    """ This updates the status of the Tasks to Done then issues the call back message
    """
    res = self.stagerClient.updateStageCompletingTasks()
    if not res['OK']:
      gLogger.fatal("RequestFinalization.clearStagedTasks: Failed to update StageCompleting Tasks from StagerDB.", res['Message'])
      return res
    gLogger.info("RequestFinalization.clearStagedTasks: Updated %s Tasks from StageCompleting to Staged." % len(res['Value']))
    res = self.stagerClient.getTasksWithStatus('Staged')
    if not res['OK']:
      gLogger.fatal("RequestFinalization.clearStagedTasks: Failed to get Staged Tasks from StagerDB.", res['Message'])
      return res
    stagedTasks = res['Value']
    gLogger.info("RequestFinalization.clearStagedTasks: Obtained %s tasks in the 'Staged' status." % len(stagedTasks))
    for taskID,(source,callback,sourceTask) in stagedTasks.items():
      if (callback and sourceTask):
        res = self.__performCallback(callback,sourceTask)
        if not res['OK']:
          gLogger.error("RequestFinalization.clearStagedTasks: Failed to perform callback for task.", res['Message'])
          stagedTasks.pop(taskID)
    if not stagedTasks:
      gLogger.info("RequestFinalization.clearStagedTasks: No tasks to remove.")
      return S_OK()
    gLogger.info("RequestFinalization.clearStagedTasks: Removing %s tasks..." % len(stagedTasks))
    res = self.stagerClient.removeTasks(stagedTasks.keys())
    if not res['OK']:
      gLogger.error("RequestFinalization.clearStagedTasks: Failed to remove tasks.",res['Message'])
      return res
    gLogger.info("RequestFinalization.clearStagedTasks: ...removed.")
    return S_OK()

  def removeUnlinkedReplicas(self):
    gLogger.info("RequestFinalization.removeUnlinkedReplicas: Attempting to cleanup unlinked Replicas.")
    res = self.stagerClient.removeUnlinkedReplicas()
    if not res['OK']:
      gLogger.error("RequestFinalization.removeUnlinkedReplicas: Failed to cleanup unlinked Replicas.",res['Message'])
    else:
      gLogger.info("RequestFinalization.removeUnlinkedReplicas: Successfully removed unlinked Replicas.")
    return res

  def __performCallback(self):
    return S_OK()
