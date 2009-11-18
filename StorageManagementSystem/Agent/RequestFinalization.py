# $Header: /tmp/libdirac/tmp.FKduyw2449/dirac/DIRAC3/DIRAC/StorageManagementSystem/Agent/RequestFinalization.py,v 1.2 2009/10/30 22:03:03 acsmith Exp $

__RCSID__ = "$Id: RequestFinalization.py,v 1.2 2009/10/30 22:03:03 acsmith Exp $"

from DIRAC import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath

from DIRAC.Core.Base.AgentModule             import AgentModule
from DIRAC.Core.DISET.RPCClient              import RPCClient
import time,os,sys,re
from types import *

AGENT_NAME = 'StorageManagement/RequestFinalization'

class RequestFinalization(AgentModule):

  def initialize(self):
    self.stagerClient = RPCClient('StorageManagement/Stager')
    return S_OK()

  def execute(self):
    res = self.clearFailedTasks()
    res = self.callbackStagedTasks()
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
        res = self.__performCallback('Failed',callback,sourceTask)
        if not res['OK']:
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

  def callbackStagedTasks(self):
    """ This updates the status of the Tasks to Done then issues the call back message
    """
    res = self.stagerClient.updateStageCompletingTasks()
    if not res['OK']:
      gLogger.fatal("RequestFinalization.callbackStagedTasks: Failed to update StageCompleting Tasks from StagerDB.", res['Message'])
      return res
    gLogger.info("RequestFinalization.callbackStagedTasks: Updated %s Tasks from StageCompleting to Staged." % len(res['Value']))
    res = self.stagerClient.getTasksWithStatus('Staged')
    if not res['OK']:
      gLogger.fatal("RequestFinalization.callbackStagedTasks: Failed to get Staged Tasks from StagerDB.", res['Message'])
      return res
    stagedTasks = res['Value']
    gLogger.info("RequestFinalization.callbackStagedTasks: Obtained %s tasks in the 'Staged' status." % len(stagedTasks))
    for taskID,(source,callback,sourceTask) in stagedTasks.items():
      if (callback and sourceTask):
        res = self.__performCallback('Done',callback,sourceTask)
        if not res['OK']:
          stagedTasks.pop(taskID)
    if not stagedTasks:
      gLogger.info("RequestFinalization.callbackStagedTasks: No tasks to update to Done.")
      return S_OK()
    res = self.stagerClient.setTasksDone(stagedTasks.keys())
    if not res['OK']:
      gLogger.fatal("RequestFinalization.callbackStagedTasks: Failed to set status of Tasks to Done.", res['Message'])
    return res

  def __performCallback(self, status, callback, sourceTask):
    method,service = callback.split('@')
    gLogger.debug("RequestFinalization.__performCallback: Attempting to perform call back for %s with %s status" % (sourceTask,status))
    client = RPCClient(service)
    gLogger.debug("RequestFinalization.__performCallback: Created RPCClient to %s" % service)
    execString = "res = client.%s(%s,'%s')" % (method,sourceTask,status)
    gLogger.debug("RequestFinalization.__performCallback: Attempting to invoke %s service method" % method)
    exec(execString)
    if not res['OK']:
      gLogger.error("RequestFinalization.__performCallback: Failed to perform callback",res['Message'])
    else:
      gLogger.info("RequestFinalization.__performCallback: Successfully issued callback to %s for %s with %s status" % (callback,sourceTask, status))
    return res

  def removeUnlinkedReplicas(self):
    gLogger.info("RequestFinalization.removeUnlinkedReplicas: Attempting to cleanup unlinked Replicas.")
    res = self.stagerClient.removeUnlinkedReplicas()
    if not res['OK']:
      gLogger.error("RequestFinalization.removeUnlinkedReplicas: Failed to cleanup unlinked Replicas.",res['Message'])
    else:
      gLogger.info("RequestFinalization.removeUnlinkedReplicas: Successfully removed unlinked Replicas.")
    return res

  def clearReleasedTasks(self):
    # TODO: issue release of the pins assoicated to this task
    res = self.stagerClient.getTasksWithStatus('Released')
    if not res['OK']:
      gLogger.fatal("RequestFinalization.clearReleasedTasks: Failed to get Released Tasks from StagerDB.", res['Message'])
      return res
    gLogger.info("RequestFinalization.clearReleasedTasks: Removing %s tasks..." % len(stagedTasks))
    res = self.stagerClient.removeTasks(stagedTasks.keys())
    if not res['OK']:
      gLogger.error("RequestFinalization.clearReleasedTasks: Failed to remove tasks.",res['Message'])
      return res
    gLogger.info("RequestFinalization.clearReleasedTasks: ...removed.")
    return S_OK()
