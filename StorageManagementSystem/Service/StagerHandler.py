########################################################################
# $Header: /tmp/libdirac/tmp.FKduyw2449/dirac/DIRAC3/DIRAC/StorageManagementSystem/Service/StagerHandler.py,v 1.1 2009/10/30 19:38:56 acsmith Exp $
########################################################################

"""
    StagerHandler is the implementation of the StagerDB in the DISET framework
"""

__RCSID__ = "$Id: StagerHandler.py,v 1.1 2009/10/30 19:38:56 acsmith Exp $"

from types import *
from DIRAC                                             import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                   import RequestHandler
from DIRAC.StorageManagementSystem.DB.StagerDB         import StagerDB
# This is a global instance of the StagerDB class
stagerDB = False

def initializeStagerHandler(serviceInfo):
  global stagerDB
  stagerDB = StagerDB()
  return S_OK()

class StagerHandler(RequestHandler):

  ######################################################################
  #
  #  Example call back methods
  #

  types_updateTaskStatus = []
  def export_updateTaskStatus(self,sourceID,status, successful=[],failed=[]):
    """ An example to show the usage of the callbacks. """
    gLogger.info("StagerHandler.updateTaskStatus: Received callback information for ID %s" % sourceID)
    gLogger.info("StagerHandler.updateTaskStatus: Status = '%s'" % status)
    if successful:
      gLogger.info("StagerHandler.updateTaskStatus: %s files successfully staged" % len(successful))
      for lfn,time in successful:
        gLogger.info("StagerHandler.updateTaskStatus: %s %s" % (lfn.ljust(100),time.ljust(10)))
    if failed:
      gLogger.info("StagerHandler.updateTaskStatus: %s files failed to stage" % len(successful))
      for lfn,time in failed:
        gLogger.info("StagerHandler.updateTaskStatus: %s %s" % (lfn.ljust(100),time.ljust(10)))
    return S_OK()

  ######################################################################
  #
  #  Monitoring methods
  #

  types_getTaskStatus = [IntType]
  def export_getTaskStatus(self,taskID):
    """ Obtain the status of the stage task from the DB. """
    try:
      res = stagerDB.getTaskStatus(taskID)
      if res['OK']:
        gLogger.info('StagerHandler.getTaskStatus: Successfully obtained task status')
      else:
        gLogger.error('StagerHandler.getTaskStatus: Failed to get task status',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getTaskStatus: Exception when getting task status'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_getTaskInfo = [IntType]
  def export_getTaskInfo(self,taskID):
    """ Obtain the metadata of the stage task from the DB. """
    try:
      res = stagerDB.getTaskInfo(taskID)
      if res['OK']:
        gLogger.info('StagerHandler.getTaskInfo: Successfully obtained task metadata')
      else:
        gLogger.error('StagerHandler.getTaskInfo: Failed to get task metadata',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getTaskInfo: Exception when getting task metadata'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_getTaskSummary = [IntType]
  def export_getTaskSummary(self,taskID):
    """ Obtain the summary of the stage task from the DB. """
    try:
      res = stagerDB.getTaskSummary(taskID)
      if res['OK']:
        gLogger.info('StagerHandler.getTaskSummary: Successfully obtained task summary')
      else:
        gLogger.error('StagerHandler.getTaskSummary: Failed to get task summary',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getTaskSummary: Exception when getting task summary'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  ####################################################################
  #
  # setRequest is used to initially insert tasks and their associated files. Leaves files in New status.
  #

  types_setRequest = [ListType,StringType,StringType,StringType,IntType]
  def export_setRequest(self,lfns,storageElement,source,callbackMethod,taskID):
    """
        This method allows stage requests to be set into the StagerDB
    """
    try:
      res = stagerDB.setRequest(lfns,storageElement,source,callbackMethod,taskID)
      if res['OK']:
        gLogger.info('StagerHandler.setRequest: Successfully set stage request')
      else:
        gLogger.error('StagerHandler.setRequest: Failed to set stage request',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.setRequest: Exception when setting request'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  ####################################################################
  #
  # The state transition of the Replicas from New->Waiting
  #

  types_updateReplicaInformation = [ListType]
  def export_updateReplicaInformation(self,replicaTuples):
    """
        This method sets the pfn and size for the supplied replicas
    """
    try:
      res = stagerDB.updateReplicaInformation(replicaTuples)
      if res['OK']:
        gLogger.info('StagerHandler.updateReplicaInformation: Successfully updated replica information')
      else:
        gLogger.error('StagerHandler.updateRelicaInformation: Failed to update replica information',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.updateReplicaInformation: Exception when updating replica information'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  ####################################################################
  #
  # The state transition of the Replicas from Waiting->StageSubmitted
  #

  types_getWaitingReplicas = []
  def export_getWaitingReplicas(self):
    """
        This method obtains the replicas for which all replicas in the task are Waiting
    """
    try:
      res = stagerDB.getWaitingReplicas()
      if res['OK']:
        gLogger.info('StagerHandler.getWaitingReplicas: Successfully obtained Waiting replicas')
      else:
        gLogger.error('StagerHandler.getWaitingReplicas: Failed to obtain Waiting replicas',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getWaitingReplicas: Exception when obtaining Waiting replicas.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_getSubmittedStagePins = []
  def export_getSubmittedStagePins(self):
    """
        This method obtains the number of files and size of the requests submitted for each storage element
    """
    try:
      res = stagerDB.getSubmittedStagePins()
      if res['OK']:
        gLogger.info('StagerHandler.getSubmittedStagePins: Successfully obtained submitted request summary')
      else:
        gLogger.error('StagerHandler.getSubmittedStagePins: Failed to obtain submitted request summary',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getSubmittedStagePins: Exception when obtaining submitted request summary.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_insertStageRequest = [DictType]
  def export_insertStageRequest(self,requestReplicas):
    """
        This method inserts the stage request ID assocaited to supplied replicaIDs
    """
    try:
      res = stagerDB.insertStageRequest(requestReplicas)
      if res['OK']:
        gLogger.info('StagerHandler.insertStageRequest: Successfully inserted stage request information')
      else:
        gLogger.error('StagerHandler.insertStageRequest: Failed to insert stage request information',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.insertStageRequest: Exception when inserting stage request.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  ####################################################################
  #
  # The state transition of the Replicas from StageSubmitted->Staged
  #

  types_getStageSubmittedReplicas = []
  def export_getStageSubmittedReplicas(self):
    """
        This method obtains the replica metadata and the stage requestID for the replicas in StageSubmitted status
    """
    try:
      res = stagerDB.getStageSubmittedReplicas()
      if res['OK']:
        gLogger.info('StagerHandler.getStageSubmittedReplicas: Successfully obtained StageSubmitted replicas')
      else:
        gLogger.error('StagerHandler.getStageSubmittedReplicas: Failed to obtain StageSubmitted replicas',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getStageSubmittedReplicas: Exception when obtaining StageSubmitted replicas.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_setStageComplete = [ListType]
  def export_setStageComplete(self,replicaIDs):
    """
        This method updates the status of the stage request for the supplied replica IDs
    """
    try:
      res = stagerDB.setStageComplete(replicaIDs)
      if res['OK']:
        gLogger.info('StagerHandler.setStageComplete: Successfully set StageRequest complete')
      else:
        gLogger.error('StagerHandler.setStageComplete: Failed to set StageRequest complete',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.setStageComplete: Exception when setting StageRequest complete.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  ####################################################################
  #
  # The state transition of the Replicas from Staged->Pinned
  #

  types_getStagedReplicas = []
  def export_getStagedReplicas(self):
    """
        This method obtains the replicas and SRM request information which are in the Staged status
    """
    try:
      res = stagerDB.getStagedReplicas()
      if res['OK']:
        gLogger.info('StagerHandler.getStagedReplicas: Successfully obtained Staged replicas')
      else:
        gLogger.error('StagerHandler.getStagedReplicas: Failed to obtain Staged replicas',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getStagedReplicas: Exception when obtaining Staged replicas.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_insertPinRequest = [DictType,IntType]
  def export_insertPinRequest(self,requestReplicas,pinLifeTime):
    """
        This method inserts pins for the supplied replicaIDs with the supplied lifetime
    """
    try:
      res = stagerDB.insertPinRequest(requestReplicas,pinLifeTime)
      if res['OK']:
        gLogger.info('StagerHandler.insertPinRequest: Successfully inserted pin information')
      else:
        gLogger.error('StagerHandler.insertPinRequest: Failed to insert pin information',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.insertPinRequest: Exception when inserting pin information'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  ####################################################################
  #
  # The methods for finalization of tasks
  #

  types_updateStageCompletingTasks = []
  def export_updateStageCompletingTasks(self):
    """
        This method checks whether the file for Tasks in 'StageCompleting' status are all Staged and updates the Task status to Staged
    """
    try:
      res = stagerDB.updateStageCompletingTasks()
      if res['OK']:
        gLogger.info('StagerHandler.updateStageCompletingTasks: Successfully updated %s StageCompleting tasks.' % len(res['Value']))
      else:
        gLogger.error('StagerHandler.updateStageCompletingTasks: Failed to update StageCompleting tasks.',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.updateStageCompletingTasks: Exception when updating StageCompleting tasks.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_setTasksDone = [ListType]
  def export_setTasksDone(self,taskIDs):
    """
        This method sets the status in the Tasks table to Done for the list of supplied task IDs.
    """
    try:
      res = stagerDB.setTasksDone(taskIDs)
      if res['OK']:
        gLogger.info('StagerHandler.setTasksDone: Successfully set status of tasks to Done')
      else:
        gLogger.error('StagerHandler.setTasksDone: Failed to set status of tasks to Done',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.setTasksDone: Exception when updating status of tasks.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_removeTasks = [ListType]
  def export_removeTasks(self,taskIDs):
    """
        This method removes the entries from TaskReplicas and Tasks with the supplied task IDs
    """
    try:
      res = stagerDB.removeTasks(taskIDs)
      if res['OK']:
        gLogger.info('StagerHandler.removeTasks: Successfully removed Tasks')
      else:
        gLogger.error('StagerHandler.removeTasks: Failed to remove Tasks',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.removeTasks: Exception when removing Tasks.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_removeUnlinkedReplicas = []
  def export_removeUnlinkedReplicas(self):
    """
        This method removes Replicas which have no associated Tasks
    """
    try:
      res = stagerDB.removeUnlinkedReplicas()
      if res['OK']:
        gLogger.info('StagerHandler.removeUnlinkedReplicas: Successfully removed unlinked Replicas')
      else:
        gLogger.error('StagerHandler.removeUnlinkedReplicas: Failed to remove unlinked Replicas',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.removeUnlinkedReplicas: Exception when removing unlinked Replicas.'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  ####################################################################
  #
  # The state transition of the Replicas from *->Failed
  #

  types_updateReplicaFailure = [DictType]
  def export_updateReplicaFailure(self,replicaFailures):
    """
        This method sets the status of the replica to failed with the supplied reason
    """
    try:
      res = stagerDB.updateReplicaFailure(replicaFailures)
      if res['OK']:
        gLogger.info('StagerHandler.updateReplicaFailure: Successfully updated replica failure information')
      else:
        gLogger.error('StagerHandler.updateRelicaFailure: Failed to update replica failure information',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.updateReplicaFailure: Exception when updating replica failure information'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  ####################################################################
  #
  # General methods for obtaining Tasks, Replicas with supplied state
  #

  types_getTasksWithStatus = [StringType]
  def export_getTasksWithStatus(self,status):
    """
        This method allows to retrieve Tasks with the supplied status
    """
    try:
      res = stagerDB.getTasksWithStatus(status)
      if res['OK']:
        gLogger.info('StagerHandler.getTasksWithStatus: Successfully got tasks with %s status' % status)
      else:
        gLogger.error('StagerHandler.getTasksWithStatus: Failed to get tasks with %s status' % status,res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getTasksWithStatus: Exception when getting tasks with %s status' % status
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_getReplicasWithStatus = [StringType]
  def export_getReplicasWithStatus(self,status):
    """
        This method allows to retrieve replicas with the supplied status
    """
    try:
      res = stagerDB.getReplicasWithStatus(status)
      if res['OK']:
        gLogger.info('StagerHandler.getReplicasWithStatus: Successfully got replicas with %s status' % status)
      else:
        gLogger.error('StagerHandler.getReplicasWithStatus: Failed to get replicas with %s status' % status,res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getReplicasWithStatus: Exception when getting replicas with %s status' % status
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)
