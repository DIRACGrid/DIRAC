""" StorageManagerHandler is the implementation of the StorageManagementDB in the DISET framework """

__RCSID__ = "$Id$"

from types import IntType, DictType, ListType, StringType, LongType
from DIRAC                                                 import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler                       import RequestHandler
from DIRAC.StorageManagementSystem.DB.StorageManagementDB  import StorageManagementDB
# This is a global instance of the StorageDB
storageDB = False

def initializeStorageManagerHandler( serviceInfo ):
  global storageDB
  storageDB = StorageManagementDB()
  return storageDB._checkTable()
  

class StorageManagerHandler( RequestHandler ):

  ######################################################################
  #
  #  Example call back methods
  #

  types_updateTaskStatus = []
  def export_updateTaskStatus( self, sourceID, status, successful = [], failed = [] ):
    """ An example to show the usage of the callbacks. """
    gLogger.info( "updateTaskStatus: Received callback information for ID %s" % sourceID )
    gLogger.info( "updateTaskStatus: Status = '%s'" % status )
    if successful:
      gLogger.info( "updateTaskStatus: %s files successfully staged" % len( successful ) )
      for lfn, time in successful:
        gLogger.info( "updateTaskStatus: %s %s" % ( lfn.ljust( 100 ), time.ljust( 10 ) ) )
    if failed:
      gLogger.info( "updateTaskStatus: %s files failed to stage" % len( successful ) )
      for lfn, time in failed:
        gLogger.info( "updateTaskStatus: %s %s" % ( lfn.ljust( 100 ), time.ljust( 10 ) ) )
    return S_OK()

  ######################################################################
  #
  #  Monitoring methods
  #

  types_getTaskStatus = [IntType]
  def export_getTaskStatus( self, taskID ):
    """ Obtain the status of the stage task from the DB. """
    res = storageDB.getTaskStatus( taskID )
    if not res['OK']:
      gLogger.error( 'getTaskStatus: Failed to get task status', res['Message'] )
    return res

  types_getTaskInfo = [IntType]
  def export_getTaskInfo( self, taskID ):
    """ Obtain the metadata of the stage task from the DB. """
    res = storageDB.getTaskInfo( taskID )
    if not res['OK']:
      gLogger.error( 'getTaskInfo: Failed to get task metadata', res['Message'] )
    return res

  types_getTaskSummary = [IntType]
  def export_getTaskSummary( self, taskID ):
    """ Obtain the summary of the stage task from the DB. """
    res = storageDB.getTaskSummary( taskID )
    if not res['OK']:
      gLogger.error( 'getTaskSummary: Failed to get task summary', res['Message'] )
    return res

  types_getTasks = [DictType]
  def export_getTasks( self, condDict, older = None, newer = None, timeStamp = 'LastUpdate', orderAttribute = None, limit = None ):
    """ Get the replicas known to the DB. """
    res = storageDB.getTasks( condDict = condDict, older = older, newer = newer, timeStamp = timeStamp, orderAttribute = orderAttribute, limit = limit )
    if not res['OK']:
      gLogger.error( 'getTasks: Failed to get Cache replicas', res['Message'] )
    return res

  types_removeStageRequests = [ListType]
  def export_removeStageRequests( self, replicaIDs):
    res = storageDB.removeStageRequests( replicaIDs )
    if not res['OK']:
      gLogger.error( 'removeStageRequests: Failed to remove StageRequests', res['Message'] )
    return res
      
  types_getCacheReplicas = [DictType]
  def export_getCacheReplicas( self, condDict, older = None, newer = None, timeStamp = 'LastUpdate', orderAttribute = None, limit = None ):
    """ Get the replcias known to the DB. """
    res = storageDB.getCacheReplicas( condDict = condDict, older = older, newer = newer, timeStamp = timeStamp, orderAttribute = orderAttribute, limit = limit )
    if not res['OK']:
      gLogger.error( 'getCacheReplicas: Failed to get Cache replicas', res['Message'] )
    return res

  types_getStageRequests = [DictType]
  def export_getStageRequests( self, condDict, older = None, newer = None, timeStamp = 'StageRequestSubmitTime', orderAttribute = None, limit = None ):
    """ Get the replcias known to the DB. """
    res = storageDB.getStageRequests( condDict = condDict, older = older, newer = newer, timeStamp = timeStamp, orderAttribute = orderAttribute, limit = limit )
    if not res['OK']:
      gLogger.error( 'getStageRequests: Failed to get Cache replicas', res['Message'] )
    return res
  #
  #                                                Monitoring methods
  #
  ######################################################################


  ####################################################################
  #
  # setRequest is used to initially insert tasks and their associated files. Leaves files in New status.
  #

  types_setRequest = [DictType, StringType, StringType, IntType]
  def export_setRequest( self, lfnDict, source, callbackMethod, taskID ):
    """ This method allows stage requests to be set into the StagerDB """
    res = storageDB.setRequest( lfnDict, source, callbackMethod, taskID )
    if not res['OK']:
      gLogger.error( 'setRequest: Failed to set stage request', res['Message'] )
    return res

  ####################################################################
  #
  # The state transition of Replicas method
  #

  types_updateReplicaStatus = [ListType, StringType]
  def export_updateReplicaStatus( self, replicaIDs, newReplicaStatus ):
    """ This allows to update the status of replicas """
    res = storageDB.updateReplicaStatus( replicaIDs, newReplicaStatus )
    if not res['OK']:
      gLogger.error( 'updateReplicaStatus: Failed to update replica status', res['Message'] )
    return res

  ####################################################################
  #
  # The state transition of the Replicas from New->Waiting
  #

  types_updateReplicaInformation = [ListType]
  def export_updateReplicaInformation( self, replicaTuples ):
    """ This method sets the pfn and size for the supplied replicas """
    res = storageDB.updateReplicaInformation( replicaTuples )
    if not res['OK']:
      gLogger.error( 'updateRelicaInformation: Failed to update replica information', res['Message'] )
    return res

  ####################################################################
  #
  # The state transition of the Replicas from Waiting->StageSubmitted
  #
  types_getStagedReplicas = []
  def export_getStagedReplicas( self ):
    """ This method obtains the replicas for which all replicas in the task are Staged/StageSubmitted """
    res = storageDB.getStagedReplicas()
    if not res['OK']:
      gLogger.error( 'getStagedReplicas: Failed to obtain Staged/StageSubmitted replicas', res['Message'] )
    return res

  types_getWaitingReplicas = []
  def export_getWaitingReplicas( self ):
    """ This method obtains the replicas for which all replicas in the task are Waiting """
    res = storageDB.getWaitingReplicas()
    if not res['OK']:
      gLogger.error( 'getWaitingReplicas: Failed to obtain Waiting replicas', res['Message'] )
    return res

  types_getOfflineReplicas = []
  def export_getOfflineReplicas( self ):
    """ This method obtains the replicas for which all replicas in the task are Offline """
    res = storageDB.getOfflineReplicas()
    if not res['OK']:
      gLogger.error( 'getOfflineReplicas: Failed to obtain Offline replicas', res['Message'] )
    return res

  types_getSubmittedStagePins = []
  def export_getSubmittedStagePins( self ):
    """ This method obtains the number of files and size of the requests submitted for each storage element """
    res = storageDB.getSubmittedStagePins()
    if not res['OK']:
      gLogger.error( 'getSubmittedStagePins: Failed to obtain submitted request summary', res['Message'] )
    return res

  types_insertStageRequest = [DictType, [IntType, LongType]]
  def export_insertStageRequest( self, requestReplicas, pinLifetime ):
    """ This method inserts the stage request ID assocaited to supplied replicaIDs """
    res = storageDB.insertStageRequest( requestReplicas, pinLifetime )
    if not res['OK']:
      gLogger.error( 'insertStageRequest: Failed to insert stage request information', res['Message'] )
    return res

  ####################################################################
  #
  # The state transition of the Replicas from StageSubmitted->Staged
  #

  types_setStageComplete = [ListType]
  def export_setStageComplete( self, replicaIDs ):
    """ This method updates the status of the stage request for the supplied replica IDs """
    res = storageDB.setStageComplete( replicaIDs )
    if not res['OK']:
      gLogger.error( 'setStageComplete: Failed to set StageRequest complete', res['Message'] )
    return res

  ####################################################################
  #
  # The methods for finalization of tasks
  #
  # Daniela: useless method
  '''types_updateStageCompletingTasks = []
  def export_updateStageCompletingTasks(self):
    """ This method checks whether the file for Tasks in 'StageCompleting' status are all Staged and updates the Task status to Staged """
    res = storageDB.updateStageCompletingTasks()
    if not res['OK']:
      gLogger.error('updateStageCompletingTasks: Failed to update StageCompleting tasks.',res['Message'])
    return res
  '''

  types_setTasksDone = [ListType]
  def export_setTasksDone( self, taskIDs ):
    """ This method sets the status in the Tasks table to Done for the list of supplied task IDs """
    res = storageDB.setTasksDone( taskIDs )
    if not res['OK']:
      gLogger.error( 'setTasksDone: Failed to set status of tasks to Done', res['Message'] )
    return res

  types_removeTasks = [ListType]
  def export_removeTasks( self, taskIDs ):
    """ This method removes the entries from TaskReplicas and Tasks with the supplied task IDs """
    res = storageDB.removeTasks( taskIDs )
    if not res['OK']:
      gLogger.error( 'removeTasks: Failed to remove Tasks', res['Message'] )
    return res

  types_removeUnlinkedReplicas = []
  def export_removeUnlinkedReplicas( self ):
    """ This method removes Replicas which have no associated Tasks """
    res = storageDB.removeUnlinkedReplicas()
    if not res['OK']:
      gLogger.error( 'removeUnlinkedReplicas: Failed to remove unlinked Replicas', res['Message'] )
    return res

  ####################################################################
  #
  # The state transition of the Replicas from *->Failed
  #

  types_updateReplicaFailure = [DictType]
  def export_updateReplicaFailure( self, replicaFailures ):
    """ This method sets the status of the replica to failed with the supplied reason """
    res = storageDB.updateReplicaFailure( replicaFailures )
    if not res['OK']:
      gLogger.error( 'updateRelicaFailure: Failed to update replica failure information', res['Message'] )
    return res

  ####################################################################
  #
  # Methods for obtaining Tasks, Replicas with supplied state
  #

  types_getTasksWithStatus = [StringType]
  def export_getTasksWithStatus( self, status ):
    """ This method allows to retrieve Tasks with the supplied status """
    res = storageDB.getTasksWithStatus( status )
    if not res['OK']:
      gLogger.error( 'getTasksWithStatus: Failed to get tasks with %s status' % status, res['Message'] )
    return res

  types_getReplicasWithStatus = [StringType]
  def export_getReplicasWithStatus( self, status ):
    """ This method allows to retrieve replicas with the supplied status """
    res = storageDB.getCacheReplicas( {'Status':status} )
    if not res['OK']:
      gLogger.error( 'getReplicasWithStatus: Failed to get replicas with %s status' % status, res['Message'] )
    return res

  types_getStageSubmittedReplicas = []
  def export_getStageSubmittedReplicas( self ):
    """ This method obtains the replica metadata and the stage requestID for the replicas in StageSubmitted status """
    res = storageDB.getCacheReplicas( {'Status':'StageSubmitted'} )
    if not res['OK']:
      gLogger.error( 'getStageSubmittedReplicas: Failed to obtain StageSubmitted replicas', res['Message'] )
    return res

  types_wakeupOldRequests = [ListType, IntType ]
  def export_wakeupOldRequests( self, oldRequests, retryInterval ):
    """  get only StageRequests with StageRequestSubmitTime older than 1 day AND are still not staged
    delete these requests
    reset Replicas with corresponding ReplicaIDs to Status='New'
    """
    res = storageDB.wakeupOldRequests( oldRequests, retryInterval )
    if not res['OK']:
      gLogger.error( 'wakeupOldRequests: Failed to wake up old requests', res['Message'] )
    return res

  types_setOldTasksAsFailed = [IntType]
  def export_setOldTasksAsFailed( self, daysOld ):
    """
    Set Tasks older than "daysOld" number of days to Failed
    These tasks have already been retried every day for staging
    """
    res = storageDB.setOldTasksAsFailed( daysOld )
    if not res['OK']:
      gLogger.error( 'setOldTasksAsFailed: Failed to set old Tasks to Failed state. ', res['Message'] )
    return res

  types_getAssociatedReplicas = [ListType]
  def export_getAssociatedReplicas( self, replicaIDs ):
    """
    Retrieve the list of Replicas that belong to the same Tasks as the provided list
    """
    res = storageDB.getAssociatedReplicas( replicaIDs )
    if not res['OK']:
      gLogger.error( 'getAssociatedReplicas: Failed to get Associated Replicas. ', res['Message'] )
    return res

  types_killTasksBySourceTaskID = [ListType]
  def export_killTasksBySourceTaskID(self, sourceTaskIDs ):
    """ Given SourceTaskIDs (jobIDs), this will cancel further staging of files for the corresponding tasks"""
    res = storageDB.killTasksBySourceTaskID( sourceTaskIDs )
    if not res['OK']:
      gLogger.error( 'removeTasks: Failed to kill staging', res['Message'] )
    return res

  types_getCacheReplicasSummary = []
  def export_getCacheReplicasSummary(self):
    """ Reports breakdown of file number/size in different staging states across storage elements """
    res = storageDB.getCacheReplicasSummary()
    if not res['OK']:
      gLogger.error(' getCacheReplicasSummary: Failed to retrieve summary from server', res['Message'])
    return res

