""" StorageManagerHandler is the implementation of the StorageManagementDB in the DISET framework """


from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.StorageManagementSystem.DB.StorageManagementDB import StorageManagementDB


class StorageManagerHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """Initialization of DB object"""

        cls.storageManagementDB = StorageManagementDB()
        return S_OK()

    ######################################################################
    #
    #  Example call back methods
    #

    types_updateTaskStatus = []

    @classmethod
    def export_updateTaskStatus(cls, sourceID, status, successful=[], failed=[]):
        """An example to show the usage of the callbacks."""
        gLogger.info("updateTaskStatus: Received callback information for ID %s" % sourceID)
        gLogger.info("updateTaskStatus: Status = '%s'" % status)
        if successful:
            gLogger.info("updateTaskStatus: %s files successfully staged" % len(successful))
            for lfn, time in successful:
                gLogger.info(f"updateTaskStatus: {lfn.ljust(100)} {time.ljust(10)}")
        if failed:
            gLogger.info("updateTaskStatus: %s files failed to stage" % len(successful))
            for lfn, time in failed:
                gLogger.info(f"updateTaskStatus: {lfn.ljust(100)} {time.ljust(10)}")
        return S_OK()

    ######################################################################
    #
    #  Monitoring methods
    #

    types_getTaskStatus = [(int,)]

    @classmethod
    def export_getTaskStatus(cls, taskID):
        """Obtain the status of the stage task from the DB."""
        res = cls.storageManagementDB.getTaskStatus(taskID)
        if not res["OK"]:
            gLogger.error("getTaskStatus: Failed to get task status", res["Message"])
        return res

    types_getTaskInfo = [(int,)]

    @classmethod
    def export_getTaskInfo(cls, taskID):
        """Obtain the metadata of the stage task from the DB."""
        res = cls.storageManagementDB.getTaskInfo(taskID)
        if not res["OK"]:
            gLogger.error("getTaskInfo: Failed to get task metadata", res["Message"])
        return res

    types_getTaskSummary = [(int,)]

    @classmethod
    def export_getTaskSummary(cls, taskID):
        """Obtain the summary of the stage task from the DB."""
        res = cls.storageManagementDB.getTaskSummary(taskID)
        if not res["OK"]:
            gLogger.error("getTaskSummary: Failed to get task summary", res["Message"])
        return res

    types_getTasks = [dict]

    @classmethod
    def export_getTasks(cls, condDict, older=None, newer=None, timeStamp="LastUpdate", orderAttribute=None, limit=None):
        """Get the replicas known to the DB."""
        res = cls.storageManagementDB.getTasks(
            condDict=condDict, older=older, newer=newer, timeStamp=timeStamp, orderAttribute=orderAttribute, limit=limit
        )
        if not res["OK"]:
            gLogger.error("getTasks: Failed to get Cache replicas", res["Message"])
        return res

    types_removeStageRequests = [list]

    @classmethod
    def export_removeStageRequests(cls, replicaIDs):
        res = cls.storageManagementDB.removeStageRequests(replicaIDs)
        if not res["OK"]:
            gLogger.error("removeStageRequests: Failed to remove StageRequests", res["Message"])
        return res

    types_getCacheReplicas = [dict]

    @classmethod
    def export_getCacheReplicas(
        cls, condDict, older=None, newer=None, timeStamp="LastUpdate", orderAttribute=None, limit=None
    ):
        """Get the replcias known to the DB."""
        res = cls.storageManagementDB.getCacheReplicas(
            condDict=condDict, older=older, newer=newer, timeStamp=timeStamp, orderAttribute=orderAttribute, limit=limit
        )
        if not res["OK"]:
            gLogger.error("getCacheReplicas: Failed to get Cache replicas", res["Message"])
        return res

    types_getStageRequests = [dict]

    @classmethod
    def export_getStageRequests(
        cls, condDict, older=None, newer=None, timeStamp="StageRequestSubmitTime", orderAttribute=None, limit=None
    ):
        """Get the replcias known to the DB."""
        res = cls.storageManagementDB.getStageRequests(
            condDict=condDict, older=older, newer=newer, timeStamp=timeStamp, orderAttribute=orderAttribute, limit=limit
        )
        if not res["OK"]:
            gLogger.error("getStageRequests: Failed to get Cache replicas", res["Message"])
        return res

    #
    #                                                Monitoring methods
    #
    ######################################################################

    ####################################################################
    #
    # setRequest is used to initially insert tasks and their associated files. Leaves files in New status.
    #

    types_setRequest = [dict, (str,), (str,), (int,)]

    @classmethod
    def export_setRequest(cls, lfnDict, source, callbackMethod, taskID):
        """This method allows stage requests to be set into the StagerDB"""
        res = cls.storageManagementDB.setRequest(lfnDict, source, callbackMethod, taskID)
        if not res["OK"]:
            gLogger.error("setRequest: Failed to set stage request", res["Message"])
        return res

    ####################################################################
    #
    # The state transition of Replicas method
    #

    types_updateReplicaStatus = [list, (str,)]

    @classmethod
    def export_updateReplicaStatus(cls, replicaIDs, newReplicaStatus):
        """This allows to update the status of replicas"""
        res = cls.storageManagementDB.updateReplicaStatus(replicaIDs, newReplicaStatus)
        if not res["OK"]:
            gLogger.error("updateReplicaStatus: Failed to update replica status", res["Message"])
        return res

    ####################################################################
    #
    # The state transition of the Replicas from New->Waiting
    #

    types_updateReplicaInformation = [list]

    @classmethod
    def export_updateReplicaInformation(cls, replicaTuples):
        """This method sets the pfn and size for the supplied replicas"""
        res = cls.storageManagementDB.updateReplicaInformation(replicaTuples)
        if not res["OK"]:
            gLogger.error("updateRelicaInformation: Failed to update replica information", res["Message"])
        return res

    ####################################################################
    #
    # The state transition of the Replicas from Waiting->StageSubmitted
    #
    types_getStagedReplicas = []

    @classmethod
    def export_getStagedReplicas(cls):
        """This method obtains the replicas for which all replicas in the task are Staged/StageSubmitted"""
        res = cls.storageManagementDB.getStagedReplicas()
        if not res["OK"]:
            gLogger.error("getStagedReplicas: Failed to obtain Staged/StageSubmitted replicas", res["Message"])
        return res

    types_getWaitingReplicas = []

    @classmethod
    def export_getWaitingReplicas(cls):
        """This method obtains the replicas for which all replicas in the task are Waiting"""
        res = cls.storageManagementDB.getWaitingReplicas()
        if not res["OK"]:
            gLogger.error("getWaitingReplicas: Failed to obtain Waiting replicas", res["Message"])
        return res

    types_getOfflineReplicas = []

    @classmethod
    def export_getOfflineReplicas(cls):
        """This method obtains the replicas for which all replicas in the task are Offline"""
        res = cls.storageManagementDB.getOfflineReplicas()
        if not res["OK"]:
            gLogger.error("getOfflineReplicas: Failed to obtain Offline replicas", res["Message"])
        return res

    types_getSubmittedStagePins = []

    @classmethod
    def export_getSubmittedStagePins(cls):
        """This method obtains the number of files and size of the requests submitted for each storage element"""
        res = cls.storageManagementDB.getSubmittedStagePins()
        if not res["OK"]:
            gLogger.error("getSubmittedStagePins: Failed to obtain submitted request summary", res["Message"])
        return res

    types_insertStageRequest = [dict, [(int,), (int,)]]

    @classmethod
    def export_insertStageRequest(cls, requestReplicas, pinLifetime):
        """This method inserts the stage request ID assocaited to supplied replicaIDs"""
        res = cls.storageManagementDB.insertStageRequest(requestReplicas, pinLifetime)
        if not res["OK"]:
            gLogger.error("insertStageRequest: Failed to insert stage request information", res["Message"])
        return res

    ####################################################################
    #
    # The state transition of the Replicas from StageSubmitted->Staged
    #

    types_setStageComplete = [list]

    @classmethod
    def export_setStageComplete(cls, replicaIDs):
        """This method updates the status of the stage request for the supplied replica IDs"""
        res = cls.storageManagementDB.setStageComplete(replicaIDs)
        if not res["OK"]:
            gLogger.error("setStageComplete: Failed to set StageRequest complete", res["Message"])
        return res

    ####################################################################
    #
    # The methods for finalization of tasks
    #
    # Daniela: useless method
    '''types_updateStageCompletingTasks = []
  def export_updateStageCompletingTasks(self):
    """ This method checks whether the file for Tasks in 'StageCompleting' status
    are all Staged and updates the Task status to Staged """
    res = cls.storageManagementDB.updateStageCompletingTasks()
    if not res['OK']:
      gLogger.error('updateStageCompletingTasks: Failed to update StageCompleting tasks.',res['Message'])
    return res
  '''

    types_setTasksDone = [list]

    @classmethod
    def export_setTasksDone(cls, taskIDs):
        """This method sets the status in the Tasks table to Done for the list of supplied task IDs"""
        res = cls.storageManagementDB.setTasksDone(taskIDs)
        if not res["OK"]:
            gLogger.error("setTasksDone: Failed to set status of tasks to Done", res["Message"])
        return res

    types_removeTasks = [list]

    @classmethod
    def export_removeTasks(cls, taskIDs):
        """This method removes the entries from TaskReplicas and Tasks with the supplied task IDs"""
        res = cls.storageManagementDB.removeTasks(taskIDs)
        if not res["OK"]:
            gLogger.error("removeTasks: Failed to remove Tasks", res["Message"])
        return res

    types_removeUnlinkedReplicas = []

    @classmethod
    def export_removeUnlinkedReplicas(cls):
        """This method removes Replicas which have no associated Tasks"""
        res = cls.storageManagementDB.removeUnlinkedReplicas()
        if not res["OK"]:
            gLogger.error("removeUnlinkedReplicas: Failed to remove unlinked Replicas", res["Message"])
        return res

    ####################################################################
    #
    # The state transition of the Replicas from *->Failed
    #

    types_updateReplicaFailure = [dict]

    @classmethod
    def export_updateReplicaFailure(cls, replicaFailures):
        """This method sets the status of the replica to failed with the supplied reason"""
        res = cls.storageManagementDB.updateReplicaFailure(replicaFailures)
        if not res["OK"]:
            gLogger.error("updateRelicaFailure: Failed to update replica failure information", res["Message"])
        return res

    ####################################################################
    #
    # Methods for obtaining Tasks, Replicas with supplied state
    #

    types_getTasksWithStatus = [(str,)]

    @classmethod
    def export_getTasksWithStatus(cls, status):
        """This method allows to retrieve Tasks with the supplied status"""
        res = cls.storageManagementDB.getTasksWithStatus(status)
        if not res["OK"]:
            gLogger.error("getTasksWithStatus: Failed to get tasks with %s status" % status, res["Message"])
        return res

    types_getReplicasWithStatus = [(str,)]

    @classmethod
    def export_getReplicasWithStatus(cls, status):
        """This method allows to retrieve replicas with the supplied status"""
        res = cls.storageManagementDB.getCacheReplicas({"Status": status})
        if not res["OK"]:
            gLogger.error("getReplicasWithStatus: Failed to get replicas with %s status" % status, res["Message"])
        return res

    types_getStageSubmittedReplicas = []

    @classmethod
    def export_getStageSubmittedReplicas(cls):
        """This method obtains the replica metadata and the stage requestID for the replicas in StageSubmitted status"""
        res = cls.storageManagementDB.getCacheReplicas({"Status": "StageSubmitted"})
        if not res["OK"]:
            gLogger.error("getStageSubmittedReplicas: Failed to obtain StageSubmitted replicas", res["Message"])
        return res

    types_wakeupOldRequests = [list, (int,)]

    @classmethod
    def export_wakeupOldRequests(cls, oldRequests, retryInterval):
        """get only StageRequests with StageRequestSubmitTime older than 1 day AND are still not staged
        delete these requests
        reset Replicas with corresponding ReplicaIDs to Status='New'
        """
        res = cls.storageManagementDB.wakeupOldRequests(oldRequests, retryInterval)
        if not res["OK"]:
            gLogger.error("wakeupOldRequests: Failed to wake up old requests", res["Message"])
        return res

    types_setOldTasksAsFailed = [(int,)]

    @classmethod
    def export_setOldTasksAsFailed(cls, daysOld):
        """
        Set Tasks older than "daysOld" number of days to Failed
        These tasks have already been retried every day for staging
        """
        res = cls.storageManagementDB.setOldTasksAsFailed(daysOld)
        if not res["OK"]:
            gLogger.error("setOldTasksAsFailed: Failed to set old Tasks to Failed state. ", res["Message"])
        return res

    types_getAssociatedReplicas = [list]

    @classmethod
    def export_getAssociatedReplicas(cls, replicaIDs):
        """
        Retrieve the list of Replicas that belong to the same Tasks as the provided list
        """
        res = cls.storageManagementDB.getAssociatedReplicas(replicaIDs)
        if not res["OK"]:
            gLogger.error("getAssociatedReplicas: Failed to get Associated Replicas. ", res["Message"])
        return res

    types_killTasksBySourceTaskID = [list]

    @classmethod
    def export_killTasksBySourceTaskID(cls, sourceTaskIDs):
        """Given SourceTaskIDs (jobIDs), this will cancel further staging of files for the corresponding tasks"""
        res = cls.storageManagementDB.killTasksBySourceTaskID(sourceTaskIDs)
        if not res["OK"]:
            gLogger.error("removeTasks: Failed to kill staging", res["Message"])
        return res

    types_getCacheReplicasSummary = []

    @classmethod
    def export_getCacheReplicasSummary(cls):
        """Reports breakdown of file number/size in different staging states across storage elements"""
        res = cls.storageManagementDB.getCacheReplicasSummary()
        if not res["OK"]:
            gLogger.error(" getCacheReplicasSummary: Failed to retrieve summary from server", res["Message"])
        return res
