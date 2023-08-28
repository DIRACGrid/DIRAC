""" StorageManagementDB is a front end to the Stager Database.

    There are five tables in the StorageManagementDB: Tasks, CacheReplicas, TaskReplicas, StageRequests.

    The Tasks table is the place holder for the tasks that have requested files to be staged.
    These can be from different systems and have different associated call back methods.
    The CacheReplicas table keeps the information on all the CacheReplicas in the system.
    It maps all the file information LFN, PFN, SE to an assigned ReplicaID.
    The TaskReplicas table maps the TaskIDs from the Tasks table to the ReplicaID from the CacheReplicas table.
    The StageRequests table contains each of the prestage request IDs for each of the replicas.
"""
import inspect
import threading

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.List import intListToString, stringListToString

# Stage Request are issue with a length of "PinLength"
# However, once Staged, the entry in the StageRequest will set a PinExpiryTime only for "PinLength" / THROTTLING_STEPS
# As PinExpiryTime arrives, StageRequest and their corresponding CacheReplicas entries are cleaned
# This allows to throttle the submission of Stage Requests up to a maximum of "DiskCacheTB" per "PinLength"
# After "PinLength" / THROTTLING_STEPS seconds, entries are removed, so new requests for the same replica will trigger
# a new Stage Request to the SE, and thus an update of the Pinning on the SE.
#
#  - "PinLength" is an Option of the StageRequest Agent that defaults to THROTTLING_TIME
#  - "DiskCacheTB" is an Option of the StorageElement that defaults to 1 (TB)
#
THROTTLING_TIME = 86400
THROTTLING_STEPS = 12


class StorageManagementDB(DB):
    def __init__(self, systemInstance="Default", parentLogger=None):
        DB.__init__(self, "StorageManagementDB", "StorageManagement/StorageManagementDB", parentLogger=parentLogger)
        self.lock = threading.Lock()
        self.TASKPARAMS = [
            "TaskID",
            "Status",
            "Source",
            "SubmitTime",
            "LastUpdate",
            "CompleteTime",
            "CallBackMethod",
            "SourceTaskID",
        ]
        self.REPLICAPARAMS = [
            "ReplicaID",
            "Type",
            "Status",
            "SE",
            "LFN",
            "PFN",
            "Size",
            "FileChecksum",
            "GUID",
            "SubmitTime",
            "LastUpdate",
            "Reason",
            "Links",
        ]
        self.STAGEPARAMS = [
            "ReplicaID",
            "StageStatus",
            "RequestID",
            "StageRequestSubmitTime",
            "StageRequestCompletedTime",
            "PinLength",
            "PinExpiryTime",
        ]
        self.STATES = ["Failed", "New", "Waiting", "Offline", "StageSubmitted", "Staged"]

    def __getConnection(self, connection):
        if connection:
            return connection
        res = self._getConnection()
        if res["OK"]:
            return res["Value"]
        gLogger.warn("Failed to get MySQL connection", res["Message"])
        return connection

    def _caller(self):
        return inspect.stack()[2][3]

    ################################################################
    #
    # State machine management
    #

    def updateTaskStatus(self, taskIDs, newTaskStatus, connection=False):
        return self.__updateTaskStatus(taskIDs, newTaskStatus, connection=connection)

    def __updateTaskStatus(self, taskIDs, newTaskStatus, force=False, connection=False):
        connection = self.__getConnection(connection)
        if not taskIDs:
            return S_OK(taskIDs)
        if force:
            toUpdate = taskIDs
        else:
            res = self._checkTaskUpdate(taskIDs, newTaskStatus, connection=connection)
            if not res["OK"]:
                return res
            toUpdate = res["Value"]
        if not toUpdate:
            return S_OK(toUpdate)

        # reqSelect = "SELECT * FROM Tasks WHERE TaskID IN (%s) AND Status != '%s';" % (
        #     intListToString( toUpdate ), newTaskStatus
        # )
        reqSelect = "SELECT TaskID FROM Tasks WHERE TaskID IN ({}) AND Status != '{}';".format(
            intListToString(toUpdate),
            newTaskStatus,
        )
        resSelect = self._query(reqSelect, conn=connection)
        if not resSelect["OK"]:
            gLogger.error(
                f"{self._caller()}.__updateTaskStatus_DB: problem retrieving record:",
                f"{reqSelect}. {resSelect['Message']}",
            )

        req = "UPDATE Tasks SET Status='{}',LastUpdate=UTC_TIMESTAMP() WHERE TaskID IN ({}) AND Status != '{}';".format(
            newTaskStatus,
            intListToString(toUpdate),
            newTaskStatus,
        )
        res = self._update(req, conn=connection)
        if not res["OK"]:
            return res

        taskIDs = []
        for record in resSelect["Value"]:
            taskIDs.append(record[0])
            gLogger.verbose(f"{self._caller()}.__updateTaskStatus_DB: to_update Tasks =  {record}")

        if taskIDs:
            reqSelect1 = f"SELECT * FROM Tasks WHERE TaskID IN ({intListToString(taskIDs)});"
            resSelect1 = self._query(reqSelect1, conn=connection)
            if not resSelect1["OK"]:
                gLogger.warn(
                    "%s.%s_DB: problem retrieving records: %s. %s"
                    % (self._caller(), "__updateTaskStatus", reqSelect1, resSelect1["Message"])
                )
            else:
                for record in resSelect1["Value"]:
                    gLogger.verbose(f"{self._caller()}.__updateTaskStatus_DB: updated Tasks = {record}")

        return S_OK(toUpdate)

    def _checkTaskUpdate(self, taskIDs, newTaskState, connection=False):
        connection = self.__getConnection(connection)
        if not taskIDs:
            return S_OK(taskIDs)
        # * -> Failed
        if newTaskState == "Failed":
            oldTaskState = []
        # StageCompleting -> Done
        elif newTaskState == "Done":
            oldTaskState = ["StageCompleting"]
        # StageSubmitted -> StageCompleting
        elif newTaskState == "StageCompleting":
            oldTaskState = ["StageSubmitted"]
        # Waiting -> StageSubmitted
        elif newTaskState == "StageSubmitted":
            oldTaskState = ["Waiting", "Offline"]
        # New -> Waiting
        elif newTaskState == "Waiting":
            oldTaskState = ["New"]
        elif newTaskState == "Offline":
            oldTaskState = ["Waiting"]
        else:
            return S_ERROR("Task status not recognized")
        if not oldTaskState:
            toUpdate = taskIDs
        else:
            req = "SELECT TaskID FROM Tasks WHERE Status in ({}) AND TaskID IN ({})".format(
                stringListToString(oldTaskState),
                intListToString(taskIDs),
            )
            res = self._query(req, conn=connection)
            if not res["OK"]:
                return res
            toUpdate = [row[0] for row in res["Value"]]
        return S_OK(toUpdate)

    def updateReplicaStatus(self, replicaIDs, newReplicaStatus, connection=False):
        connection = self.__getConnection(connection)
        if not replicaIDs:
            return S_OK(replicaIDs)
        res = self._checkReplicaUpdate(replicaIDs, newReplicaStatus)
        if not res["OK"]:
            return res
        toUpdate = res["Value"]
        if not toUpdate:
            return S_OK(toUpdate)
        # reqSelect = "SELECT * FROM CacheReplicas WHERE ReplicaID IN (%s) AND Status != '%s';" % (
        #     intListToString( toUpdate ), newReplicaStatus
        # )
        reqSelect = "SELECT ReplicaID FROM CacheReplicas WHERE ReplicaID IN ({}) AND Status != '{}';".format(
            intListToString(toUpdate),
            newReplicaStatus,
        )
        resSelect = self._query(reqSelect, conn=connection)
        if not resSelect["OK"]:
            gLogger.error(
                f"{self._caller()}.updateReplicaStatus_DB: problem retrieving record:",
                f"{reqSelect}. {resSelect['Message']}",
            )

        req = (
            "UPDATE CacheReplicas SET Status='%s',LastUpdate=UTC_TIMESTAMP() WHERE ReplicaID IN (%s) AND Status != '%s';"
            % (newReplicaStatus, intListToString(toUpdate), newReplicaStatus)
        )
        res = self._update(req, conn=connection)
        if not res["OK"]:
            return res

        replicaIDs = []
        for record in resSelect["Value"]:
            replicaIDs.append(record[0])
            gLogger.verbose(f"{self._caller()}.updateReplicaStatus_DB: to_update CacheReplicas =  {record}")
        if replicaIDs:
            reqSelect1 = f"SELECT * FROM CacheReplicas WHERE ReplicaID IN ({intListToString(replicaIDs)});"
            resSelect1 = self._query(reqSelect1, conn=connection)
            if not resSelect1["OK"]:
                gLogger.warn(
                    "%s.%s_DB: problem retrieving records: %s. %s"
                    % (self._caller(), "updateReplicaStatus", reqSelect1, resSelect1["Message"])
                )
            else:
                for record in resSelect1["Value"]:
                    gLogger.verbose(f"{self._caller()}.updateReplicaStatus_DB: updated CacheReplicas = {record}")

        res = self._updateTasksForReplica(replicaIDs, connection=connection)
        if not res["OK"]:
            return res
        return S_OK(toUpdate)

    def _updateTasksForReplica(self, replicaIDs, connection=False):
        tasksInStatus = {}
        for state in self.STATES:
            tasksInStatus[state] = []

        req = (
            "SELECT T.TaskID,T.Status FROM Tasks AS T, TaskReplicas AS R WHERE R.ReplicaID IN "
            "( %s ) AND R.TaskID = T.TaskID GROUP BY T.TaskID, T.Status;"
        ) % intListToString(replicaIDs)
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res

        for taskId, status in res["Value"]:
            subreq = (
                "SELECT DISTINCT(C.Status) FROM TaskReplicas AS R, CacheReplicas AS C WHERE R.TaskID=%s AND R.ReplicaID = C.ReplicaID;"
                % taskId
            )
            subres = self._query(subreq, conn=connection)
            if not subres["OK"]:
                return subres

            cacheStatesForTask = [row[0] for row in subres["Value"]]
            if not cacheStatesForTask:
                tasksInStatus["Failed"].append(taskId)
                continue

            wrongState = False
            for state in cacheStatesForTask:
                if state not in self.STATES:
                    wrongState = True
                    break
            if wrongState:
                tasksInStatus["Failed"].append(taskId)
                continue
            for state in self.STATES:
                if state in cacheStatesForTask:
                    if status != state:
                        tasksInStatus[state].append(taskId)
                    break

        for newStatus in tasksInStatus:
            if tasksInStatus[newStatus]:
                res = self.__updateTaskStatus(tasksInStatus[newStatus], newStatus, True, connection=connection)
                if not res["OK"]:
                    gLogger.warn("Failed to update task associated to replicas", res["Message"])
                    # return res
        return S_OK(tasksInStatus)

    def getAssociatedReplicas(self, replicaIDs):
        """Retrieve the list of Replicas that belong to the same Tasks as the provided list"""
        res = self._getReplicaIDTasks(replicaIDs)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.getAssociatedReplicas: Failed to get Tasks.", res["Message"])
            return res
        taskIDs = res["Value"]

        return self.getCacheReplicas({"TaskID": taskIDs})

    def _checkReplicaUpdate(self, replicaIDs, newReplicaState, connection=False):
        connection = self.__getConnection(connection)
        if not replicaIDs:
            return S_OK(replicaIDs)
        # * -> Failed
        if newReplicaState == "Failed":
            oldReplicaState = []
        # New -> Waiting
        elif newReplicaState == "Waiting":
            oldReplicaState = ["New"]
        # Waiting -> StageSubmitted
        elif newReplicaState == "StageSubmitted":
            oldReplicaState = ["Waiting", "Offline"]
        # StageSubmitted -> Staged
        elif newReplicaState == "Staged":
            oldReplicaState = ["StageSubmitted"]
        elif newReplicaState == "Offline":
            oldReplicaState = ["Waiting"]
        else:
            return S_ERROR("Replica status not recognized")
        if not oldReplicaState:
            toUpdate = replicaIDs
        else:
            req = "SELECT ReplicaID FROM CacheReplicas WHERE Status IN ({}) AND ReplicaID IN ({})".format(
                stringListToString(oldReplicaState),
                intListToString(replicaIDs),
            )
            res = self._query(req, conn=connection)
            if not res["OK"]:
                return res
            toUpdate = [row[0] for row in res["Value"]]
        return S_OK(toUpdate)

    def __getTaskStateFromReplicaState(self, replicaState):
        # For the moment the task state just references to the replicaState
        return replicaState

    def updateStageRequestStatus(self, replicaIDs, newStageStatus, connection=False):
        connection = self.__getConnection(connection)
        if not replicaIDs:
            return S_OK(replicaIDs)
        res = self._checkStageUpdate(replicaIDs, newStageStatus, connection=connection)
        if not res["OK"]:
            return res
        toUpdate = res["Value"]
        if not toUpdate:
            return S_OK(toUpdate)
        # reqSelect = "Select * FROM CacheReplicas WHERE ReplicaID IN (%s) AND Status != '%s';" % ( intListToString( toUpdate ), newStageStatus )
        reqSelect = "Select ReplicaID FROM CacheReplicas WHERE ReplicaID IN ({}) AND Status != '{}';".format(
            intListToString(toUpdate),
            newStageStatus,
        )
        resSelect = self._query(reqSelect, conn=connection)
        if not resSelect["OK"]:
            gLogger.warn(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "updateStageRequestStatus", reqSelect, resSelect["Message"])
            )

        req = (
            "UPDATE CacheReplicas SET Status='%s',LastUpdate=UTC_TIMESTAMP() WHERE ReplicaID IN (%s) AND Status != '%s';"
            % (newStageStatus, intListToString(toUpdate), newStageStatus)
        )
        res = self._update(req, conn=connection)
        if not res["OK"]:
            return res

        replicaIDs = []
        for record in resSelect["Value"]:
            replicaIDs.append(record[0])
            gLogger.verbose(f"{self._caller()}.updateStageRequestStatus_DB: to_update CacheReplicas =  {record}")

        reqSelect1 = f"SELECT * FROM CacheReplicas WHERE ReplicaID IN ({intListToString(replicaIDs)});"
        resSelect1 = self._query(reqSelect1, conn=connection)
        if not resSelect1["OK"]:
            gLogger.warn(
                "%s.%s_DB: problem retrieving records: %s. %s"
                % (self._caller(), "updateStageRequestStatus", reqSelect1, resSelect1["Message"])
            )
        else:
            for record in resSelect1["Value"]:
                gLogger.verbose(f"{self._caller()}.updateStageRequestStatus_DB: updated CacheReplicas = {record}")

        # Now update the replicas associated to the replicaIDs
        newReplicaStatus = self.__getReplicaStateFromStageState(newStageStatus)
        res = self.updateReplicaStatus(toUpdate, newReplicaStatus, connection=connection)
        if not res["OK"]:
            gLogger.warn("Failed to update cache replicas associated to stage requests", res["Message"])
        return S_OK(toUpdate)

    def _checkStageUpdate(self, replicaIDs, newStageState, connection=False):
        connection = self.__getConnection(connection)
        if not replicaIDs:
            return S_OK(replicaIDs)
        # * -> Failed
        if newStageState == "Failed":
            oldStageState = []
        elif newStageState == "Staged":
            oldStageState = ["StageSubmitted"]
        else:
            return S_ERROR("StageRequest status not recognized")
        if not oldStageState:
            toUpdate = replicaIDs
        else:
            req = "SELECT ReplicaID FROM StageRequests WHERE StageStatus = '{}' AND ReplicaID IN ({})".format(
                oldStageState,
                intListToString(replicaIDs),
            )
            res = self._query(req, conn=connection)
            if not res["OK"]:
                return res
            toUpdate = [row[0] for row in res["Value"]]
        return S_OK(toUpdate)

    def __getReplicaStateFromStageState(self, stageState):
        # For the moment the replica state just references to the stage state
        return stageState

    #
    #                               End of state machine management
    #
    ################################################################

    ################################################################
    #
    # Monitoring of stage tasks
    #
    def getTaskStatus(self, taskID, connection=False):
        """Obtain the task status from the Tasks table."""
        connection = self.__getConnection(connection)
        res = self.getTaskInfo(taskID, connection=connection)
        if not res["OK"]:
            return res
        taskInfo = res["Value"][taskID]
        return S_OK(taskInfo["Status"])

    def getTaskInfo(self, taskID, connection=False):
        """Obtain all the information from the Tasks table for a supplied task."""
        connection = self.__getConnection(connection)
        req = (
            "SELECT TaskID,Status,Source,SubmitTime,CompleteTime,CallBackMethod,SourceTaskID from Tasks WHERE TaskID IN (%s);"
            % intListToString(taskID)
        )
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.getTaskInfo: Failed to get task information.", res["Message"])
            return res
        resDict = {}
        for taskID, status, source, submitTime, completeTime, callBackMethod, sourceTaskID in res["Value"]:
            resDict[sourceTaskID] = {
                "Status": status,
                "Source": source,
                "SubmitTime": submitTime,
                "CompleteTime": completeTime,
                "CallBackMethod": callBackMethod,
                "SourceTaskID": sourceTaskID,
            }
        if not resDict:
            gLogger.error("StorageManagementDB.getTaskInfo: The supplied task did not exist", taskID)
            return S_ERROR(f"The supplied task {taskID} did not exist")
        return S_OK(resDict)

    def _getTaskIDForJob(self, jobID, connection=False):
        # Stager taskID is retrieved from the source DIRAC jobID
        connection = self.__getConnection(connection)
        req = f"SELECT TaskID from Tasks WHERE SourceTaskID={int(jobID)};"
        res = self._query(req)
        if not res["OK"]:
            gLogger.error(
                f"{self._caller()}._getTaskIDForJob_DB: problem retrieving record:",
                f"{req}. {res['Message']}",
            )
            return S_ERROR("The supplied JobID does not exist!")
        taskID = [row[0] for row in res["Value"]]
        return S_OK(taskID)

    def getTaskSummary(self, jobID, connection=False):
        """Obtain the task summary from the database."""
        connection = self.__getConnection(connection)
        res = self._getTaskIDForJob(jobID, connection=connection)
        if not res["OK"]:
            return res
        if res["Value"]:
            taskID = res["Value"]
        else:
            return S_OK()
        res = self.getTaskInfo(taskID, connection=connection)
        if not res["OK"]:
            return res
        taskInfo = res["Value"]
        req = (
            "SELECT R.LFN,R.SE,R.PFN,R.Size,R.Status,R.LastUpdate,R.Reason FROM CacheReplicas AS R, TaskReplicas AS TR WHERE TR.TaskID in (%s) AND TR.ReplicaID=R.ReplicaID;"
            % intListToString(taskID)
        )
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.getTaskSummary: Failed to get Replica summary for task.", res["Message"])
            return res
        replicaInfo = {}
        for lfn, storageElement, pfn, fileSize, status, lastupdate, reason in res["Value"]:
            replicaInfo[lfn] = {
                "StorageElement": storageElement,
                "PFN": pfn,
                "FileSize": fileSize,
                "Status": status,
                "LastUpdate": lastupdate,
                "Reason": reason,
            }
        resDict = {"TaskInfo": taskInfo, "ReplicaInfo": replicaInfo}
        return S_OK(resDict)

    def getTasks(
        self,
        condDict={},
        older=None,
        newer=None,
        timeStamp="SubmitTime",
        orderAttribute=None,
        limit=None,
        connection=False,
    ):
        """Get stage requests for the supplied selection with support for web standard structure"""
        connection = self.__getConnection(connection)
        req = f"SELECT {intListToString(self.TASKPARAMS)} FROM Tasks"
        if condDict or older or newer:
            if "ReplicaID" in condDict:
                replicaIDs = condDict.pop("ReplicaID")
                if not isinstance(replicaIDs, (list, tuple)):
                    replicaIDs = [replicaIDs]
                res = self._getReplicaIDTasks(replicaIDs, connection=connection)
                if not res["OK"]:
                    return res
                condDict["TaskID"] = res["Value"]
            req = f"{req} {self.buildCondition(condDict, older, newer, timeStamp, orderAttribute, limit)}"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res
        # Cast to list to be able to serialize
        tasks = [list(row) for row in res["Value"]]
        resultDict = {}
        for row in tasks:
            resultDict[row[0]] = dict(zip(self.TASKPARAMS[1:], row[1:]))
        result = S_OK(resultDict)
        result["Records"] = tasks
        result["ParameterNames"] = self.TASKPARAMS
        return result

    def getCacheReplicas(
        self,
        condDict={},
        older=None,
        newer=None,
        timeStamp="LastUpdate",
        orderAttribute=None,
        limit=None,
        connection=False,
    ):
        """Get cache replicas for the supplied selection with support for the web standard structure"""
        connection = self.__getConnection(connection)
        req = f"SELECT {intListToString(self.REPLICAPARAMS)} FROM CacheReplicas"
        if condDict or older or newer:
            if "TaskID" in condDict:
                taskIDs = condDict.pop("TaskID")
                if not isinstance(taskIDs, (list, tuple)):
                    taskIDs = [taskIDs]
                res = self._getTaskReplicaIDs(taskIDs, connection=connection)
                if not res["OK"]:
                    return res
                if res["Value"]:
                    condDict["ReplicaID"] = res["Value"]
                else:
                    condDict["ReplicaID"] = [-1]
            # BUG: limit is ignored unless there is a nonempty condition dictionary OR
            # older OR newer is nonemtpy
            req = f"{req} {self.buildCondition(condDict, older, newer, timeStamp, orderAttribute, limit)}"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res
        # Cast to list to be able to serialize
        cacheReplicas = [list(row) for row in res["Value"]]
        resultDict = {}
        for row in cacheReplicas:
            resultDict[row[0]] = dict(zip(self.REPLICAPARAMS[1:], row[1:]))
        result = S_OK(resultDict)
        result["Records"] = cacheReplicas
        result["ParameterNames"] = self.REPLICAPARAMS
        return result

    def getStageRequests(
        self,
        condDict={},
        older=None,
        newer=None,
        timeStamp="StageRequestSubmitTime",
        orderAttribute=None,
        limit=None,
        connection=False,
    ):
        """Get stage requests for the supplied selection with support for web standard structure"""
        connection = self.__getConnection(connection)
        req = f"SELECT {intListToString(self.STAGEPARAMS)} FROM StageRequests"
        if condDict or older or newer:
            if "TaskID" in condDict:
                taskIDs = condDict.pop("TaskID")
                if not isinstance(taskIDs, (list, tuple)):
                    taskIDs = [taskIDs]
                res = self._getTaskReplicaIDs(taskIDs, connection=connection)
                if not res["OK"]:
                    return res
                if res["Value"]:
                    condDict["ReplicaID"] = res["Value"]
                else:
                    condDict["ReplicaID"] = [-1]
            req = f"{req} {self.buildCondition(condDict, older, newer, timeStamp, orderAttribute, limit)}"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res
        # Cast to list to be able to serialize
        stageRequests = [list(row) for row in res["Value"]]
        resultDict = {}
        for row in stageRequests:
            resultDict[row[0]] = dict(zip(self.STAGEPARAMS[1:], row[1:]))
        result = S_OK(resultDict)
        result["Records"] = stageRequests
        result["ParameterNames"] = self.STAGEPARAMS
        return result

    def _getTaskReplicaIDs(self, taskIDs, connection=False):
        if not taskIDs:
            return S_OK([])
        req = f"SELECT DISTINCT(ReplicaID) FROM TaskReplicas WHERE TaskID IN ({intListToString(taskIDs)});"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res
        replicaIDs = [row[0] for row in res["Value"]]
        return S_OK(replicaIDs)

    def _getReplicaIDTasks(self, replicaIDs, connection=False):
        if not replicaIDs:
            return S_OK([])
        req = f"SELECT DISTINCT(TaskID) FROM TaskReplicas WHERE ReplicaID IN ({intListToString(replicaIDs)});"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res
        taskIDs = [row[0] for row in res["Value"]]

        return S_OK(taskIDs)

    #
    #                               End of monitoring of stage tasks
    #
    ################################################################

    ####################################################################
    #
    # Submission of stage requests
    #

    def setRequest(self, lfnDict, source, callbackMethod, sourceTaskID, connection=False):
        """This method populates the StorageManagementDB Tasks table with the requested files."""
        connection = self.__getConnection(connection)
        if not lfnDict:
            return S_ERROR("No files supplied in request")
        # The first step is to create the task in the Tasks table
        res = self._createTask(source, callbackMethod, sourceTaskID, connection=connection)
        if not res["OK"]:
            return res
        taskID = res["Value"]
        # Get the Replicas which already exist in the CacheReplicas table
        allReplicaIDs = []
        taskStates = []
        for se, lfns in lfnDict.items():
            if isinstance(lfns, str):
                lfns = [lfns]
            res = self._getExistingReplicas(se, lfns, connection=connection)
            if not res["OK"]:
                return res
            existingReplicas = res["Value"]
            # Insert the CacheReplicas that do not already exist
            for lfn in lfns:
                if lfn in existingReplicas:
                    gLogger.verbose(
                        "StorageManagementDB.setRequest: Replica already exists in CacheReplicas table %s @ %s"
                        % (lfn, se)
                    )
                    existingFileState = existingReplicas[lfn][1]
                    taskState = self.__getTaskStateFromReplicaState(existingFileState)
                else:
                    res = self._insertReplicaInformation(lfn, se, "Stage", connection=connection)
                    if not res["OK"]:
                        self._cleanTask(taskID, connection=connection)
                        return res

                    existingReplicas[lfn] = (res["Value"], "New")
                    newFileState = existingReplicas[lfn][1]
                    taskState = self.__getTaskStateFromReplicaState(newFileState)
                if taskState not in taskStates:
                    taskStates.append(taskState)

            allReplicaIDs.extend(existingReplicas.values())
        # Insert all the replicas into the TaskReplicas table
        res = self._insertTaskReplicaInformation(taskID, allReplicaIDs, connection=connection)
        if not res["OK"]:
            self._cleanTask(taskID, connection=connection)
            return res
        # Check whether the the task status is Done based on the existing file states
        # If all the files for a particular Task are 'Staged', update the Task
        if taskStates == ["Staged"]:
            # so if the tasks are for LFNs from the lfns dictionary, which are already staged,
            # they immediately change state New->Done. Fixed it to translate such tasks to 'Staged' state
            self.__updateTaskStatus([taskID], "Staged", True, connection=connection)
        if "Failed" in taskStates:
            self.__updateTaskStatus([taskID], "Failed", True, connection=connection)
        return S_OK(taskID)

    def _cleanTask(self, taskID, connection=False):
        """Remove a task and any related information"""
        connection = self.__getConnection(connection)
        self.removeTasks([taskID], connection=connection)
        self.removeUnlinkedReplicas(connection=connection)

    def _createTask(self, source, callbackMethod, sourceTaskID, connection=False):
        """Enter the task details into the Tasks table"""
        connection = self.__getConnection(connection)
        req = (
            "INSERT INTO Tasks (Source,SubmitTime,CallBackMethod,SourceTaskID) VALUES ('%s',UTC_TIMESTAMP(),'%s','%s');"
            % (source, callbackMethod, sourceTaskID)
        )
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB._createTask: Failed to create task.", res["Message"])
            return res
        # gLogger.info( "%s_DB:%s" % ('_createTask',req))
        taskID = res["lastRowId"]
        reqSelect = f"SELECT * FROM Tasks WHERE TaskID = {taskID};"
        resSelect = self._query(reqSelect, conn=connection)
        if not resSelect["OK"]:
            gLogger.info(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "_createTask", reqSelect, resSelect["Message"])
            )
        else:
            gLogger.verbose(f"{self._caller()}._createTask_DB: inserted Tasks = {resSelect['Value'][0]}")

        # gLogger.info("StorageManagementDB._createTask: Created task with ('%s','%s','%s') and obtained TaskID %s" % (source,callbackMethod,sourceTaskID,taskID))
        return S_OK(taskID)

    def _getExistingReplicas(self, storageElement, lfns, connection=False):
        """Obtains the ReplicasIDs for the replicas already entered in the CacheReplicas table"""
        connection = self.__getConnection(connection)
        req = "SELECT ReplicaID,LFN,Status FROM CacheReplicas WHERE SE = '{}' AND LFN IN ({});".format(
            storageElement,
            stringListToString(lfns),
        )
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB._getExistingReplicas: Failed to get existing replicas.", res["Message"])
            return res
        existingReplicas = {}
        for replicaID, lfn, status in res["Value"]:
            existingReplicas[lfn] = (replicaID, status)
        return S_OK(existingReplicas)

    def _insertReplicaInformation(self, lfn, storageElement, rType, connection=False):
        """Enter the replica into the CacheReplicas table"""
        connection = self.__getConnection(connection)
        req = (
            "INSERT INTO CacheReplicas (Type,SE,LFN,PFN,Size,FileChecksum,GUID,SubmitTime,LastUpdate) VALUES ('%s','%s','%s','',0,'','',UTC_TIMESTAMP(),UTC_TIMESTAMP());"
            % (rType, storageElement, lfn)
        )
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error("_insertReplicaInformation: Failed to insert to CacheReplicas table.", res["Message"])
            return res
        # gLogger.info( "%s_DB:%s" % ('_insertReplicaInformation',req))

        replicaID = res["lastRowId"]
        reqSelect = f"SELECT * FROM CacheReplicas WHERE ReplicaID = {replicaID};"
        resSelect = self._query(reqSelect, conn=connection)
        if not resSelect["OK"]:
            gLogger.warn(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "_insertReplicaInformation", reqSelect, resSelect["Message"])
            )
        else:
            gLogger.verbose(
                "%s.%s_DB: inserted CacheReplicas = %s"
                % (self._caller(), "_insertReplicaInformation", resSelect["Value"][0])
            )
        # gLogger.verbose("_insertReplicaInformation: Inserted Replica ('%s','%s') and obtained ReplicaID %s" % (lfn,storageElement,replicaID))
        return S_OK(replicaID)

    def _insertTaskReplicaInformation(self, taskID, replicaIDs, connection=False):
        """Enter the replicas into TaskReplicas table"""
        connection = self.__getConnection(connection)
        req = "INSERT INTO TaskReplicas (TaskID,ReplicaID) VALUES "
        for replicaID, _status in replicaIDs:
            replicaString = f"({taskID},{replicaID}),"
            req = f"{req} {replicaString}"
        req = req.rstrip(",")
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error(
                "StorageManagementDB._insertTaskReplicaInformation: Failed to insert to TaskReplicas table.",
                res["Message"],
            )
            return res
        # gLogger.info( "%s_DB:%s" % ('_insertTaskReplicaInformation',req))
        gLogger.verbose(
            "StorageManagementDB._insertTaskReplicaInformation: Successfully added %s CacheReplicas to Task %s."
            % (res["Value"], taskID)
        )
        return S_OK()

    #
    #                               End of insertion methods
    #
    ################################################################

    ####################################################################

    def getStagedReplicas(self, connection=False):
        connection = self.__getConnection(connection)
        req = "SELECT TR.TaskID, R.Status, COUNT(*) from TaskReplicas as TR, CacheReplicas as R where TR.ReplicaID=R.ReplicaID GROUP BY TR.TaskID,R.Status;"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.getStagedReplicas: Failed to get eligible TaskReplicas", res["Message"])
            return res
        goodTasks = []
        for taskID, status, _count in res["Value"]:
            if taskID in goodTasks:
                continue
            elif status in ("Staged", "StageSubmitted"):
                goodTasks.append(taskID)
        return self.getCacheReplicas({"Status": "Staged", "TaskID": goodTasks}, connection=connection)

    def getWaitingReplicas(self, connection=False):
        connection = self.__getConnection(connection)
        req = "SELECT TR.TaskID, R.Status, COUNT(*) from TaskReplicas as TR, CacheReplicas as R where TR.ReplicaID=R.ReplicaID GROUP BY TR.TaskID,R.Status;"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.getWaitingReplicas: Failed to get eligible TaskReplicas", res["Message"])
            return res
        badTasks = []
        goodTasks = []
        for taskID, status, _count in res["Value"]:
            if taskID in badTasks:
                continue
            elif status in ("New", "Failed"):
                badTasks.append(taskID)
            elif status == "Waiting":
                goodTasks.append(taskID)
        return self.getCacheReplicas({"Status": "Waiting", "TaskID": goodTasks}, connection=connection)

    ####################################################################

    def getOfflineReplicas(self, connection=False):
        connection = self.__getConnection(connection)
        req = "SELECT TR.TaskID, R.Status, COUNT(*) from TaskReplicas as TR, CacheReplicas as R where TR.ReplicaID=R.ReplicaID GROUP BY TR.TaskID,R.Status;"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.getOfflineReplicas: Failed to get eligible TaskReplicas", res["Message"])
            return res
        badTasks = []
        goodTasks = []
        for taskID, status, _count in res["Value"]:
            if taskID in badTasks:
                continue
            elif status in ("New", "Failed"):
                badTasks.append(taskID)
            elif status == "Offline":
                goodTasks.append(taskID)
        return self.getCacheReplicas({"Status": "Offline", "TaskID": goodTasks}, connection=connection)

    ####################################################################

    def getTasksWithStatus(self, status):
        """This method retrieves the TaskID from the Tasks table with the supplied Status."""
        req = f"SELECT TaskID,Source,CallBackMethod,SourceTaskID from Tasks WHERE Status = '{status}';"
        res = self._query(req)
        if not res["OK"]:
            return res
        taskIDs = {}
        for taskID, source, callback, sourceTask in res["Value"]:
            taskIDs[taskID] = (source, callback, sourceTask)
        return S_OK(taskIDs)

    ####################################################################
    #
    # The state transition of the CacheReplicas from *->Failed
    #

    def updateReplicaFailure(self, terminalReplicaIDs):
        """This method sets the status to Failure with the failure reason for the supplied Replicas."""
        res = self.updateReplicaStatus(list(terminalReplicaIDs), "Failed")
        if not res["OK"]:
            return res
        updated = res["Value"]
        if not updated:
            return S_OK(updated)
        for replicaID in updated:
            reqSelect = "Select * FROM CacheReplicas WHERE ReplicaID = %d" % (replicaID)
            resSelect = self._query(reqSelect)
            if not resSelect["OK"]:
                gLogger.warn(
                    "%s.%s_DB: problem retrieving record: %s. %s"
                    % (self._caller(), "updateReplicaFailure", reqSelect, resSelect["Message"])
                )

            req = "UPDATE CacheReplicas SET Reason = '%s' WHERE ReplicaID = %d" % (
                terminalReplicaIDs[replicaID],
                replicaID,
            )
            res = self._update(req)
            if not res["OK"]:
                gLogger.error(
                    "StorageManagementDB.updateReplicaFailure: Failed to update replica fail reason.", res["Message"]
                )
                return res

            replicaIDs = []
            for record in resSelect["Value"]:
                replicaIDs.append(record[0])
                gLogger.verbose(f"{self._caller()}.updateReplicaFailure_DB: to_update CacheReplicas =  {record}")

            reqSelect1 = f"SELECT * FROM CacheReplicas WHERE ReplicaID IN ({intListToString(replicaIDs)});"
            resSelect1 = self._query(reqSelect1)
            if not resSelect1["OK"]:
                gLogger.warn(
                    "%s.%s_DB: problem retrieving records: %s. %s"
                    % (self._caller(), "updateReplicaFailure", reqSelect1, resSelect1["Message"])
                )
            else:
                for record in resSelect1["Value"]:
                    gLogger.verbose(f"{self._caller()}.updateReplicaFailure_DB: updated CacheReplicas = {record}")

        return S_OK(updated)

    ####################################################################
    #
    # The state transition of the CacheReplicas from New->Waiting
    #

    def updateReplicaInformation(self, replicaTuples):
        """This method set the replica size information and pfn for the requested storage element."""
        for replicaID, pfn, size in replicaTuples:
            # reqSelect = "SELECT * FROM CacheReplicas WHERE ReplicaID = %s and Status != 'Cancelled';" % ( replicaID )
            reqSelect = "SELECT ReplicaID FROM CacheReplicas WHERE ReplicaID = %s and Status != 'Cancelled';" % (
                replicaID
            )
            resSelect = self._query(reqSelect)
            if not resSelect["OK"]:
                gLogger.warn(
                    "%s.%s_DB: problem retrieving record: %s. %s"
                    % (self._caller(), "updateReplicaInformation", reqSelect, resSelect["Message"])
                )

            req = (
                "UPDATE CacheReplicas SET PFN = '%s', Size = %s, Status = 'Waiting' WHERE ReplicaID = %s and Status != 'Cancelled';"
                % (pfn, size, replicaID)
            )
            res = self._update(req)
            if not res["OK"]:
                gLogger.error(
                    "StagerDB.updateReplicaInformation: Failed to insert replica information.", res["Message"]
                )

            replicaIDs = []
            for record in resSelect["Value"]:
                replicaIDs.append(record[0])
                gLogger.verbose(f"{self._caller()}.updateReplicaInformation_DB: to_update CacheReplicas =  {record}")

            reqSelect1 = f"SELECT * FROM CacheReplicas WHERE ReplicaID IN ({intListToString(replicaIDs)});"
            resSelect1 = self._query(reqSelect1)
            if not resSelect1["OK"]:
                gLogger.warn(
                    "%s.%s_DB: problem retrieving record: %s. %s"
                    % (self._caller(), "updateReplicaInformation", reqSelect1, resSelect1["Message"])
                )
            else:
                for record in resSelect1["Value"]:
                    gLogger.verbose(
                        "{}.{}_DB: updated CacheReplicas = {}".format(
                            self._caller(), "updateReplicaInformation", record
                        )
                    )

            gLogger.debug(
                "StagerDB.updateReplicaInformation: Successfully updated CacheReplicas record With Status=Waiting, for ReplicaID= %s"
                % (replicaID)
            )
        return S_OK()

    ####################################################################
    #
    # The state transition of the CacheReplicas from Waiting->StageSubmitted
    #

    def getSubmittedStagePins(self):
        # change the query to take into account pin expiry time
        req = "SELECT SE,COUNT(*),SUM(Size) from CacheReplicas WHERE Status NOT IN ('New','Waiting','Offline','Failed') GROUP BY SE;"
        # req = "SELECT SE,Count(*),SUM(Size) from CacheReplicas,StageRequests WHERE Status NOT IN ('New','Waiting','Failed') and CacheReplicas.ReplicaID=StageRequests.ReplicaID and PinExpiryTime>UTC_TIMESTAMP() GROUP BY SE;"
        res = self._query(req)
        if not res["OK"]:
            gLogger.error(
                "StorageManagementDB.getSubmittedStagePins: Failed to obtain submitted requests.", res["Message"]
            )
            return res
        storageRequests = {}
        for storageElement, replicas, totalSize in res["Value"]:
            storageRequests[storageElement] = {"Replicas": int(replicas), "TotalSize": int(totalSize)}
        return S_OK(storageRequests)

    def insertStageRequest(self, requestDict, pinLifeTime):
        req = "INSERT INTO StageRequests (ReplicaID,RequestID,StageRequestSubmitTime,PinLength) VALUES "
        for requestID, replicaIDs in requestDict.items():
            for replicaID in replicaIDs:
                replicaString = "(%s,'%s',UTC_TIMESTAMP(),%d)," % (replicaID, requestID, pinLifeTime)
                req = f"{req} {replicaString}"
        req = req.rstrip(",")
        res = self._update(req)
        if not res["OK"]:
            gLogger.error(
                "StorageManagementDB.insertStageRequest: Failed to insert to StageRequests table.", res["Message"]
            )
            return res

        for requestID, replicaIDs in requestDict.items():
            for replicaID in replicaIDs:
                # fix, no individual queries
                reqSelect = "SELECT * FROM StageRequests WHERE ReplicaID = {} AND RequestID = '{}';".format(
                    replicaID,
                    requestID,
                )
                resSelect = self._query(reqSelect)
                if not resSelect["OK"]:
                    gLogger.warn(
                        "%s.%s_DB: problem retrieving record: %s. %s"
                        % (self._caller(), "insertStageRequest", reqSelect, resSelect["Message"])
                    )
                else:
                    gLogger.verbose(
                        "%s.%s_DB: inserted StageRequests = %s"
                        % (self._caller(), "insertStageRequest", resSelect["Value"][0])
                    )

        # gLogger.info( "%s_DB: howmany = %s" % ('insertStageRequest',res))

        # gLogger.info( "%s_DB:%s" % ('insertStageRequest',req))
        gLogger.debug(
            "StorageManagementDB.insertStageRequest: Successfully added %s StageRequests with RequestID %s."
            % (res["Value"], requestID)
        )
        return S_OK()

    ####################################################################
    #
    # The state transition of the CacheReplicas from StageSubmitted->Staged
    #

    def setStageComplete(self, replicaIDs):
        # Daniela: FIX wrong PinExpiryTime (84000->86400 seconds = 1 day)

        reqSelect = f"SELECT * FROM StageRequests WHERE ReplicaID IN ({intListToString(replicaIDs)});"
        resSelect = self._query(reqSelect)
        if not resSelect["OK"]:
            gLogger.warn(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "setStageComplete", reqSelect, resSelect["Message"])
            )
            return resSelect

        req = (
            "UPDATE StageRequests SET StageStatus='Staged',StageRequestCompletedTime = UTC_TIMESTAMP(),PinExpiryTime = DATE_ADD(UTC_TIMESTAMP(),INTERVAL ( PinLength / %s ) SECOND) WHERE ReplicaID IN (%s);"
            % (THROTTLING_STEPS, intListToString(replicaIDs))
        )
        res = self._update(req)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.setStageComplete: Failed to set StageRequest completed.", res["Message"])
            return res

        for record in resSelect["Value"]:
            gLogger.verbose(f"{self._caller()}.setStageComplete_DB: to_update StageRequests =  {record}")

        reqSelect1 = f"SELECT * FROM StageRequests WHERE ReplicaID IN ({intListToString(replicaIDs)});"
        resSelect1 = self._query(reqSelect1)
        if not resSelect1["OK"]:
            gLogger.warn(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "setStageComplete", reqSelect1, resSelect1["Message"])
            )

        for record in resSelect1["Value"]:
            gLogger.verbose(f"{self._caller()}.setStageComplete_DB: updated StageRequests = {record}")

        gLogger.debug(
            "StorageManagementDB.setStageComplete: Successfully updated %s StageRequests table with StageStatus=Staged for ReplicaIDs: %s."
            % (res["Value"], replicaIDs)
        )
        return res

    def wakeupOldRequests(self, replicaIDs, retryInterval, connection=False):
        """
        get only StageRequests with StageRequestSubmitTime older than 1 day AND are still not staged
        delete these requests
        reset Replicas with corresponding ReplicaIDs to Status='New'
        """
        try:
            retryInterval = max(retryInterval, 2)
            retryInterval = min(retryInterval, 24)
            retryInterval = int(retryInterval)
        except Exception:
            errorString = "Wrong argument type"
            gLogger.exception(errorString)
            return S_ERROR(errorString)
        if replicaIDs:
            req = (
                "SELECT ReplicaID FROM StageRequests WHERE ReplicaID IN (%s) AND StageStatus='StageSubmitted' AND DATE_ADD( StageRequestSubmitTime, INTERVAL %s HOUR ) < UTC_TIMESTAMP();"
                % (intListToString(replicaIDs), retryInterval)
            )
            res = self._query(req)
            if not res["OK"]:
                gLogger.error(
                    "StorageManagementDB.wakeupOldRequests: Failed to select old StageRequests.", res["Message"]
                )
                return res

            old_replicaIDs = [row[0] for row in res["Value"]]

            if old_replicaIDs:
                req = (
                    "UPDATE CacheReplicas SET Status='New',LastUpdate = UTC_TIMESTAMP(), Reason = 'wakeupOldRequests' WHERE ReplicaID in (%s);"
                    % intListToString(old_replicaIDs)
                )
                res = self._update(req, conn=connection)
                if not res["OK"]:
                    gLogger.error(
                        "StorageManagementDB.wakeupOldRequests: Failed to roll CacheReplicas back to Status=New.",
                        res["Message"],
                    )
                    return res

                req = f"DELETE FROM StageRequests WHERE ReplicaID in ({intListToString(old_replicaIDs)});"
                res = self._update(req, conn=connection)
                if not res["OK"]:
                    gLogger.error("StorageManagementDB.wakeupOldRequests. Problem removing entries from StageRequests.")
                    return res

        return S_OK()

    ####################################################################
    #
    # This code handles the finalization of stage tasks
    #
    # Daniela: useless method
    '''
  def updateStageCompletingTasks(self):
    """ This will select all the Tasks in StageCompleting status and check whether all the associated files are Staged. """
    req = "SELECT TR.TaskID,COUNT(if(R.Status NOT IN ('Staged'),1,NULL)) FROM Tasks AS T, TaskReplicas AS TR, CacheReplicas AS R WHERE T.Status='StageCompleting' AND T.TaskID=TR.TaskID AND TR.ReplicaID=R.ReplicaID GROUP BY TR.TaskID;"
    res = self._query(req)
    if not res['OK']:
      return res
    taskIDs = []
    for taskID,count in res['Value']:
      if int(count) == 0:
        taskIDs.append(taskID)
    if not taskIDs:
      return S_OK(taskIDs)
    req = "UPDATE Tasks SET Status = 'Staged' WHERE TaskID IN (%s);" % intListToString(taskIDs)
    res = self._update(req)
    if not res['OK']:
      return res
    return S_OK(taskIDs)
  '''

    def setTasksDone(self, taskIDs):
        """This will update the status for a list of taskIDs to Done."""
        reqSelect = f"SELECT * FROM Tasks WHERE TaskID IN ({intListToString(taskIDs)});"
        resSelect = self._query(reqSelect)
        if not resSelect["OK"]:
            gLogger.error(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "setTasksDone", reqSelect, resSelect["Message"])
            )

        req = (
            "UPDATE Tasks SET Status = 'Done', CompleteTime = UTC_TIMESTAMP() WHERE TaskID IN (%s);"
            % intListToString(taskIDs)
        )
        res = self._update(req)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.setTasksDone: Failed to set Tasks status to Done.", res["Message"])
            return res

        for record in resSelect["Value"]:
            gLogger.verbose(f"{self._caller()}.setTasksDone_DB: to_update Tasks =  {record}")
            # fix, no individual queries
        reqSelect1 = f"SELECT * FROM Tasks WHERE TaskID IN ({intListToString(taskIDs)});"
        resSelect1 = self._query(reqSelect1)
        if not resSelect1["OK"]:
            gLogger.warn(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "setTasksDone", reqSelect1, resSelect1["Message"])
            )
        else:
            for record in resSelect1["Value"]:
                gLogger.verbose(f"{self._caller()}.setTasksDone_DB: updated Tasks = {record}")

        gLogger.debug(
            "StorageManagementDB.setTasksDone: Successfully updated %s Tasks with StageStatus=Done for taskIDs: %s."
            % (res["Value"], taskIDs)
        )
        return res

    def killTasksBySourceTaskID(self, sourceTaskIDs, connection=False):
        """Given SourceTaskIDs (jobs), this will cancel further staging of files for the corresponding tasks.
        The "cancel" is actually removing all stager DB records for these jobs.
        Care must be taken to NOT cancel staging of files that are requested also by other tasks."""
        connection = self.__getConnection(connection)

        # get the TaskIDs
        req = f"SELECT TaskID from Tasks WHERE SourceTaskID IN ({intListToString(sourceTaskIDs)});"
        res = self._query(req)
        if not res["OK"]:
            gLogger.error(
                "%s.%s_DB: problem retrieving records: %s. %s"
                % (self._caller(), "killTasksBySourceTaskID", req, res["Message"])
            )
        taskIDs = [row[0] for row in res["Value"]]

        # ! Make sure to only cancel file staging for files with no relations with other tasks (jobs) but the killed ones
        if taskIDs:
            req = (
                "SELECT DISTINCT(CR.ReplicaID) FROM TaskReplicas AS TR, CacheReplicas AS CR WHERE TR.TaskID IN (%s) AND CR.Links=1 and TR.ReplicaID=CR.ReplicaID;"
                % intListToString(taskIDs)
            )
            res = self._query(req)
            if not res["OK"]:
                gLogger.error(
                    "%s.%s_DB: problem retrieving records: %s. %s"
                    % (self._caller(), "killTasksBySourceTaskID", req, res["Message"])
                )

            replicaIDs = [row[0] for row in res["Value"]]

            if replicaIDs:
                req = f"DELETE FROM StageRequests WHERE ReplicaID IN ({intListToString(replicaIDs)});"
                res = self._update(req, conn=connection)
                if not res["OK"]:
                    gLogger.error(
                        "%s.%s_DB: problem removing records: %s. %s"
                        % (self._caller(), "killTasksBySourceTaskID", req, res["Message"])
                    )

                req = f"DELETE FROM CacheReplicas WHERE ReplicaID in ({intListToString(replicaIDs)}) AND Links=1;"
                res = self._update(req, conn=connection)
                if not res["OK"]:
                    gLogger.error(
                        "%s.%s_DB: problem removing records: %s. %s"
                        % (self._caller(), "killTasksBySourceTaskID", req, res["Message"])
                    )

            # Finally, remove the Task and TaskReplicas entries.
            res = self.removeTasks(taskIDs, connection=connection)
        return res

    def removeStageRequests(self, replicaIDs, connection=False):
        connection = self.__getConnection(connection)
        req = f"DELETE FROM StageRequests WHERE ReplicaID in ({intListToString(replicaIDs)});"
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.removeStageRequests. Problem removing entries from StageRequests.")
            return res
        return res

    def removeTasks(self, taskIDs, connection=False):
        """This will delete the entries from the TaskReplicas for the provided taskIDs."""
        connection = self.__getConnection(connection)
        req = f"DELETE FROM TaskReplicas WHERE TaskID IN ({intListToString(taskIDs)});"
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.removeTasks. Problem removing entries from TaskReplicas.")
            return res
        # gLogger.info( "%s_DB:%s" % ('removeTasks',req))
        reqSelect = f"SELECT * FROM Tasks WHERE TaskID IN ({intListToString(taskIDs)});"
        resSelect = self._query(reqSelect)
        if not resSelect["OK"]:
            gLogger.error(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "removeTasks", reqSelect, resSelect["Message"])
            )
        else:
            for record in resSelect["Value"]:
                gLogger.verbose(f"{self._caller()}.removeTasks_DB: to_delete Tasks =  {record}")

        req = f"DELETE FROM Tasks WHERE TaskID in ({intListToString(taskIDs)});"
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.removeTasks. Problem removing entries from Tasks.")
        gLogger.verbose(f"{self._caller()}.removeTasks_DB: deleted Tasks")
        # gLogger.info( "%s_DB:%s" % ('removeTasks',req))
        return res

    def setOldTasksAsFailed(self, daysOld, connection=False):
        """
        Set Tasks older than "daysOld" number of days to Failed
        These tasks have already been retried every day for staging
        """
        req = "UPDATE Tasks SET Status='Failed' WHERE DATE_ADD(SubmitTime, INTERVAL %s DAY ) < UTC_TIMESTAMP();" % (
            daysOld
        )
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.setOldTasksAsFailed. Problem setting old Tasks to Failed.")
            return res
        return res

    def getCacheReplicasSummary(self, connection=False):
        """
        Reports breakdown of file number/size in different staging states across storage elements
        """
        connection = self.__getConnection(connection)
        req = "SELECT DISTINCT(Status),SE,COUNT(*),sum(size)/(1024*1024*1024) FROM CacheReplicas GROUP BY Status,SE;"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error("StorageManagementDB.getCacheReplicasSummary failed.")
            return res

        resSummary = {}
        i = 1
        for status, se, numFiles, sumFiles in res["Value"]:
            resSummary[i] = {"Status": status, "SE": se, "NumFiles": int(numFiles), "SumFiles": float(sumFiles)}
            i += 1
        return S_OK(resSummary)

    def removeUnlinkedReplicas(self, connection=False):
        """This will remove Replicas from the CacheReplicas that are not associated to any Task.
        If the Replica has been Staged,
        wait until StageRequest.PinExpiryTime and remove the StageRequest and CacheReplicas entries
        """
        connection = self.__getConnection(connection)
        # First, check if there is a StageRequest and PinExpiryTime has arrived
        req = "select SR.ReplicaID from CacheReplicas CR,StageRequests SR WHERE CR.Links = 0 and CR.ReplicaID=SR.ReplicaID group by SR.ReplicaID HAVING max(SR.PinExpiryTime) < UTC_TIMESTAMP();"
        # req = "SELECT ReplicaID from CacheReplicas WHERE Links = 0;"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error(
                "StorageManagementDB.removeUnlinkedReplicas. Problem selecting entries from CacheReplicas where Links = 0."
            )
            return res

        replicaIDs = [row[0] for row in res["Value"]]
        # Look for Failed CacheReplicas which are not associated to any Task. These have no PinExpiryTime in StageRequests
        # as they were not staged successfully (for various reasons), even though
        # a staging request had been submitted
        req = "SELECT ReplicaID FROM CacheReplicas WHERE Links = 0 AND Status = 'Failed';"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error(
                "StorageManagementDB.removeUnlinkedReplicas. Problem selecting entries from CacheReplicas where Links = 0 AND Status=Failed."
            )
        else:
            replicaIDs.extend([row[0] for row in res["Value"]])

        if replicaIDs:
            # Removed the entries from the StageRequests table that are expired
            reqSelect = f"SELECT * FROM StageRequests WHERE ReplicaID IN ({intListToString(replicaIDs)});"
            resSelect = self._query(reqSelect)
            if not resSelect["OK"]:
                gLogger.warn(
                    "%s.%s_DB: problem retrieving record: %s. %s"
                    % (self._caller(), "removeUnlinkedReplicas", reqSelect, resSelect["Message"])
                )
            else:
                for record in resSelect["Value"]:
                    gLogger.verbose(
                        "{}.{}_DB: to_delete StageRequests = {}".format(
                            self._caller(), "removeUnlinkedReplicas", record
                        )
                    )

            req = f"DELETE FROM StageRequests WHERE ReplicaID IN ({intListToString(replicaIDs)});"
            res = self._update(req, conn=connection)
            if not res["OK"]:
                gLogger.error("StorageManagementDB.removeUnlinkedReplicas. Problem deleting from StageRequests.")
                return res
            gLogger.verbose(f"{self._caller()}.removeUnlinkedReplicas_DB: deleted StageRequests")

            gLogger.debug(
                "StorageManagementDB.removeUnlinkedReplicas: Successfully removed %s StageRequests entries for ReplicaIDs: %s."
                % (res["Value"], replicaIDs)
            )

        # Second look for CacheReplicas for which there is no entry in StageRequests
        req = "SELECT ReplicaID FROM CacheReplicas WHERE Links = 0 AND ReplicaID NOT IN ( SELECT DISTINCT( ReplicaID ) FROM StageRequests )"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            gLogger.error(
                "StorageManagementDB.removeUnlinkedReplicas. Problem selecting entries from CacheReplicas where Links = 0."
            )
        else:
            replicaIDs.extend([row[0] for row in res["Value"]])

        if not replicaIDs:
            return S_OK()

        # Now delete all CacheReplicas
        reqSelect = f"SELECT * FROM CacheReplicas WHERE ReplicaID IN ({intListToString(replicaIDs)});"
        resSelect = self._query(reqSelect)
        if not resSelect["OK"]:
            gLogger.warn(
                "%s.%s_DB: problem retrieving record: %s. %s"
                % (self._caller(), "removeUnlinkedReplicas", reqSelect, resSelect["Message"])
            )
        else:
            for record in resSelect["Value"]:
                gLogger.verbose(f"{self._caller()}.removeUnlinkedReplicas_DB: to_delete CacheReplicas =  {record}")

        req = f"DELETE FROM CacheReplicas WHERE ReplicaID IN ({intListToString(replicaIDs)}) AND Links= 0;"
        res = self._update(req, conn=connection)
        if res["OK"]:
            gLogger.verbose(f"{self._caller()}.removeUnlinkedReplicas_DB: deleted CacheReplicas")
            gLogger.debug(
                "StorageManagementDB.removeUnlinkedReplicas: Successfully removed %s CacheReplicas entries for ReplicaIDs: %s."
                % (res["Value"], replicaIDs)
            )
        else:
            gLogger.error("StorageManagementDB.removeUnlinkedReplicas. Problem removing entries from CacheReplicas.")
        return res
