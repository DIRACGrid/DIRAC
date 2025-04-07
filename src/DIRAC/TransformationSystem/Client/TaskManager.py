""" TaskManager contains TaskBase, which is inherited by WorkflowTasks and RequestTasks modules,
for managing jobs and requests tasks
"""
# pylint: disable=protected-access


import time

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities import TransformationAgentsUtilities
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


class TaskBase(TransformationAgentsUtilities):
    """The other classes inside here inherits from this one."""

    def __init__(self, transClient=None, logger=None):
        """c'tor"""

        if not transClient:
            self.transClient = TransformationClient()
        else:
            self.transClient = transClient

        if not logger:
            self.log = gLogger.getSubLogger("TaskBase")
        else:
            self.log = logger

        self.pluginLocation = "DIRAC.TransformationSystem.Client.TaskManagerPlugin"

        super().__init__()

    def prepareTransformationTasks(
        self,
        _transBody,
        _taskDict,  # pylint: disable=no-self-use, unused-argument
        owner="",
        ownerGroup="",
        bulkSubmissionFlag=False,
    ):  # pylint: disable=unused-argument
        """To make sure the method is implemented in the derived class"""
        if owner or ownerGroup or bulkSubmissionFlag:  # Makes pylint happy
            pass
        return S_ERROR("Not implemented")

    def submitTransformationTasks(self, _taskDict):  # pylint: disable=no-self-use
        """To make sure the method is implemented in the derived class"""
        return S_ERROR("Not implemented")

    def submitTasksToExternal(self, _task):  # pylint: disable=no-self-use
        """To make sure the method is implemented in the derived class"""
        return S_ERROR("Not implemented")

    def updateDBAfterTaskSubmission(self, taskDict):
        """Sets tasks status after the submission to "Submitted", in case of success"""
        updated = 0
        startTime = time.time()
        for taskID, task in taskDict.items():
            transID = task["TransformationID"]
            if task["Success"]:
                res = self.transClient.setTaskStatusAndWmsID(transID, int(taskID), "Submitted", str(task["ExternalID"]))
                if not res["OK"]:
                    self._logWarn(
                        "Failed to update task status after submission",
                        f"{task['ExternalID']} {res['Message']}",
                        transID=transID,
                        method="updateDBAfterSubmission",
                    )
                updated += 1
        if updated:
            self._logInfo(
                "Updated %d tasks in %.1f seconds" % (updated, time.time() - startTime),
                transID=transID,
                method="updateDBAfterSubmission",
            )
        return S_OK()

    def updateTransformationReservedTasks(self, _taskDicts):  # pylint: disable=no-self-use
        """To make sure the method is implemented in the derived class"""
        return S_ERROR("Not implemented")

    def getSubmittedTaskStatus(self, _taskDicts):  # pylint: disable=no-self-use
        """To make sure the method is implemented in the derived class"""
        return S_ERROR("Not implemented")

    def getSubmittedFileStatus(self, _fileDicts):  # pylint: disable=no-self-use
        """To make sure the method is implemented in the derived class"""
        return S_ERROR("Not implemented")
