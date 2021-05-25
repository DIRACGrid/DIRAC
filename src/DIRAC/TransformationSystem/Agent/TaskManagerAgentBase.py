""" NOTA BENE: This agent should NOT be run alone. Instead, it serves as a base class for extensions.

    The TaskManagerAgentBase is the base class to submit tasks to external systems,
    monitor and update the tasks and file status in the transformation DB.

    This agent is extended in WorkflowTaskAgent and RequestTaskAgent.
    In case you want to further extend it you are required to follow the note on the
    initialize method and on the _getClients method.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import time
import datetime
import concurrent.futures

from DIRAC import S_OK

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.Dictionaries import breakDictionaryIntoChunks
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername, getUsernameForDN
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.TransformationSystem.Client.TaskManager import WorkflowTasks
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities import TransformationAgentsUtilities
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient

AGENT_NAME = 'Transformation/TaskManagerAgentBase'


class TaskManagerAgentBase(AgentModule, TransformationAgentsUtilities):
  """ To be extended. Please look at WorkflowTaskAgent and RequestTaskAgent.
  """

  def __init__(self, *args, **kwargs):
    """ c'tor

        Always call this in the extension agent
    """
    AgentModule.__init__(self, *args, **kwargs)
    TransformationAgentsUtilities.__init__(self)

    self.transClient = None
    self.jobManagerClient = None
    self.transType = []

    self.tasksPerLoop = 50
    self.maxParametricJobs = 20  # will be updated in execute()

    # credentials
    self.shifterProxy = None
    self.credentials = None
    self.credTuple = (None, None, None)

    self.pluginLocation = ''
    self.bulkSubmissionFlag = False

  #############################################################################

  def initialize(self):
    """ Agent initialization.

        The extensions MUST provide in the initialize method the following data members:
        - TransformationClient objects (self.transClient),
        - set the shifterProxy if different from the default one set here ('ProductionManager')
        - list of transformation types to be looked (self.transType)
    """

    gMonitor.registerActivity("SubmittedTasks", "Automatically submitted tasks", "Transformation Monitoring", "Tasks",
                              gMonitor.OP_ACUM)

    self.pluginLocation = self.am_getOption('PluginLocation', 'DIRAC.TransformationSystem.Client.TaskManagerPlugin')

    # Default clients
    self.transClient = TransformationClient()
    self.jobManagerClient = JobManagerClient()

    # Bulk submission flag
    self.bulkSubmissionFlag = self.am_getOption('BulkSubmission', self.bulkSubmissionFlag)

    # Shifter credentials to use, could replace the use of shifterProxy eventually
    self.shifterProxy = self.am_getOption('shifterProxy', self.shifterProxy)
    self.credentials = self.am_getOption('ShifterCredentials', self.credentials)
    resCred = self.__getCredentials()
    if not resCred['OK']:
      return resCred
    # setting up the threading
    maxNumberOfThreads = self.am_getOption('maxNumberOfThreads', 15)
    self.log.verbose("Multithreaded with %d threads" % maxNumberOfThreads)

    self.threadPoolExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=maxNumberOfThreads)

    return S_OK()

  def finalize(self):
    """ graceful finalization
    """
    method = 'finalize'
    self._logInfo("Wait for threads to get empty before terminating the agent", method=method)
    self.threadPoolExecutor.shutdown()
    self._logInfo("Threads are empty, terminating the agent...", method=method)
    return S_OK()

  def execute(self):
    """ The execution method is transformations that need to be processed
    """

    # 1. determining which credentials will be used for the submission
    owner, ownerGroup, ownerDN = None, None, None
    # getting the credentials for submission
    resProxy = getProxyInfo(proxy=False, disableVOMS=False)
    if resProxy['OK']:  # there is a shifterProxy
      proxyInfo = resProxy['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']
      ownerDN = proxyInfo['identity']
      self.log.info("ShifterProxy: Tasks will be submitted with the credentials %s:%s" % (owner, ownerGroup))
    elif self.credentials:
      owner, ownerGroup, ownerDN = self.credTuple
    else:
      self.log.info("Using per Transformation Credentials!")

    # 2. Determining which operations to do on each transformation
    self.operationsOnTransformationDict = {}  # key: TransID. Value: dict with body, and list of operations

    # 2.1 Determine whether the task status is to be monitored and updated
    if not self.am_getOption('MonitorTasks', ''):
      self.log.verbose(
          "Monitoring of tasks is disabled. To enable it, create the 'MonitorTasks' option")
    else:
      # Get the transformations for which the tasks have to be updated
      status = self.am_getOption(
          'UpdateTasksTransformationStatus',
          self.am_getOption('UpdateTasksStatus', ['Active', 'Completing', 'Stopped']))
      transformations = self._selectTransformations(
          transType=self.transType, status=status, agentType=[])
      if not transformations['OK']:
        self.log.warn("Could not select transformations:", transformations['Message'])
      else:
        self._addOperationForTransformations(
            self.operationsOnTransformationDict,
            'updateTaskStatus',
            transformations,
            owner=owner,
            ownerGroup=ownerGroup,
            ownerDN=ownerDN)

    # 2.2. Determine whether the task files status is to be monitored and updated
    if not self.am_getOption('MonitorFiles', ''):
      self.log.verbose(
          "Monitoring of files is disabled. To enable it, create the 'MonitorFiles' option")
    else:
      # Get the transformations for which the files have to be updated
      status = self.am_getOption(
          'UpdateFilesTransformationStatus',
          self.am_getOption('UpdateFilesStatus', ['Active', 'Completing', 'Stopped']))
      transformations = self._selectTransformations(
          transType=self.transType, status=status, agentType=[])
      if not transformations['OK']:
        self.log.warn("Could not select transformations:", transformations['Message'])
      else:
        self._addOperationForTransformations(
            self.operationsOnTransformationDict,
            'updateFileStatus',
            transformations,
            owner=owner,
            ownerGroup=ownerGroup,
            ownerDN=ownerDN)

    # Determine whether the checking of reserved tasks is to be performed
    if not self.am_getOption('CheckReserved', ''):
      self.log.verbose(
          "Checking of reserved tasks is disabled. To enable it, create the 'CheckReserved' option")
    else:
      # Get the transformations for which the check of reserved tasks have to be performed
      status = self.am_getOption(
          'CheckReservedTransformationStatus',
          self.am_getOption('CheckReservedStatus', ['Active', 'Completing', 'Stopped']))
      transformations = self._selectTransformations(
          transType=self.transType, status=status, agentType=[])
      if not transformations['OK']:
        self.log.warn("Could not select transformations:", transformations['Message'])
      else:
        self._addOperationForTransformations(
            self.operationsOnTransformationDict,
            'checkReservedTasks',
            transformations,
            owner=owner,
            ownerGroup=ownerGroup,
            ownerDN=ownerDN)

    # Determine whether the submission of tasks is to be performed
    if not self.am_getOption('SubmitTasks', 'yes'):
      self.log.verbose(
          "Submission of tasks is disabled. To enable it, create the 'SubmitTasks' option")
    else:
      # Get the transformations for which the submission of tasks have to be performed
      status = self.am_getOption(
          'SubmitTransformationStatus',
          self.am_getOption('SubmitStatus', ['Active', 'Completing']))
      transformations = self._selectTransformations(
          transType=self.transType, status=status)
      if not transformations['OK']:
        self.log.warn("Could not select transformations:", transformations['Message'])
      else:
        # Get the transformations which should be submitted
        self.tasksPerLoop = self.am_getOption('TasksPerLoop', self.tasksPerLoop)
        res = self.jobManagerClient.getMaxParametricJobs()
        if not res['OK']:
          self.log.warn("Could not get the maxParametricJobs from JobManager", res['Message'])
        else:
          self.maxParametricJobs = res['Value']

        self._addOperationForTransformations(
            self.operationsOnTransformationDict,
            'submitTasks',
            transformations,
            owner=owner,
            ownerGroup=ownerGroup,
            ownerDN=ownerDN)

    # now call _execute...
    future_to_transID = {}
    for transID, transDict in self.operationsOnTransformationDict.items():
      future = self.threadPoolExecutor.submit(self._execute, transDict)
      future_to_transID[future] = transID

    for future in concurrent.futures.as_completed(future_to_transID):
      transID = future_to_transID[future]
      try:
        future.result()
      except Exception as exc:
        self._logError('%d generated an exception: %s' % (transID, exc))
      else:
        self._logInfo('Processed %d' % transID)

    return S_OK()

  def _selectTransformations(self, transType=None, status=None, agentType=None):
    """ get the transformations
    """
    if status is None:
      status = ['Active', 'Completing']
    if agentType is None:
      agentType = ['Automatic']
    selectCond = {}
    if status:
      selectCond['Status'] = status
    if transType is not None:
      selectCond['Type'] = transType
    if agentType:
      selectCond['AgentType'] = agentType
    res = self.transClient.getTransformations(condDict=selectCond)
    if not res['OK']:
      self.log.error("Failed to get transformations:", res['Message'])
    elif not res['Value']:
      self.log.verbose("No transformations found")
    else:
      self.log.verbose("Obtained %d transformations" % len(res['Value']))
    return res

  #############################################################################

  def _getClients(self, ownerDN=None, ownerGroup=None):
    """Returns the clients used in the threads

    This is another function that should be extended.

    The clients provided here are defaults, and should be adapted

    If ownerDN and ownerGroup are not None the clients will delegate to these credentials

    :param str ownerDN: DN of the owner of the submitted jobs
    :param str ownerGroup: group of the owner of the submitted jobs
    :returns: dict of Clients
    """
    threadTransformationClient = TransformationClient()
    threadTaskManager = WorkflowTasks(ownerDN=ownerDN, ownerGroup=ownerGroup)
    threadTaskManager.pluginLocation = self.pluginLocation

    return {'TransformationClient': threadTransformationClient,
            'TaskManager': threadTaskManager}

  def _execute(self, transDict):
    """ This is what runs inside the threads, in practice this is the function that does the real stuff
    """
    # Each thread will have its own clients if we use credentials/shifterProxy
    clients = self._getClients() if self.shifterProxy else \
        self._getClients(ownerGroup=self.credTuple[1], ownerDN=self.credTuple[2]) if self.credentials \
        else None

    method = '_execute'
    operation = 'None'

    startTime = time.time()

    try:
      transID = transDict['TransformationID']
      operations = transDict['Operations']
      if not (self.credentials or self.shifterProxy):
        ownerDN, group = transDict['OwnerDN'], transDict['OwnerGroup']
        clients = self._getClients(ownerDN=ownerDN, ownerGroup=group)
      self._logInfo("Start processing transformation", method=method, transID=transID)
      for operation in operations:
        self._logInfo("Executing %s" % operation, method=method, transID=transID)
        startOperation = time.time()
        res = getattr(self, operation)(transDict, clients)
        if not res['OK']:
          self._logError(
              "Failed to execute '%s': %s" % (operation, res['Message']),
              method=method,
              transID=transID)
        self._logInfo(
            "Executed %s in %.1f seconds" % (operation, time.time() - startOperation),
            method=method,
            transID=transID)
    except Exception as x:  # pylint: disable=broad-except
      self._logException('Exception executing operation %s' % operation, lException=x,
                         method=method, transID=transID)
    finally:
      self._logInfo(
          "Processed transformation in %.1f seconds" % (time.time() - startTime),
          method=method,
          transID=transID)

  #############################################################################
  # real operations done

  def updateTaskStatus(self, transDict, clients):
    """ Updates the task status
    """
    transID = transDict['TransformationID']
    method = 'updateTaskStatus'

    # Get the tasks which are in an UPDATE state, i.e. job statuses + request-specific statuses
    updateStatus = self.am_getOption(
        "TaskUpdateStatus",
        [
            JobStatus.CHECKING,
            JobStatus.DELETED,
            JobStatus.KILLED,
            JobStatus.STAGING,
            JobStatus.STALLED,
            JobStatus.MATCHED,
            JobStatus.RESCHEDULED,
            JobStatus.COMPLETING,
            JobStatus.COMPLETED,
            JobStatus.SUBMITTING,
            JobStatus.RECEIVED,
            JobStatus.WAITING,
            JobStatus.RUNNING,
            "Scheduled",
            "Assigned",
        ],
    )
    condDict = {"TransformationID": transID, "ExternalStatus": updateStatus}
    timeStamp = str(datetime.datetime.utcnow() - datetime.timedelta(minutes=10))

    # Get transformation tasks
    transformationTasks = clients["TransformationClient"].getTransformationTasks(
        condDict=condDict, older=timeStamp, timeStamp="LastUpdateTime"
    )
    if not transformationTasks['OK']:
      self._logError(
          "Failed to get tasks to update:", transformationTasks['Message'],
          method=method,
          transID=transID)
      return transformationTasks
    if not transformationTasks['Value']:
      self._logVerbose("No tasks found to update",
                       method=method, transID=transID)
      return transformationTasks

    # Get status for the transformation tasks
    chunkSize = self.am_getOption('TaskUpdateChunkSize', 0)
    try:
      chunkSize = int(chunkSize)
    except ValueError:
      chunkSize = 0
    if chunkSize:
      self._logVerbose("Getting %d tasks status (chunks of %d)" %
                       (len(transformationTasks['Value']), chunkSize),
                       method=method, transID=transID)
    else:
      self._logVerbose("Getting %d tasks status" %
                       len(transformationTasks['Value']),
                       method=method, transID=transID)
    updated = {}
    for nb, taskChunk in enumerate(breakListIntoChunks(transformationTasks['Value'], chunkSize)
                                   if chunkSize else
                                   [transformationTasks['Value']]):
      submittedTaskStatus = clients['TaskManager'].getSubmittedTaskStatus(taskChunk)
      if not submittedTaskStatus['OK']:
        self._logError("Failed to get updated task states:", submittedTaskStatus['Message'],
                       method=method, transID=transID)
        return submittedTaskStatus
      statusDict = submittedTaskStatus['Value']
      if not statusDict:
        self._logVerbose("%4d: No tasks to update" % nb,
                         method=method, transID=transID)

      # Set status for tasks that changes
      for status, taskIDs in statusDict.items():
        self._logVerbose("%4d: Updating %d task(s) to %s" % (nb, len(taskIDs), status),
                         method=method, transID=transID)
        setTaskStatus = clients['TransformationClient'].setTaskStatus(
            transID, taskIDs, status)
        if not setTaskStatus['OK']:
          self._logError(
              "Failed to update task status for transformation:", setTaskStatus['Message'],
              method=method,
              transID=transID)
          return setTaskStatus
        updated[status] = updated.setdefault(status, 0) + len(taskIDs)

    for status, nb in updated.items():
      self._logInfo(
          "Updated %d tasks to status %s" % (nb, status),
          method=method,
          transID=transID)
    return S_OK()

  def updateFileStatus(self, transDict, clients):
    """ Update the files status
    """
    transID = transDict['TransformationID']
    method = 'updateFileStatus'

    timeStamp = str(datetime.datetime.utcnow() - datetime.timedelta(minutes=10))

    # get transformation files
    condDict = {'TransformationID': transID, 'Status': ['Assigned']}
    transformationFiles = clients['TransformationClient'].getTransformationFiles(
        condDict=condDict,
        older=timeStamp,
        timeStamp='LastUpdate')
    if not transformationFiles['OK']:
      self._logError(
          "Failed to get transformation files to update:",
          transformationFiles['Message'],
          method=method,
          transID=transID)
      return transformationFiles
    if not transformationFiles['Value']:
      self._logInfo("No files to be updated", method=method, transID=transID)
      return transformationFiles

    # Get the status of the transformation files
    # Sort the files by taskID
    taskFiles = {}
    for fileDict in transformationFiles['Value']:
      taskFiles.setdefault(fileDict['TaskID'], []).append(fileDict)

    chunkSize = 100
    self._logVerbose("Getting file status for %d tasks (chunks of %d)" %
                     (len(taskFiles), chunkSize),
                     method=method, transID=transID)
    updated = {}
    # Process 100 tasks at a time
    for nb, taskIDs in enumerate(breakListIntoChunks(taskFiles, chunkSize)):
      fileChunk = []
      for taskID in taskIDs:
        fileChunk += taskFiles[taskID]
      submittedFileStatus = clients['TaskManager'].getSubmittedFileStatus(fileChunk)
      if not submittedFileStatus['OK']:
        self._logError("Failed to get updated file states for transformation:", submittedFileStatus['Message'],
                       method=method, transID=transID)
        return submittedFileStatus
      statusDict = submittedFileStatus['Value']
      if not statusDict:
        self._logVerbose("%4d: No file states to be updated" % nb,
                         method=method, transID=transID)
        continue

      # Set the status of files
      fileReport = FileReport(server=clients['TransformationClient'].getServer())
      for lfn, status in statusDict.items():
        updated[status] = updated.setdefault(status, 0) + 1
        setFileStatus = fileReport.setFileStatus(transID, lfn, status)
        if not setFileStatus['OK']:
          return setFileStatus
      commit = fileReport.commit()
      if not commit['OK']:
        self._logError("Failed to update file states for transformation:", commit['Message'],
                       method=method, transID=transID)
        return commit
      else:
        self._logVerbose("%4d: Updated the states of %d files" % (nb, len(commit['Value'])),
                         method=method, transID=transID)

    for status, nb in updated.items():
      self._logInfo("Updated %d files to status %s" % (nb, status),
                    method=method, transID=transID)
    return S_OK()

  def checkReservedTasks(self, transDict, clients):
    """ Checking Reserved tasks
    """
    transID = transDict['TransformationID']
    method = 'checkReservedTasks'

    # Select the tasks which have been in Reserved status for more than 1 hour for selected transformations
    condDict = {"TransformationID": transID, "ExternalStatus": 'Reserved'}
    time_stamp_older = str(datetime.datetime.utcnow() - datetime.timedelta(hours=1))

    res = clients['TransformationClient'].getTransformationTasks(condDict=condDict, older=time_stamp_older)
    self._logDebug("getTransformationTasks(%s) return value:" % condDict, res,
                   method=method, transID=transID)
    if not res['OK']:
      self._logError("Failed to get Reserved tasks:", res['Message'],
                     method=method, transID=transID)
      return res
    if not res['Value']:
      self._logVerbose("No Reserved tasks found", transID=transID)
      return res
    reservedTasks = res['Value']

    # Update the reserved tasks
    res = clients['TaskManager'].updateTransformationReservedTasks(reservedTasks)
    self._logDebug("updateTransformationReservedTasks(%s) return value:" % reservedTasks, res,
                   method=method, transID=transID)
    if not res['OK']:
      self._logError("Failed to update transformation reserved tasks:", res['Message'],
                     method=method, transID=transID)
      return res
    noTasks = res['Value']['NoTasks']
    taskNameIDs = res['Value']['TaskNameIDs']

    # For the tasks with no associated request found re-set the status of the task in the transformationDB
    if noTasks:
      self._logInfo("Resetting status of %d tasks to Created as no associated job/request found" % len(noTasks),
                    method=method, transID=transID)
      for taskName in noTasks:
        transID, taskID = self._parseTaskName(taskName)
        res = clients['TransformationClient'].setTaskStatus(transID, taskID, 'Created')
        if not res['OK']:
          self._logError("Failed to update task status and ID after recovery:",
                         '%s %s' % (taskName, res['Message']),
                         method=method, transID=transID)
          return res

    # For the tasks for which an associated request was found update the task details in the transformationDB
    for taskName, extTaskID in taskNameIDs.items():
      transID, taskID = self._parseTaskName(taskName)
      self._logInfo("Setting status of %s to Submitted with ID %s" % (taskName, extTaskID),
                    method=method, transID=transID)
      setTaskStatusAndWmsID = clients['TransformationClient'].setTaskStatusAndWmsID(
          transID,
          taskID,
          'Submitted',
          str(extTaskID))
      if not setTaskStatusAndWmsID['OK']:
        self._logError(
            "Failed to update task status and ID after recovery:",
            "%s %s" % (taskName, setTaskStatusAndWmsID['Message']),
            method=method,
            transID=transID)
        return setTaskStatusAndWmsID

    return S_OK()

  def submitTasks(self, transDict, clients):
    """ Submit the tasks to an external system, using the taskManager provided

    :param dict transIDOPBody: transformation body
    :param dict clients: dictionary of client objects

    :return: S_OK/S_ERROR
    """
    transID = transDict['TransformationID']
    transBody = transDict['Body']
    owner = transDict['Owner']
    ownerGroup = transDict['OwnerGroup']
    ownerDN = transDict['OwnerDN']
    method = 'submitTasks'

    # Get all tasks to submit
    tasksToSubmit = clients['TransformationClient'].getTasksToSubmit(
        transID, self.tasksPerLoop)
    self._logDebug("getTasksToSubmit(%s, %s) return value:" % (transID, self.tasksPerLoop), tasksToSubmit,
                   method=method, transID=transID)
    if not tasksToSubmit['OK']:
      self._logError("Failed to obtain tasks:", tasksToSubmit['Message'],
                     method=method, transID=transID)
      return tasksToSubmit
    tasks = tasksToSubmit['Value']['JobDictionary']
    if not tasks:
      self._logVerbose("No tasks found for submission",
                       method=method, transID=transID)
      return tasksToSubmit
    self._logInfo("Obtained %d tasks for submission" % len(tasks),
                  method=method, transID=transID)

    # Prepare tasks and submits them, by chunks
    chunkSize = self.maxParametricJobs if self.bulkSubmissionFlag else self.tasksPerLoop
    for taskDictChunk in breakDictionaryIntoChunks(tasks, chunkSize):
      res = self._prepareAndSubmitAndUpdateTasks(transID, transBody, taskDictChunk,
                                                 owner, ownerDN, ownerGroup,
                                                 clients)
      if not res['OK']:
        return res
      self._logVerbose("Submitted %d jobs, bulkSubmissionFlag = %s" % (len(taskDictChunk), self.bulkSubmissionFlag))

    return S_OK()

  def _prepareAndSubmitAndUpdateTasks(self, transID, transBody, tasks, owner, ownerDN, ownerGroup, clients):
    """ prepare + submit + monitor a dictionary of tasks

    :param int transID: transformation ID
    :param str transBody: transformation job template
    :param dict tasks: dictionary of per task parameters
    :param str owner: owner of the transformation
    :param str ownerDN: DN of the owner of the transformation
    :param str ownerGroup: group of the owner of the transformation
    :param dict clients: dictionary of client objects

    :return: S_OK/S_ERROR
    """

    method = '_prepareAndSubmitAndUpdateTasks'
    # prepare tasks
    preparedTransformationTasks = clients['TaskManager'].prepareTransformationTasks(transBody,
                                                                                    tasks,
                                                                                    owner,
                                                                                    ownerGroup,
                                                                                    ownerDN,
                                                                                    self.bulkSubmissionFlag)
    self._logDebug("prepareTransformationTasks return value:", preparedTransformationTasks,
                   method=method, transID=transID)
    if not preparedTransformationTasks['OK']:
      self._logError("Failed to prepare tasks", preparedTransformationTasks['Message'],
                     method=method, transID=transID)
      return preparedTransformationTasks

    # Submit tasks
    res = clients['TaskManager'].submitTransformationTasks(preparedTransformationTasks['Value'])
    self._logDebug("submitTransformationTasks return value:", res,
                   method=method, transID=transID)
    if not res['OK']:
      self._logError("Failed to submit prepared tasks:", res['Message'],
                     method=method, transID=transID)
      return res

    # Update tasks after submission
    res = clients['TaskManager'].updateDBAfterTaskSubmission(res['Value'])
    self._logDebug("updateDBAfterTaskSubmission return value:", res,
                   method=method, transID=transID)
    if not res['OK']:
      self._logError("Failed to update DB after task submission:", res['Message'],
                     method=method, transID=transID)
      return res

    return S_OK()

  @staticmethod
  def _addOperationForTransformations(
      operationsOnTransformationDict,
      operation,
      transformations,
      owner=None,
      ownerGroup=None,
      ownerDN=None,
  ):
    """Fill the operationsOnTransformationDict"""

    transformationIDsAndBodies = (
        (
            transformation["TransformationID"],
            transformation["Body"],
            transformation["AuthorDN"],
            transformation["AuthorGroup"],
        )
        for transformation in transformations["Value"]
    )
    for transID, body, t_ownerDN, t_ownerGroup in transformationIDsAndBodies:
      if transID in operationsOnTransformationDict:
        operationsOnTransformationDict[transID]["Operations"].append(operation)
      else:
        operationsOnTransformationDict[transID] = {
            "TransformationID": transID,
            "Body": body,
            "Operations": [operation],
            "Owner": owner if owner else getUsernameForDN(t_ownerDN)["Value"],
            "OwnerGroup": ownerGroup if owner else t_ownerGroup,
            "OwnerDN": ownerDN if owner else t_ownerDN,
        }

  def __getCredentials(self):
    """Get the credentials to use if ShifterCredentials are set, otherwise do nothing.

    This function fills the self.credTuple tuple.
    """
    if not self.credentials:
      return S_OK()
    resCred = Operations().getOptionsDict("/Shifter/%s" % self.credentials)
    if not resCred['OK']:
      self.log.error("Cred: Failed to find shifter credentials", self.credentials)
      return resCred
    owner = resCred['Value']['User']
    ownerGroup = resCred['Value']['Group']
    # returns  a list
    ownerDN = getDNForUsername(owner)['Value'][0]
    self.credTuple = (owner, ownerGroup, ownerDN)
    self.log.info("Cred: Tasks will be submitted with the credentials %s:%s" % (owner, ownerGroup))
    return S_OK()
