""" TaskManager contains WorkflowTasks and RequestTasks modules, for managing jobs and requests tasks
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# pylint: disable=protected-access


import six
import time
from six import StringIO
import json
import copy
import os

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.ModuleFactory import ModuleFactory
from DIRAC.Core.Utilities.DErrno import ETSDATA, ETSUKN
from DIRAC.Interfaces.API.Job import Job
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities import TransformationAgentsUtilities

COMPONENT_NAME = 'TaskManager'


class TaskBase(TransformationAgentsUtilities):
  ''' The other classes inside here inherits from this one.
  '''

  def __init__(self, transClient=None, logger=None):
    """ c'tor """

    if not transClient:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    if not logger:
      self.log = gLogger.getSubLogger('TaskBase')
    else:
      self.log = logger

    self.pluginLocation = 'DIRAC.TransformationSystem.Client.TaskManagerPlugin'

    super(TaskBase, self).__init__()

  def prepareTransformationTasks(self, _transBody, _taskDict,  # pylint: disable=no-self-use, unused-argument
                                 owner='', ownerGroup='', ownerDN='',
                                 bulkSubmissionFlag=False):  # pylint: disable=unused-argument
    """ To make sure the method is implemented in the derived class """
    if owner or ownerGroup or ownerDN or bulkSubmissionFlag:  # Makes pylint happy
      pass
    return S_ERROR("Not implemented")

  def submitTransformationTasks(self, _taskDict):  # pylint: disable=no-self-use
    """ To make sure the method is implemented in the derived class """
    return S_ERROR("Not implemented")

  def submitTasksToExternal(self, _task):  # pylint: disable=no-self-use
    """ To make sure the method is implemented in the derived class """
    return S_ERROR("Not implemented")

  def updateDBAfterTaskSubmission(self, taskDict):
    """ Sets tasks status after the submission to "Submitted", in case of success
    """
    updated = 0
    startTime = time.time()
    for taskID, task in taskDict.items():
      transID = task['TransformationID']
      if task['Success']:
        res = self.transClient.setTaskStatusAndWmsID(transID, taskID, 'Submitted',
                                                     str(task['ExternalID']))
        if not res['OK']:
          self._logWarn("Failed to update task status after submission",
                        "%s %s" % (task['ExternalID'], res['Message']),
                        transID=transID, method='updateDBAfterSubmission')
        updated += 1
    if updated:
      self._logInfo("Updated %d tasks in %.1f seconds" % (updated, time.time() - startTime),
                    transID=transID, method='updateDBAfterSubmission')
    return S_OK()

  def updateTransformationReservedTasks(self, _taskDicts):  # pylint: disable=no-self-use
    """ To make sure the method is implemented in the derived class """
    return S_ERROR("Not implemented")

  def getSubmittedTaskStatus(self, _taskDicts):  # pylint: disable=no-self-use
    """ To make sure the method is implemented in the derived class """
    return S_ERROR("Not implemented")

  def getSubmittedFileStatus(self, _fileDicts):  # pylint: disable=no-self-use
    """ To make sure the method is implemented in the derived class """
    return S_ERROR("Not implemented")


class RequestTasks(TaskBase):
  """
  Class for handling tasks for the RMS
  """

  def __init__(self, transClient=None, logger=None, requestClient=None,
               requestClass=None, requestValidator=None,
               ownerDN=None, ownerGroup=None):
    """ c'tor

        the requestClass is by default Request.
        If extensions want to use an extended type, they can pass it as a parameter.
        This is the same behavior as WorfkloTasks and jobClass
    """

    if not logger:
      logger = gLogger.getSubLogger('RequestTasks')

    super(RequestTasks, self).__init__(transClient, logger)
    useCertificates = True if (bool(ownerDN) and bool(ownerGroup)) else False

    if not requestClient:
      self.requestClient = ReqClient(useCertificates=useCertificates,
                                     delegatedDN=ownerDN,
                                     delegatedGroup=ownerGroup)
    else:
      self.requestClient = requestClient

    if not requestClass:
      self.requestClass = Request
    else:
      self.requestClass = requestClass

    if not requestValidator:
      self.requestValidator = RequestValidator()
    else:
      self.requestValidator = requestValidator

  def prepareTransformationTasks(self, transBody, taskDict, owner='', ownerGroup='', ownerDN='',
                                 bulkSubmissionFlag=False):
    """ Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
    """
    if not taskDict:
      return S_OK({})

    if (not owner) or (not ownerGroup):
      res = getProxyInfo(False, False)
      if not res['OK']:
        return res
      proxyInfo = res['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']

    if not ownerDN:
      res = getDNForUsername(owner)
      if not res['OK']:
        return res
      ownerDN = res['Value'][0]

    try:
      transJson = json.loads(transBody)
      self._multiOperationsBody(transJson, taskDict, ownerDN, ownerGroup)
    except ValueError:  # #json couldn't load
      self._singleOperationsBody(transBody, taskDict, ownerDN, ownerGroup)

    return S_OK(taskDict)

  def _multiOperationsBody(self, transJson, taskDict, ownerDN, ownerGroup):
    """ deal with a Request that has multiple operations

    :param transJson: list of lists of string and dictionaries, e.g.:

      .. code :: python

        body = [ ( "ReplicateAndRegister", { "SourceSE":"FOO-SRM", "TargetSE":"BAR-SRM" }),
                 ( "RemoveReplica", { "TargetSE":"FOO-SRM" } ),
               ]

    :param dict taskDict: dictionary of tasks, modified in this function
    :param str ownerDN: certificate DN used for the requests
    :param str onwerGroup: dirac group used for the requests

    :returns: None
    """
    failedTasks = []
    for taskID, task in list(taskDict.items()):
      transID = task['TransformationID']
      if not task.get('InputData'):
        self._logError("Error creating request for task", "%s, No input data" % taskID, transID=transID)
        taskDict.pop(taskID)
        continue
      files = []

      oRequest = Request()
      if isinstance(task['InputData'], list):
        files = task['InputData']
      elif isinstance(task['InputData'], six.string_types):
        files = task['InputData'].split(';')

      # create the operations from the json structure
      for operationTuple in transJson:
        op = Operation()
        op.Type = operationTuple[0]
        for parameter, value in operationTuple[1].items():
          setattr(op, parameter, value)

        for lfn in files:
          opFile = File()
          opFile.LFN = lfn
          op.addFile(opFile)

        oRequest.addOperation(op)

      result = self._assignRequestToTask(oRequest, taskDict, transID, taskID, ownerDN, ownerGroup)
      if not result['OK']:
        failedTasks.append(taskID)
    # Remove failed tasks
    for taskID in failedTasks:
      taskDict.pop(taskID)

  def _singleOperationsBody(self, transBody, taskDict, ownerDN, ownerGroup):
    """ deal with a Request that has just one operation, as it was sofar

    :param transBody: string, can be an empty string
    :param dict taskDict: dictionary of tasks, modified in this function
    :param str ownerDN: certificate DN used for the requests
    :param str onwerGroup: dirac group used for the requests

    :returns: None
    """

    requestOperation = 'ReplicateAndRegister'
    if transBody:
      try:
        _requestType, requestOperation = transBody.split(';')
      except AttributeError:
        pass
    failedTasks = []
    # Do not remove sorted, we might pop elements in the loop
    for taskID, task in taskDict.items():

      transID = task['TransformationID']

      oRequest = Request()
      transfer = Operation()
      transfer.Type = requestOperation
      transfer.TargetSE = task['TargetSE']

      # If there are input files
      if task.get('InputData'):
        if isinstance(task['InputData'], list):
          files = task['InputData']
        elif isinstance(task['InputData'], six.string_types):
          files = task['InputData'].split(';')
        for lfn in files:
          trFile = File()
          trFile.LFN = lfn

          transfer.addFile(trFile)

      oRequest.addOperation(transfer)
      result = self._assignRequestToTask(oRequest, taskDict, transID, taskID, ownerDN, ownerGroup)
      if not result['OK']:
        failedTasks.append(taskID)
    # Remove failed tasks
    for taskID in failedTasks:
      taskDict.pop(taskID)

  def _assignRequestToTask(self, oRequest, taskDict, transID, taskID, ownerDN, ownerGroup):
    """set ownerDN and group to request, and add the request to taskDict if it is
    valid, otherwise remove the task from the taskDict

    :param oRequest: Request
    :param dict taskDict: dictionary of tasks, modified in this function
    :param int transID: Transformation ID
    :param int taskID: Task ID
    :param str ownerDN: certificate DN used for the requests
    :param str onwerGroup: dirac group used for the requests

    :returns: None
    """

    oRequest.RequestName = self._transTaskName(transID, taskID)
    oRequest.OwnerDN = ownerDN
    oRequest.OwnerGroup = ownerGroup

    isValid = self.requestValidator.validate(oRequest)
    if not isValid['OK']:
      self._logError("Error creating request for task", "%s %s" % (taskID, isValid),
                     transID=transID)
      return S_ERROR('Error creating request')
    taskDict[taskID]['TaskObject'] = oRequest
    return S_OK()

  def submitTransformationTasks(self, taskDict):
    """ Submit requests one by one
    """
    submitted = 0
    failed = 0
    startTime = time.time()
    method = 'submitTransformationTasks'
    for task in taskDict.values():
      # transID is the same for all tasks, so pick it up every time here
      transID = task['TransformationID']
      if not task['TaskObject']:
        task['Success'] = False
        failed += 1
        continue
      res = self.submitTaskToExternal(task['TaskObject'])
      if res['OK']:
        task['ExternalID'] = res['Value']
        task['Success'] = True
        submitted += 1
      else:
        self._logError("Failed to submit task to RMS", res['Message'], transID=transID)
        task['Success'] = False
        failed += 1
    if submitted:
      self._logInfo('Submitted %d tasks to RMS in %.1f seconds' % (submitted, time.time() - startTime),
                    transID=transID, method=method)
    if failed:
      self._logWarn('Failed to submit %d tasks to RMS.' % (failed),
                    transID=transID, method=method)
    return S_OK(taskDict)

  def submitTaskToExternal(self, oRequest):
    """
    Submits a request to RMS
    """
    if isinstance(oRequest, self.requestClass):
      return self.requestClient.putRequest(oRequest, useFailoverProxy=False, retryMainService=2)
    return S_ERROR("Request should be a Request object")

  def updateTransformationReservedTasks(self, taskDicts):
    requestNameIDs = {}
    noTasks = []
    for taskDict in taskDicts:
      requestName = self._transTaskName(taskDict['TransformationID'], taskDict['TaskID'])
      reqID = taskDict['ExternalID']
      if reqID and int(reqID):
        requestNameIDs[requestName] = reqID
      else:
        noTasks.append(requestName)
    return S_OK({'NoTasks': noTasks, 'TaskNameIDs': requestNameIDs})

  def getSubmittedTaskStatus(self, taskDicts):
    """
    Check if tasks changed status, and return a list of tasks per new status
    """
    updateDict = {}
    badRequestID = 0
    for taskDict in taskDicts:
      oldStatus = taskDict['ExternalStatus']
      # ExternalID is normally a string
      if taskDict['ExternalID'] and int(taskDict['ExternalID']):
        newStatus = self.requestClient.getRequestStatus(taskDict['ExternalID'])
        if not newStatus['OK']:
          log = self._logVerbose if 'not exist' in newStatus['Message'] else self._logWarn
          log("getSubmittedTaskStatus: Failed to get requestID for request", newStatus['Message'],
              transID=taskDict['TransformationID'])
        else:
          newStatus = newStatus['Value']
          # We don't care updating the tasks to Assigned while the request is being processed
          if newStatus != oldStatus and newStatus != 'Assigned':
            updateDict.setdefault(newStatus, []).append(taskDict['TaskID'])
      else:
        badRequestID += 1
    if badRequestID:
      self._logWarn("%d requests have identifier 0" % badRequestID)
    return S_OK(updateDict)

  def getSubmittedFileStatus(self, fileDicts):
    """
    Check if transformation files changed status, and return a list of taskIDs per new status
    """
    # Don't try and get status of not submitted tasks!
    transID = None
    taskFiles = {}
    for fileDict in fileDicts:
      # There is only one transformation involved, get however the transID in the loop
      transID = fileDict['TransformationID']
      taskID = int(fileDict['TaskID'])
      taskFiles.setdefault(taskID, []).append(fileDict['LFN'])
    # Should not happen, but just in case there are no files, return
    if transID is None:
      return S_OK({})

    res = self.transClient.getTransformationTasks({'TransformationID': transID, 'TaskID': list(taskFiles)})
    if not res['OK']:
      return res
    requestFiles = {}
    for taskDict in res['Value']:
      taskID = taskDict['TaskID']
      externalID = taskDict['ExternalID']
      # Only consider tasks that are submitted, ExternalID is a string
      if taskDict['ExternalStatus'] != 'Created' and externalID and int(externalID):
        requestFiles[externalID] = taskFiles[taskID]

    updateDict = {}
    for requestID, lfnList in requestFiles.items():
      statusDict = self.requestClient.getRequestFileStatus(requestID, lfnList)
      if not statusDict['OK']:
        log = self._logVerbose if 'not exist' in statusDict['Message'] else self._logWarn
        log("Failed to get files status for request", statusDict['Message'],
            transID=transID, method='getSubmittedFileStatus')
      else:
        for lfn, newStatus in statusDict['Value'].items():
          if newStatus == 'Done':
            updateDict[lfn] = 'Processed'
          elif newStatus == 'Failed':
            updateDict[lfn] = 'Problematic'
    return S_OK(updateDict)

############


class WorkflowTasks(TaskBase):
  """ Handles jobs
  """

  def __init__(self, transClient=None, logger=None, submissionClient=None, jobMonitoringClient=None,
               outputDataModule=None, jobClass=None, opsH=None, destinationPlugin=None,
               ownerDN=None, ownerGroup=None):
    """ Generates some default objects.
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works:
        VOs can pass in their job class extension, if present
    """

    if not logger:
      logger = gLogger.getSubLogger('WorkflowTasks')

    super(WorkflowTasks, self).__init__(transClient, logger)

    useCertificates = True if (bool(ownerDN) and bool(ownerGroup)) else False
    if not submissionClient:
      self.submissionClient = WMSClient(useCertificates=useCertificates,
                                        delegatedDN=ownerDN,
                                        delegatedGroup=ownerGroup)
    else:
      self.submissionClient = submissionClient

    if not jobMonitoringClient:
      self.jobMonitoringClient = JobMonitoringClient()
    else:
      self.jobMonitoringClient = jobMonitoringClient

    if not jobClass:
      self.jobClass = Job
    else:
      self.jobClass = jobClass

    if not opsH:
      self.opsH = Operations()
    else:
      self.opsH = opsH

    if not outputDataModule:
      self.outputDataModule = self.opsH.getValue("Transformations/OutputDataModule", "")
    else:
      self.outputDataModule = outputDataModule

    if not destinationPlugin:
      self.destinationPlugin = self.opsH.getValue('Transformations/DestinationPlugin', 'BySE')
    else:
      self.destinationPlugin = destinationPlugin

    self.destinationPlugin_o = None

    self.outputDataModule_o = None

  def prepareTransformationTasks(self, transBody, taskDict, owner='', ownerGroup='',
                                 ownerDN='', bulkSubmissionFlag=False):
    """ Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works.


    :param str transBody: transformation job template
    :param dict taskDict: dictionary of per task parameters
    :param str owner: owner of the transformation
    :param str ownerGroup: group of the owner of the transformation
    :param str ownerDN: DN of the owner of the transformation
    :param bool bulkSubmissionFlag: flag for using bulk submission or not

    :return: S_OK/S_ERROR with updated taskDict
    """

    if (not owner) or (not ownerGroup):
      res = getProxyInfo(False, False)
      if not res['OK']:
        return res
      proxyInfo = res['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']

    if not ownerDN:
      res = getDNForUsername(owner)
      if not res['OK']:
        return res
      ownerDN = res['Value'][0]

    if bulkSubmissionFlag:
      return self.__prepareTasksBulk(transBody, taskDict, owner, ownerGroup, ownerDN)
    # not a bulk submission
    return self.__prepareTasks(transBody, taskDict, owner, ownerGroup, ownerDN)

  def __prepareTasksBulk(self, transBody, taskDict, owner, ownerGroup, ownerDN):
    """ Prepare transformation tasks with a single job object for bulk submission

    :param str transBody: transformation job template
    :param dict taskDict: dictionary of per task parameters
    :param str owner: owner of the transformation
    :param str ownerGroup: group of the owner of the transformation
    :param str ownerDN: DN of the owner of the transformation

    :return: S_OK/S_ERROR with updated taskDict
    """
    if taskDict:
      transID = list(taskDict.values())[0]['TransformationID']
    else:
      return S_OK({})

    method = '__prepareTasksBulk'
    startTime = time.time()

    # Prepare the bulk Job object with common parameters
    oJob = self.jobClass(transBody)
    self._logVerbose('Setting job owner:group to %s:%s' % (owner, ownerGroup),
                     transID=transID, method=method)
    oJob.setOwner(owner)
    oJob.setOwnerGroup(ownerGroup)
    oJob.setOwnerDN(ownerDN)

    try:
      site = oJob.workflow.findParameter('Site').getValue()
    except AttributeError:
      site = None
    jobType = oJob.workflow.findParameter('JobType').getValue()
    transGroup = str(transID).zfill(8)

    # Verify that the JOB_ID parameter is added to the workflow
    if not oJob.workflow.findParameter('JOB_ID'):
      oJob._addParameter(oJob.workflow, 'JOB_ID', 'string', '00000000', "Initial JOB_ID")

    if oJob.workflow.findParameter('PRODUCTION_ID'):
      oJob._setParamValue('PRODUCTION_ID', str(transID).zfill(8))  # pylint: disable=protected-access
    else:
      oJob._addParameter(oJob.workflow,  # pylint: disable=protected-access
                         'PRODUCTION_ID',
                         'string',
                         str(transID).zfill(8),
                         "Production ID")
    oJob.setType(jobType)
    self._logVerbose('Adding default transformation group of %s' % (transGroup),
                     transID=transID, method=method)
    oJob.setJobGroup(transGroup)

    clinicPath = self._checkSickTransformations(transID)
    if clinicPath:
      self._handleHospital(oJob, clinicPath)

    # Collect per job parameters sequences
    paramSeqDict = {}
    # tasks must be sorted because we use bulk submission and we must find the correspondance
    for taskID in sorted(taskDict):
      paramsDict = taskDict[taskID]
      seqDict = {}

      if site is not None:
        paramsDict['Site'] = site
      paramsDict['JobType'] = jobType

      # Handle destination site
      sites = self._handleDestination(paramsDict)
      if not sites:
        self._logError('Could not get a list a sites',
                       transID=transID, method=method)
        return S_ERROR(ETSUKN, "Can not evaluate destination site")
      else:
        self._logVerbose('Setting Site: ', str(sites),
                         transID=transID, method=method)
        seqDict['Site'] = sites

      seqDict['JobName'] = self._transTaskName(transID, taskID)
      seqDict['JOB_ID'] = str(taskID).zfill(8)

      self._logDebug('TransID: %s, TaskID: %s, paramsDict: %s' % (transID, taskID, str(paramsDict)),
                     transID=transID, method=method)

      # Handle Input Data
      inputData = paramsDict.get('InputData')
      if inputData:
        if isinstance(inputData, six.string_types):
          inputData = inputData.replace(' ', '').split(';')
        self._logVerbose('Setting input data to %s' % inputData,
                         transID=transID, method=method)
        seqDict['InputData'] = inputData
      elif paramSeqDict.get('InputData') is not None:
        self._logError("Invalid mixture of jobs with and without input data")
        return S_ERROR(ETSDATA, "Invalid mixture of jobs with and without input data")

      for paramName, paramValue in paramsDict.items():
        if paramName not in ('InputData', 'Site', 'TargetSE'):
          if paramValue:
            self._logVerbose('Setting %s to %s' % (paramName, paramValue),
                             transID=transID, method=method)
            seqDict[paramName] = paramValue

      outputParameterList = []
      if self.outputDataModule:
        res = self.getOutputData({'Job': oJob._toXML(),  # pylint: disable=protected-access
                                  'TransformationID': transID,
                                  'TaskID': taskID, 'InputData': inputData})
        if not res['OK']:
          self._logError("Failed to generate output data", res['Message'],
                         transID=transID, method=method)
          continue
        for name, output in res['Value'].items():
          seqDict[name] = output
          outputParameterList.append(name)
          if oJob.workflow.findParameter(name):
            oJob._setParamValue(name, "%%(%s)s" % name)  # pylint: disable=protected-access
          else:
            oJob._addParameter(oJob.workflow,  # pylint: disable=protected-access
                               name,
                               'JDL',
                               "%%(%s)s" % name,
                               name)

      for pName, seq in seqDict.items():
        paramSeqDict.setdefault(pName, []).append(seq)

    for paramName, paramSeq in paramSeqDict.items():
      if paramName in ['JOB_ID', 'PRODUCTION_ID', 'InputData'] + outputParameterList:
        res = oJob.setParameterSequence(paramName, paramSeq, addToWorkflow=paramName)
      else:
        res = oJob.setParameterSequence(paramName, paramSeq)
      if not res['OK']:
        return res

    if taskDict:
      self._logInfo('Prepared %d tasks' % len(taskDict),
                    transID=transID, method=method, reftime=startTime)

    taskDict['BulkJobObject'] = oJob
    return S_OK(taskDict)

  def __prepareTasks(self, transBody, taskDict, owner, ownerGroup, ownerDN):
    """ Prepare transformation tasks with a job object per task

    :param str transBody: transformation job template
    :param dict taskDict: dictionary of per task parameters
    :param owner: owner of the transformation
    :param str ownerGroup: group of the owner of the transformation
    :param str ownerDN: DN of the owner of the transformation

    :return:  S_OK/S_ERROR with updated taskDict
    """
    if taskDict:
      transID = list(taskDict.values())[0]['TransformationID']
    else:
      return S_OK({})

    method = '__prepareTasks'
    startTime = time.time()

    oJobTemplate = self.jobClass(transBody)
    oJobTemplate.setOwner(owner)
    oJobTemplate.setOwnerGroup(ownerGroup)
    oJobTemplate.setOwnerDN(ownerDN)

    try:
      site = oJobTemplate.workflow.findParameter('Site').getValue()
    except AttributeError:
      site = None
    jobType = oJobTemplate.workflow.findParameter('JobType').getValue()
    templateOK = False
    getOutputDataTiming = 0.

    for taskID, paramsDict in taskDict.items():
      # Create a job for each task and add it to the taskDict
      if not templateOK:
        templateOK = True
        # Update the template with common information
        self._logVerbose('Job owner:group to %s:%s' % (owner, ownerGroup),
                         transID=transID, method=method)
        transGroup = str(transID).zfill(8)
        self._logVerbose('Adding default transformation group of %s' % (transGroup),
                         transID=transID, method=method)
        oJobTemplate.setJobGroup(transGroup)
        if oJobTemplate.workflow.findParameter('PRODUCTION_ID'):
          oJobTemplate._setParamValue('PRODUCTION_ID', str(transID).zfill(8))
        else:
          oJobTemplate._addParameter(oJobTemplate.workflow,
                                     'PRODUCTION_ID',
                                     'string',
                                     str(transID).zfill(8),
                                     "Production ID")
        if not oJobTemplate.workflow.findParameter('JOB_ID'):
          oJobTemplate._addParameter(oJobTemplate.workflow,
                                     'JOB_ID',
                                     'string',
                                     '00000000',
                                     "Initial JOB_ID")

      if site is not None:
        paramsDict['Site'] = site
      paramsDict['JobType'] = jobType
      # Now create the job from the template
      oJob = copy.deepcopy(oJobTemplate)
      constructedName = self._transTaskName(transID, taskID)
      self._logVerbose('Setting task name to %s' % constructedName,
                       transID=transID, method=method)
      oJob.setName(constructedName)
      oJob._setParamValue('JOB_ID', str(taskID).zfill(8))
      inputData = None

      self._logDebug('TransID: %s, TaskID: %s, paramsDict: %s' % (transID, taskID, str(paramsDict)),
                     transID=transID, method=method)

      # These helper functions do the real job
      sites = self._handleDestination(paramsDict)
      if not sites:
        self._logError('Could not get a list a sites',
                       transID=transID, method=method)
        paramsDict['TaskObject'] = ''
        continue
      else:
        self._logDebug('Setting Site: ', str(sites),
                       transID=transID, method=method)
        res = oJob.setDestination(sites)
        if not res['OK']:
          self._logError('Could not set the site: %s' % res['Message'],
                         transID=transID, method=method)
          paramsDict['TaskObject'] = ''
          continue

      self._handleInputs(oJob, paramsDict)
      self._handleRest(oJob, paramsDict)

      clinicPath = self._checkSickTransformations(transID)
      if clinicPath:
        self._handleHospital(oJob, clinicPath)

      paramsDict['TaskObject'] = ''
      if self.outputDataModule:
        getOutputDataTiming -= time.time()
        res = self.getOutputData({'Job': oJob._toXML(), 'TransformationID': transID,
                                  'TaskID': taskID, 'InputData': inputData})
        getOutputDataTiming += time.time()
        if not res['OK']:
          self._logError("Failed to generate output data", res['Message'],
                         transID=transID, method=method)
          continue
        for name, output in res['Value'].items():
          oJob._addJDLParameter(name, ';'.join(output))
      paramsDict['TaskObject'] = oJob
    if taskDict:
      self._logVerbose('Average getOutputData time: %.1f per task' % (getOutputDataTiming / len(taskDict)),
                       transID=transID, method=method)
      self._logInfo('Prepared %d tasks' % len(taskDict),
                    transID=transID, method=method, reftime=startTime)
    return S_OK(taskDict)

  #############################################################################

  def _handleDestination(self, paramsDict):
    """ Handle Sites and TargetSE in the parameters
    """

    try:
      sites = ['ANY']
      if paramsDict['Site']:
        # 'Site' comes from the XML and therefore is ; separated
        sites = fromChar(paramsDict['Site'], sepChar=';')
    except KeyError:
      pass

    if self.destinationPlugin_o:
      destinationPlugin_o = self.destinationPlugin_o
    else:
      res = self.__generatePluginObject(self.destinationPlugin)
      if not res['OK']:
        self._logFatal("Could not generate a destination plugin object")
        return res
      destinationPlugin_o = res['Value']
      self.destinationPlugin_o = destinationPlugin_o

    destinationPlugin_o.setParameters(paramsDict)
    destSites = destinationPlugin_o.run()
    if not destSites:
      return sites

    # Now we need to make the AND with the sites, if defined
    if sites != ['ANY']:
      # Need to get the AND
      destSites &= set(sites)

    return list(destSites)

  def _handleInputs(self, oJob, paramsDict):
    """ set job inputs (+ metadata)
    """
    inputData = paramsDict.get('InputData')
    transID = paramsDict['TransformationID']
    if inputData:
      self._logVerbose('Setting input data to %s' % inputData,
                       transID=transID, method='_handleInputs')
      res = oJob.setInputData(inputData)
      if not res['OK']:
        self._logError("Could not set the inputs: %s" % res['Message'],
                       transID=transID, method='_handleInputs')

  def _handleRest(self, oJob, paramsDict):
    """ add as JDL parameters all the other parameters that are not for inputs or destination
    """
    transID = paramsDict['TransformationID']
    for paramName, paramValue in paramsDict.items():
      if paramName not in ('InputData', 'Site', 'TargetSE'):
        if paramValue:
          self._logDebug('Setting %s to %s' % (paramName, paramValue),
                         transID=transID, method='_handleRest')
          oJob._addJDLParameter(paramName, paramValue)

  def _checkSickTransformations(self, transID):
    """ Check if the transformation is in the transformations to be processed at Hospital or Clinic
    """
    transID = int(transID)
    clinicPath = "Hospital"
    if transID in set(int(x) for x in self.opsH.getValue(os.path.join(clinicPath, "Transformations"), [])):
      return clinicPath
    if "Clinics" in self.opsH.getSections("Hospital").get('Value', []):
      basePath = os.path.join("Hospital", "Clinics")
      clinics = self.opsH.getSections(basePath)['Value']
      for clinic in clinics:
        clinicPath = os.path.join(basePath, clinic)
        if transID in set(int(x) for x in self.opsH.getValue(os.path.join(clinicPath, "Transformations"), [])):
          return clinicPath
    return None

  def _handleHospital(self, oJob, clinicPath):
    """ Optional handle of hospital/clinic jobs
    """
    if not clinicPath:
      return
    oJob.setInputDataPolicy('download', dataScheduling=False)

    # Check first for a clinic, if not it must be the general hospital
    hospitalSite = self.opsH.getValue(os.path.join(clinicPath, "ClinicSite"), '')
    hospitalCEs = self.opsH.getValue(os.path.join(clinicPath, "ClinicCE"), [])
    # If not found, get the hospital parameters
    if not hospitalSite:
      hospitalSite = self.opsH.getValue("Hospital/HospitalSite", 'DIRAC.JobDebugger.ch')
    if not hospitalCEs:
      hospitalCEs = self.opsH.getValue("Hospital/HospitalCEs", [])

    oJob.setDestination(hospitalSite)
    if hospitalCEs:
      oJob._addJDLParameter('GridCE', hospitalCEs)

  def __generatePluginObject(self, plugin):
    """ This simply instantiates the TaskManagerPlugin class with the relevant plugin name
    """
    method = '__generatePluginObject'
    try:
      plugModule = __import__(self.pluginLocation, globals(), locals(), ['TaskManagerPlugin'])
    except ImportError as e:
      self._logException("Failed to import 'TaskManagerPlugin' %s: %s" % (plugin, e),
                         method=method)
      return S_ERROR()
    try:
      plugin_o = getattr(plugModule, 'TaskManagerPlugin')('%s' % plugin, operationsHelper=self.opsH)
      return S_OK(plugin_o)
    except AttributeError as e:
      self._logException("Failed to create %s(): %s." % (plugin, e),
                         method=method)
      return S_ERROR()

  #############################################################################

  def getOutputData(self, paramDict):
    """ Get the list of job output LFNs from the provided plugin
    """
    if not self.outputDataModule_o:
      # Create the module object
      moduleFactory = ModuleFactory()

      moduleInstance = moduleFactory.getModule(self.outputDataModule, None)
      if not moduleInstance['OK']:
        return moduleInstance
      self.outputDataModule_o = moduleInstance['Value']
    # This is the "argument" to the module, set it and then execute
    self.outputDataModule_o.paramDict = paramDict
    return self.outputDataModule_o.execute()

  def submitTransformationTasks(self, taskDict):
    """ Submit the tasks
    """
    if 'BulkJobObject' in taskDict:
      return self.__submitTransformationTasksBulk(taskDict)
    return self.__submitTransformationTasks(taskDict)

  def __submitTransformationTasksBulk(self, taskDict):
    """ Submit jobs in one go with one parametric job
    """
    if not taskDict:
      return S_OK(taskDict)
    startTime = time.time()

    method = '__submitTransformationTasksBulk'

    oJob = taskDict.pop('BulkJobObject')
    # we can only do this, once the job has been popped, or we _might_ crash
    transID = list(taskDict.values())[0]['TransformationID']
    if oJob is None:
      self._logError('no bulk Job object found', transID=transID, method=method)
      return S_ERROR(ETSUKN, 'No bulk job object provided for submission')

    result = self.submitTaskToExternal(oJob)
    if not result['OK']:
      self._logError('Failed to submit tasks to external',
                     transID=transID, method=method)
      return result

    jobIDList = result['Value']
    if len(jobIDList) != len(taskDict):
      for task in taskDict.values():
        task['Success'] = False
      return S_ERROR(ETSUKN, 'Submitted less number of jobs than requested tasks')
    # Get back correspondence with tasks sorted by ID
    for jobID, taskID in zip(jobIDList, sorted(taskDict)):
      taskDict[taskID]['ExternalID'] = jobID
      taskDict[taskID]['Success'] = True

    submitted = len(jobIDList)
    self._logInfo('Submitted %d tasks to WMS in %.1f seconds' % (submitted, time.time() - startTime),
                  transID=transID, method=method)
    return S_OK(taskDict)

  def __submitTransformationTasks(self, taskDict):
    """ Submit jobs one by one
    """
    method = '__submitTransformationTasks'
    submitted = 0
    failed = 0
    startTime = time.time()
    for task in taskDict.values():
      transID = task['TransformationID']
      if not task['TaskObject']:
        task['Success'] = False
        failed += 1
        continue
      res = self.submitTaskToExternal(task['TaskObject'])
      if res['OK']:
        task['ExternalID'] = res['Value']
        task['Success'] = True
        submitted += 1
      else:
        self._logError("Failed to submit task to WMS", res['Message'],
                       transID=transID, method=method)
        task['Success'] = False
        failed += 1
    if submitted:
      self._logInfo('Submitted %d tasks to WMS in %.1f seconds' % (submitted, time.time() - startTime),
                    transID=transID, method=method)
    if failed:
      self._logError('Failed to submit %d tasks to WMS.' % (failed),
                     transID=transID, method=method)
    return S_OK(taskDict)

  def submitTaskToExternal(self, job):
    """ Submits a single job (which can be a bulk one) to the WMS.
    """
    if isinstance(job, six.string_types):
      try:
        oJob = self.jobClass(job)
      except Exception as x:  # pylint: disable=broad-except
        self._logException("Failed to create job object", '', x)
        return S_ERROR("Failed to create job object")
    elif isinstance(job, self.jobClass):
      oJob = job
    else:
      self._logError("No valid job description found")
      return S_ERROR("No valid job description found")

    workflowFileObject = StringIO(oJob._toXML())
    jdl = oJob._toJDL(jobDescriptionObject=workflowFileObject)
    return self.submissionClient.submitJob(jdl, workflowFileObject)

  def updateTransformationReservedTasks(self, taskDicts):
    transID = None
    jobNames = [self._transTaskName(taskDict['TransformationID'], taskDict['TaskID']) for taskDict in taskDicts]
    res = self.jobMonitoringClient.getJobs({'JobName': jobNames})
    if not res['OK']:
      self._logError("Failed to get task from WMS", res['Message'],
                     transID=transID, method='updateTransformationReservedTasks')
      return res
    jobNameIDs = {}
    for wmsID in res['Value']:
      res = self.jobMonitoringClient.getJobSummary(int(wmsID))
      if not res['OK']:
        self._logWarn("Failed to get task summary from WMS", res['Message'],
                      transID=transID, method='updateTransformationReservedTasks')
      else:
        jobNameIDs[res['Value']['JobName']] = int(wmsID)

    noTask = list(set(jobNames) - set(jobNameIDs))
    return S_OK({'NoTasks': noTask, 'TaskNameIDs': jobNameIDs})

  def getSubmittedTaskStatus(self, taskDicts):
    """
    Check the status of a list of tasks and return lists of taskIDs for each new status
    """
    method = 'getSubmittedTaskStatus'

    if taskDicts:
      wmsIDs = [int(taskDict['ExternalID']) for taskDict in taskDicts if int(taskDict['ExternalID'])]
      transID = taskDicts[0]['TransformationID']
    else:
      return S_OK({})
    res = self.jobMonitoringClient.getJobsStatus(wmsIDs)
    if not res['OK']:
      self._logWarn("Failed to get job status from the WMS system",
                    transID=transID, method=method)
      return res
    statusDict = res['Value']
    updateDict = {}
    for taskDict in taskDicts:
      taskID = taskDict['TaskID']
      wmsID = int(taskDict['ExternalID'])
      if not wmsID:
        continue
      oldStatus = taskDict['ExternalStatus']
      newStatus = statusDict.get(wmsID, {}).get('Status', 'Removed')
      if oldStatus != newStatus:
        if newStatus == "Removed":
          self._logVerbose('Production/Job %d/%d removed from WMS while it is in %s status' %
                           (transID, taskID, oldStatus),
                           transID=transID, method=method)
          newStatus = "Failed"
        self._logVerbose('Setting job status for Production/Job %d/%d to %s' % (transID, taskID, newStatus),
                         transID=transID, method=method)
        updateDict.setdefault(newStatus, []).append(taskID)
    return S_OK(updateDict)

  def getSubmittedFileStatus(self, fileDicts):
    """
    Check the status of a list of files and return the new status of each LFN
    """
    if not fileDicts:
      return S_OK({})

    method = 'getSubmittedFileStatus'

    # All files are from the same transformation
    transID = fileDicts[0]['TransformationID']
    taskFiles = {}
    for fileDict in fileDicts:
      jobName = self._transTaskName(transID, fileDict['TaskID'])
      taskFiles.setdefault(jobName, {})[fileDict['LFN']] = fileDict['Status']

    res = self.updateTransformationReservedTasks(fileDicts)
    if not res['OK']:
      self._logWarn("Failed to obtain taskIDs for files",
                    transID=transID, method=method)
      return res
    noTasks = res['Value']['NoTasks']
    taskNameIDs = res['Value']['TaskNameIDs']

    updateDict = {}
    for jobName in noTasks:
      for lfn, oldStatus in taskFiles[jobName].items():
        if oldStatus != 'Unused':
          updateDict[lfn] = 'Unused'

    res = self.jobMonitoringClient.getJobsStatus(taskNameIDs.values())
    if not res['OK']:
      self._logWarn("Failed to get job status from the WMS system",
                    transID=transID, method=method)
      return res
    statusDict = res['Value']
    for jobName, wmsID in taskNameIDs.items():
      jobStatus = statusDict.get(wmsID, {}).get('Status')
      newFileStatus = {'Done': 'Processed',
                       'Completed': 'Processed',
                       'Failed': 'Unused'}.get(jobStatus)
      if newFileStatus:
        for lfn, oldStatus in taskFiles[jobName].items():
          if newFileStatus != oldStatus:
            updateDict[lfn] = newFileStatus
    return S_OK(updateDict)
