# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/TransformationSystem/Client/TaskManager.py $
__RCSID__ = "$Id: TaskManager.py 19505 2009-12-15 15:43:27Z paterson $"
COMPONENT_NAME='TaskManager'

from DIRAC                                                      import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Client.TransformationDBClient   import TransformationDBClient

from DIRAC.RequestManagementSystem.Client.RequestContainer      import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient         import RequestClient

from DIRAC.WorkloadManagementSystem.Client.WMSClient            import WMSClient
from DIRAC.Interfaces.API.Job                                   import Job

import string

class RequestTasks(TaskBase):
  
  def __init__(self):
    TaskBase.__init__(self)
    self.requestClient = RequestClient()
    
  def prepareTasks(self,transBody,taskDict,owner,ownerGroup):
    requestType,requestOperation = transBody.split(';')
    for taskNumber in sortList(taskDict.keys()):
      paramDict = taskDict[taskNumber]
      oRequest = RequestContainer(init=False)
      subRequestIndex = oRequest.initiateSubRequest(requestType)['Value']
      attributeDict = {'Operation':requestOperation,'TargetSE':taskDict['TargetSE']}
      oRequest.setSubRequestAttributes(subRequestIndex,requestType,attributeDict)
      files = []
      for lfn in taskDict['InputData'].split(';'):
        files.append({'LFN':lfn})     
      oRequest.setSubRequestFiles(subRequestIndex,requestType,files)
      requestName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      oRequest.setRequestAttributes({'RequestName':requestName})
      taskDict[taskNumber]['TaskObject'] = oRequest.toXML()['Value']
    return S_OK(taskDict)

  def submitTasks(self,taskDict):
    submitted = 0
    failed = 0
    startTime = time.time()
    for taskID in sortLit(taskDict.keys()):
      taskDict[taskID]
      res = self.submitToExternal(taskDict[taskID]['TaskObject'])
      if res['OK']:
        taskDict[taskID]['ExternalID'] = res['Value']
        taskDict[taskID]['Success'] = True
        submitted +=1
      else:      
        self.log.warn("Failed to submit task to WMS",res['Message'])
        taskDict[taskID]['Success'] = False
        failed += 1
    self.log.info('submitTasks: Submitted %d tasks to RMS in %.1f seconds' % (submitted,time.time()-startTime))
    self.log.info('submitTasks: Failed to submit %d tasks to RMS.' % (failed,time.time()-startTime))
    return S_OK(taskDict)

  def submitToExternal(self,request):
    if not (request in types.StringTypes):
      try:
        name = request.getRequestName()['Value']
        request = request.toXML()['Value']
      except:
        return S_ERROR("Not valid request description")
    else:
      oRequest = RequestContainer(request)
      name = oRequest.getRequestName()['Value']
    return self.requestClient.setRequest(name,request)

  def checkReservedTasks(self,taskDicts):
    taskNameIDs = {}
    noTasks = []
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['JobID']
      taskName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      res = self.requestClient.getRequestInfo(taskName,'RequestManagement/RequestManager')
      if res['OK']:
        taskNameIDs[taskName] = res['Value'][0]
      elif re.search("Failed to retrieve RequestID for Request", res['Message']):
        noTasks.append(taskName)
      else:
        gLogger.warn("Failed to get requestID for request", res['Message'])
    return S_OK({'NoTasks':noTasks,'TaskNameIDs':taskNameIDs})

  def getSubmittedTaskStatus(self,taskDicts):
    updateDict = {}
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['JobID']
      oldStatus = taskDict['WmsStatus']
      taskName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      res = self.requestClient.getRequestStatus(taskName,'RequestManagement/RequestManager')
      newStatus = ''
      if res['OK']:
        newStatus = res['Value']['RequestStatus']
      elif re.search("Failed to retreive RequestID for Request", res['Message']):
        newStatus = 'Failed'
      else:
        self.log.info("getSubmittedTaskStatus: Failed to get requestID for request", res['Message'])
      if newStatus and (newStatus != oldStatus):
        if not updateDict.has_key(newStatus):
          updateDict[newStatus] = []
        updateDict[newStatus].append(taskID) 
    return S_OK(updateDict)
  
  def getSubmittedFileStatus(self,fileDicts):
    taskFiles = {}
    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = fileDict['JobID']
      taskName = str(transID).zfill(8)+'_'+str(taskID).zfill(8) 
      if not taskFiles.has_key(taskName):
        taskFiles[taskName] = {}
      taskFiles[taskName][fileDict['LFN']] = fileDict['Status']
      
    updateDict = {}  
    for taskName in sortList(taskFiles.keys()):
      lfnDict = taskFiles[taskName]
      res = self.requestClient.getRequestFileStatus(taskName,lfnDict.keys(),'RequestManagement/RequestManager')
      if not res['OK']:
        self.log.warn("getSubmittedFileStatus: Failed to get files status for request",res['Message'])
        continue
      for lfn,newStatus in res['Value'].items():
        if newStatus == lfnDict[lfn]:
          pass
        elif newStatus == 'Done':
          updateDict[lfn] = 'Processed'
        elif newStatus == 'Failed':
          updateDict[lfn] = 'Failed'
    return S_OK(updateDict)  

class WorkflowTasks(TaskBase):
  
  def __init__(self):
    TaskBase.__init__(self)
    self.wmsClient = WMSClient()

  def prepareTasks(self,transBody,taskDict,owner,ownerGroup):
    oJob = Job(transBody)
    for taskNumber in sortList(taskDict.keys()):
      paramsDict = taskDict[taskNumber]
      transID = paramsDict['TransformationID']
      self.log.verbose('Setting job owner:group to %s:%s' % (owner,ownerGroup))
      oJob.setOwner(owner)
      oJob.setOwnerGroup(ownerGroup)
      transGroup = str(transID).zfill(8)
      self.log.verbose('Adding default transformation group of %s' % (transGroup))
      oJob.setJobGroup(transGroup)
      constructedName = str(transID).zfill(8)+'_'+str(taskNumber).zfill(8)
      self.log.verbose('Setting task name to %s' % constructedName)
      oJob.setName(constructedName)
      oJob._setParamValue('PRODUCTION_ID',str(transID).zfill(8))
      oJob._setParamValue('JOB_ID',str(taskNumber).zfill(8))
      inputData = None
      for paramName,paramValue in paramsDict.items():
        self.log.verbose('TransID: %s, TaskID: %s, ParamName: %s, ParamValue: %s' %(transID,taskNumber,paramName,paramValue))
        if paramName=='InputData':
          if paramValue:
            self.log.verbose('Setting input data to %s' %paramValue)
            oJob.setInputData(paramValue)
            inputData = paramValue
        if paramName=='Site':
          if not site:
            self.log.verbose('Setting allocated site to: %s' %(paramValue))
            oJob.setDestination(paramValue)
          else:
            self.log.verbose('Setting destination to chosen site %s' %site)
            oJob.setDestination(site)
      res = self.getOutputData({'Job':Job(oJob._toXML()),'TransformationID':transID,'TaskID':taskNumber,'InputData':inputData})
      if not res ['OK']:
        continue
      for name,output in res['Value'].items():
        oJob._addJDLParameter(name,string.join(output,';'))
      taskDict[taskNumber]['TaskObject'] = Job(oJob._toXML())
    return S_OK(taskDict)

  def getOutputData(self,oJob,transID,taskNumber,inputData):
    # This should return a dictionary containing the output data file LFNs to be produced
    return S_OK({})
  
  def submitTasks(self,taskDict):
    submitted = 0
    failed = 0
    startTime = time.time()
    for taskID in sortLit(taskDict.keys()):
      taskDict[taskID]
      res = self.submitToExternal(taskDict[taskID]['TaskObject'])
      if res['OK']:
        taskDict[taskID]['ExternalID'] = res['Value']
        taskDict[taskID]['Success'] = True
        submitted +=1
      else:      
        self.log.warn("Failed to submit task to WMS",res['Message'])
        taskDict[taskID]['Success'] = False
        failed += 1
    self.log.info('submitTasks: Submitted %d tasks to WMS in %.1f seconds' % (submitted,time.time()-startTime))
    self.log.info('submitTasks: Failed to submit %d tasks to WMS.' % (failed,time.time()-startTime))
    return S_OK(taskDict)

  def submitToExternal(self,job):
    if not (job in types.StringTypes):
      try:
        job = job._toXML()
      except:
        return S_ERROR("Not valid job description")
    return self.wmsClient.submitJob(job)
  
  def checkReservedTasks(self,taskDicts):
    taskNames = []
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['JobID']
      taskName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      taskNames.append(taskName)
    res = self.wmsClient.getJobs({'JobName':taskNames})
    if not ['OK']:
      self.log.info("checkReservedTasks: Failed to get task from WMS",res['Message'])
      return res
    taskNameIDs = {}
    allAccounted = True
    for wmsID in res['Value']:
      res = self.wmsClient.getJobPrimarySummary(int(wmsID))
      if not res['OK']:
        self.log.warn("checkReservedTasks: Failed to get task summary from WMS",res['Message'])
        allAccounted = False
        continue
      jobName = res['Value']['JobName']  
      taskNameIDs[jobName] = wmsID
    noTask = []
    if allAccounted:
      for taskName in taskNames:
        if not (taskName in taskNameIDs.keys()):
          noTask.append(taskName)
    return S_OK({'NoTasks':noTask,'TaskNameIDs':taskNameIDs})

  def getSubmittedTaskStatus(self,taskDicts):
    wmsIDs = []
    for taskDict in taskDicts:
      wmsID = taskDict['JobWmsID']
    wmsIDs.append(wmsID)
    res = self.wmsClient.getJobsStatus(wmsIDs)
    if not res['OK']:
      self.log.warn("Failed to get job status from the WMS system")
      return res
    updateDict = {}
    statusDict = result['Value']
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['JobID']
      wmsID = int(taskDict['JobWmsID'])
      if not wmsID:
        continue
      oldStatus = taskDict['WmsStatus']
      newStatus = "Removed"
      if wmsID in statusDict.keys():
        newStatus = statusDict[wmsID]['Status']
      if oldStatus != newStatus:
        if status == "Removed":
          self.log.verbose('Production/Job %d/%d removed from WMS while it is in %s status' % (transID,taskID,oldStatus))
          status = "Failed"
        self.log.verbose('Setting job status for Production/Job %d/%d to %s' % (transID,jobID,status))
        if not updateDict.has_key(status):
          updateDict[status] = []
        updateDict[status].append(taskID)
    return S_OK(updateDict)
  
  def getSubmittedFileStatus(self,fileDicts):
    taskDicts = []
    taskFiles = {}
    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = fileDict['JobID']
      taskName = str(transID).zfill(8)+'_'+str(taskID).zfill(8) 
      if not taskFiles.has_key(taskName):
        taskFiles[taskName] = {}
      taskFiles[taskName][fileDict['LFN']] = fileDict['Status']
    res = self.checkReservedTasks(fileDicts)
    if not res['OK']:
      self.log.warn("Failed to obtain taskIDs for files")
      return res
    noTasks = res['Value']['NoTasks']
    taskNameIDs = res['Value']['TaskNameIDs']
    updateDict = {}
    for taskName in noTasks:
      for lfn,oldStatus in taskFiles[taskName].items():
        if oldStatus != 'Unused':
          updateDict[lfn] = 'Unused'
    res = self.wmsClient.getJobsStatus(taskNameIDs.values())
    if not res['OK']:
      self.log.warn("Failed to get job status from the WMS system")
      return res
    statusDict = res['Value']
    for taskName,wmsID in taskNameIDs.items():
      newFileStatus = ''
      if statusDict.has_key(wmsID):
        jobStatus = statusDict[wmsID]
        if jobStatus in ['Done','Completed']:
          newFileStatus = 'Processed'
        elif jobStatus in ['Failed']:
          newFileStatus = 'Unused'
      if newFileStatus:
        for lfn,oldFileStatus in taskFiles[taskName].items():
          if newFileStatus != oldFileStatus:
            updateDict[lfn] = newFileStatus
    return S_OK(updateDict)
  
class TaskBase:
  
  def __init__(self):
    self.transClient = TransformationDBClient()
  
  def prepareTasks(self,transBody,taskDict,owner,ownerGroup):
    return S_ERROR("Not implemented")
    
  def submitTasks(self,taskDict):
    return S_ERROR("Not implemented")
  
  def submitToExternal(self,task):
    return S_ERROR("Not implemented")
  
  def updateDBAfterSubmission(self,taskDict):
    updated = 0
    startTime = time.time()
    for taskID in sortList(taskDict.keys()):
      taskName = taskDict[taskID]['TaskName']
      if not taskDict[taskID]['Success']:
        transID = taskDict['TransformationID']
        res = self.transClient.setTaskStatus(transID,taskID,'Created')
        if not res['OK']:
          gLogger.warn("updateDBAfterSubmission: Failed to update task status after submission failure" , "%s %s" % (taskName,res['Message']))
      else:
        res = self.transClient.setTaskStatusAndWmsID(transID,taskID,'Submitted',str(taskDict[taskID]['ExternalID']))
        if not res['OK']:
          gLogger.warn("updateDBAfterSubmission: Failed to update task status after submission" , "%s %s" % (taskName,res['Message']))
        gMonitor.addMark("SubmittedTasks",1)
        updated +=1
    gLogger.info("updateDBAfterSubmission: Updated %d tasks in %.1f seconds" % (updated,time.time()-startTime))
    return S_OK()
  
  def checkReservedTasks(self,taskDicts):
    return S_ERROR("Not implemented")

  def getSubmittedTaskStatus(self,taskDicts):
    return S_ERROR("Not implemented")

  def getSubmittedFileStatus(self,fileDicts):
    return S_ERROR("Not implemented")
