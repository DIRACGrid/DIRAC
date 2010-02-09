########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/DIRAC/TransformationSystem/Agent/ReplicationSubmissionAgent.py $
########################################################################

"""  The Replication Submission Agent takes replication tasks created in the transformation database and submits the replication requests to the transfer management system. """

__RCSID__ = "$Id: ReplicationSubmissionAgent.py 20001 2010-01-20 12:47:38Z acsmith $"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestContainer          import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient             import RequestClient
from DIRAC.TransformationSystem.Client.FileReport                   import FileReport
from DIRAC.TransformationSystem.Client.TransforamtionDBClient       import TransformationDBClient

from DIRAC.Core.Utilities.List                                      import sortList
import os, time, string, datetime, re

AGENT_NAME = 'TransformationSystem/ReplicationSubmissionAgent'

class ReplicationSubmissionAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.am_setModuleParam('shifter','DataManager')
    self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    gMonitor.registerActivity("SubmittedTasks","Automatically submitted tasks","Transformation Monitoring","Tasks", gMonitor.OP_ACUM)
    self.transClient = TransformationDBClient()
    self.requestClient = RequestClient()
    return S_OK()

  #############################################################################
  def execute(self):
    """The ReplicationSubmissionAgent execution method. """

    # Determine whether the agent is to be executed
    enableFlag = self.am_getOption('EnableFlag','True')
    if not enableFlag == 'True':
      gLogger.info("%s.execute: Agent is disabled by configuration option %s/EnableFlag" % (AGENT_NAME,self.section))
      return S_OK()

    # Update the files statuses
    res = self.updateFileStatus()
    if not res['OK']:
      gLogger.warn('%s.execute: Failed to update file states' % AGENT_NAME, res['Message'])

    # Update the task statuses
    res = self.updateTaskStatus()
    if not res['OK']:
      gLogger.warn('%s.execute: Failed to update task states' % AGENT_NAME, res['Message'])

    # Determine whether to update tasks stuck in Reserved status
    checkReserved = self.am_getOption('CheckReservedTasks','True')
    if checkReserved == 'True':
      res = self.checkReservedTasks()
      if not res['OK']:
        gLogger.warn('%s.execute: Failed to check Reserved tasks' % AGENT_NAME, res['Message'])

    # Obtain the transformations to be submitted
    submitType = self.am_getOption('TransformationType',['Replication'])
    submitStatus = self.am_getOption('SubmitStatus',['Active'])
    submitAgentType = self.am_getOption('SubmitAgentType',['Automatic'])
    selectCond = {'Type' : submitType, 'Status' : submitStatus, 'AgentType' : submitAgentType}
    res = self.transClient.getTransformations(condDict=selectCond)
    if not res['OK']:
      gLogger.error("%s.execute: Failed to get transformations for submission." % AGENT_NAME,res['Message'])
      return res
    if not res['Value']:
      gLogger.info("%s.execute: No transformations found for submission." % AGENT_NAME)
      return res

    for transformation in res['Value']:
      transID = transformation['TransformationID']
      gLogger.info("%s.execute: Attempting to submit tasks for transformation %d" % (AGENT_NAME,transID))
      res = self.submitTasks(transID)
      if not res['OK']:
        gLogger.error("%s.execute: Failed to submit tasks for transformation" % AGENT_NAME,transID)
    return S_OK()

  def submitTasks(self,transID):
    tasksPerLoop = self.am_getOption('TasksPerLoop',50)
    res = self.transClient.getTasksToSubmit(transID,tasksPerLoop)
    if not res['OK']:
      gLogger.error("%s.submitTasks: Failed to obtain tasks from transformation database" % AGENT_NAME, res['Message'])
      return res
    tasks = res['Value']['JobDictionary']
    if not tasks:
      gLogger.info("%s.submitTasks: No tasks found for submission" % AGENT_NAME)
      return S_OK()
    submitted = 0
    startTime = time.time()
    for taskID, taskDict in tasks.items():
      oRequest = RequestContainer(init=False)
      subRequestIndex = oRequest.initiateSubRequest('transfer')['Value']
      attributeDict = {'Operation':'replicateAndRegister','TargetSE':taskDict['TargetSE']}
      oRequest.setSubRequestAttributes(subRequestIndex,'transfer',attributeDict)
      files = []
      for lfn in taskDict['InputData'].split(';'):
        files.append({'LFN':lfn})     
      oRequest.setSubRequestFiles(subRequestIndex,'transfer',files)
      requestName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      requestString = oRequest.toXML()['Value']
      res = self.requestClient.setRequest(requestName,requestString)
      if not res['OK']:
        gLogger.error("%s.submitTasks: Failed to set replication request" % AGENT_NAME, "%s %s" % (requestName, res['Message']))
        gLogger.debug("%s.submitTasks: %s" % (AGENT_NAME,requestString))
        res = self.transClient.setTaskStatus(transID,taskID,'Created')
        if not res['OK']:
          gLogger.warn("%s.submitTasks: Failed to update task status after submission failure" % AGENT_NAME, "%s %s" % (requestName,res['Message']))
      else:
        requestID = res['Value']
        gLogger.verbose("%s.submitTasks: Successfully set replication request" % AGENT_NAME, requestName)
        res = self.transClient.setTaskStatusAndWmsID(transID,taskID,'Submitted',str(requestID))
        if not res['OK']:
          gLogger.warn("%s.submitTasks: Failed to update task status after submission" % AGENT_NAME, "%s %s" % (requestName,res['Message']))
        gMonitor.addMark("SubmittedTasks",1)
        submitted+=1
    gLogger.info('%s.submitTasks: Transformation %d submission time: %.2f seconds for %d tasks' % (AGENT_NAME,transID,time.time()-startTime,submitted))
    return S_OK()

  def checkReservedTasks(self):
    """ Check if there are tasks in Reserved state for more than an hour, verify that there were not submitted and reset them to Created """
    gLogger.info("%s.checkReservedTasks: Checking Reserved tasks" % AGENT_NAME)

    # Get the transformations which should be checked
    submitType = self.am_getOption('TransformationType',['Replication'])
    submitStatus = self.am_getOption('SubmitStatus',['Active','Stopped'])
    selectCond = {'Type' : submitType, 'Status' : submitStatus}
    res = self.transClient.getTransformations(condDict=selectCond)
    if not res['OK']:
      gLogger.error("%s.checkReservedTasks: Failed to get transformations." % AGENT_NAME,res['Message'])
      return res
    if not res['Value']:
      gLogger.info("%s.checkReservedTasks: No transformations found." % AGENT_NAME)
      return res
    transIDs = []
    for transformation in res['Value']:
      transIDs.append(transformation['TransformationID'])

    # Select the tasks which have been in Reserved status for more than 1 hour for selected transformations
    condDict = {"TransformationID":transIDs,"WmsStatus":'Reserved'}
    time_stamp_older = str(datetime.datetime.utcnow() - datetime.timedelta(hours=1))
    time_stamp_newer = str(datetime.datetime.utcnow() - datetime.timedelta(days=7))
    res = self.transClient.getTransformationTasks(condDict=condDict,older=time_stamp_older,newer=time_stamp_newer, timeStamp='LastUpdateTime')
    if not res['OK']:
      gLogger.error("%s.checkReservedTasks: Failed to get Reserved tasks" % AGENT_NAME, res['Message'])
      return S_OK()
    if not res['Value']:
      gLogger.info("%s.checkReservedTasks: No Reserved tasks found" % AGENT_NAME)
      return S_OK()
    taskNameList = []
    for taskDict in res['Value']:
      transID = taskDict['TransformationID']
      taskID = taskDict['JobID']
      taskName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      taskNameList.append(taskName)

    # Determine the requestID for the Reserved tasks from the request names
    taskNameIDs = {}
    noTasks = []
    for taskName in taskNameList:
      res = self.requestClient.getRequestInfo(taskName,'RequestManagement/RequestManager')
      if res['OK']:
        taskNameIDs[taskName] = res['Value'][0]
      elif re.search("Failed to retrieve RequestID for Request", res['Message']):
        noTasks.append(taskName)
      else:
        gLogger.warn("Failed to get requestID for request", res['Message'])

    # For the tasks with no associated request found re-set the status of the task in the transformationDB
    for taskName in noTasks:
      transID,taskID = taskName.split('_')
      gLogger.info("%s.checkReservedTasks: Resetting status of %s to Reserved as no associated task found" % (AGENT_NAME,taskName))
      res = self.transClient.setTaskStatus(int(transID),int(taskID),'Created')
      if not res['OK']:
        gLogger.warn("%s.checkReservedTasks: Failed to update task status and ID after recovery" % AGENT_NAME, "%s %s" % (taskName,res['Message']))

    # For the tasks for which an associated request was found update the task details in the transformationDB
    for taskName,extTaskID in taskNameIDs.items():
      transID,taskID = taskName.split('_')
      gLogger.info("%s.checkReservedTasks: Resetting status of %s to Created with ID %s" % (AGENT_NAME,taskName,extTaskID))
      res = self.transClient.setTaskStatusAndWmsID(int(transID),int(taskID),'Submitted',str(extTaskID))
      if not res['OK']:
        gLogger.warn("%s.checkReservedTasks: Failed to update task status and ID after recovery" % AGENT_NAME, "%s %s" % (taskName,res['Message']))

    return S_OK()

  def updateFileStatus(self):
    gLogger.info("%s.updateFileStatus: Updating Status of task files" % AGENT_NAME)
   
    #Get the transformations to be updated
    submitType = self.am_getOption('TransformationType',['Replication']) 
    submitStatus = self.am_getOption('SubmitStatus',['Active','Stopped'])
    selectCond = {'Type' : submitType, 'Status' : submitStatus}   
    res = self.transClient.getTransformations(condDict=selectCond)
    if not res['OK']:
      gLogger.error("%s.updateFileStatus: Failed to get transformations." % AGENT_NAME,res['Message'])
      return res
    if not res['Value']:
      gLogger.info("%s.updateFileStatus: No transformations found." % AGENT_NAME)
      return res 
    transIDs = []
    for transformation in res['Value']:
      transIDs.append(transformation['TransformationID'])
      
    # Get the files which are in a UPDATE state
    updateStatus = self.am_getOption('UpdateStatus',['Created','Submitted','Received','Waiting','Running'])
    timeStamp = str(datetime.datetime.utcnow() - datetime.timedelta(minutes=10))
    condDict = {'TransformationID' : transIDs, 'Status' : ['Assigned']}
    res = self.transClient.getTransformationFiles(condDict=condDict,older=timeStamp, timeStamp='LastUpdate')
    if not res['OK']:
      gLogger.error("%s.updateFileStatus: Failed to get transformation files to update." % AGENT_NAME,res['Message'])
      return res
    if not res['Value']:
      gLogger.info("%s.updateFileStatus: No transformation files found to update." % AGENT_NAME)
      return res
    taskDict = {}
    for fileDict in res['Value']:
      transID = fileDict['TransformationID']
      taskID = fileDict['JobID']
      transName = str(transID).zfill(8)+'_'+str(taskID).zfill(8) 
      if not taskDict.has_key(transName):
        taskDict[transName] = {}
      taskDict[transName][fileDict['LFN']] = fileDict['Status']
    gLogger.info("%s.updateFileStatus: Found %s active files from %s tasks." % (AGENT_NAME,len(res['Value']),len(taskDict)))

    for transName,lfnDict in taskDict.items():
      lfns = lfnDict.keys()
      fileReport = FileReport(server='TransformationSystem/TransformationManager')
      transID,taskID = transName.split('_')
      res = self.requestClient.getRequestFileStatus(transName,lfns,'RequestManagement/RequestManager')
      if not res['OK']:
        gLogger.error("%s.updateFileStatus: Failed to get files status for request." % AGENT_NAME,res['Message'])
        continue
      for lfn,status in res['Value'].items():
        if status == lfnDict[lfn]:
          continue
        if status == 'Done':
          fileReport.setFileStatus(int(transID),lfn,'Processed')
        elif status == 'Failed':
          fileReport.setFileStatus(int(transID),lfn,'Failed')
      if not fileReport.getFiles():
        gLogger.info("%s.updateFileStatus: No files to update for %s" % (AGENT_NAME,transName))
        continue
      res = fileReport.commit()
      if not res['OK']:
        gLogger.error("%s.updateFileStatus: Failed to update transformation file status." % AGENT_NAME, res['Message'])
      else:
        for status,update in res['Value'].items():
          gLogger.info("%s.updateFileStatus: Updated %s files for %s to %s." % (AGENT_NAME, update, transName, status))
    gLogger.info("%s.updateFileStatus: Transformation file status update complete" % AGENT_NAME)  
    return S_OK()

  def updateTaskStatus(self):
    gLogger.info("%s.updateTaskStatus: Updating the Status of replication tasks" % AGENT_NAME)

    # Get the transformations to be updated
    submitType = self.am_getOption('TransformationType',['Replication'])
    submitStatus = self.am_getOption('SubmitStatus',['Active','Stopped'])
    selectCond = {'Type' : submitType, 'Status' : submitStatus}
    res = self.transClient.getTransformations(condDict=selectCond)
    if not res['OK']:
      gLogger.error("%s.updateTaskStatus: Failed to get transformations." % AGENT_NAME,res['Message'])
      return res
    if not res['Value']:
      gLogger.info("%s.updateTaskStatus: No transformations found." % AGENT_NAME)
      return res
    transIDs = []
    for transformation in res['Value']:
      transIDs.append(transformation['TransformationID'])

    # Get the tasks which are in a UPDATE state
    updateStatus = self.am_getOption('UpdateStatus',['Created','Submitted','Received','Waiting','Running'])
    condDict = {"TransformationID":transIDs,"WmsStatus":updateStatus}
    timeStamp = str(datetime.datetime.utcnow() - datetime.timedelta(minutes=10))
    res = self.transClient.getTransformationTasks(condDict=condDict,older=timeStamp, timeStamp='LastUpdateTime')
    if not res['OK']:
      gLogger.error("%s.updateTaskStatus: Failed to get tasks to update" % AGENT_NAME, res['Message'])
      return S_OK()
    if not res['Value']:
      gLogger.info("%s.updateTaskStatus: No tasks found to update" % AGENT_NAME)
      return S_OK()
    oldTaskNameStatus = {}
    for taskDict in res['Value']:
      transID = taskDict['TransformationID']
      taskID = taskDict['JobID']
      status = taskDict['WmsStatus']
      taskName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      oldTaskNameStatus[taskName] = status

    # Get the state of the replication requests
    updateStatusDict = {}
    for taskName in oldTaskNameStatus.keys():
      transID,taskID = taskName.split('_')
      transID = int(transID)
      taskID = int(taskID)
      if not updateStatusDict.has_key(transID):
        updateStatusDict[transID] = {}
      res = self.requestClient.getRequestStatus(taskName,'RequestManagement/RequestManager')
      if res['OK']:
        requestStatus = res['Value']['RequestStatus']
        if requestStatus != oldTaskNameStatus[taskName]:
          if not updateStatusDict[transID].has_key(requestStatus):
            updateStatusDict[transID][requestStatus] = []
          updateStatusDict[transID][requestStatus].append(taskID)
      elif re.search("Failed to retreive RequestID for Request", res['Message']):
        if not updateStatusDict[transID].has_key('Failed'):
          updateStatusDict[transID]['Failed'] = []
        updateStatusDict[transID]['Failed'].append(taskID)
      else:
        gLogger.info("%s.updateTaskStatus: Failed to get requestID for request" % AGENT_NAME, res['Message'])

    # Perform the updates of the task statuses
    for transID in sortList(updateStatusDict.keys()):
      statusDict = updateStatusDict[transID]
      for status in sortList(statusDict.keys()):
        taskIDs = statusDict[status]
        gLogger.info("%s.updateTaskStatus: Updating %d task(s) from transformation %d to %s" % (AGENT_NAME,len(taskIDs),transID,status))
        for taskID in taskIDs:
          res = self.transClient.setTaskStatus(transID,taskID,status)        
          if not res['OK']:
            gLogger.error("%s.updateTaskStatus: Failed to update task status" % AGENT_NAME, res['Message'])
    return S_OK()
