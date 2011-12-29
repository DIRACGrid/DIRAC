COMPONENT_NAME = 'TaskManager'

from DIRAC                                                      import gConfig, S_OK, S_ERROR

from DIRAC.Core.Utilities.List                                  import sortList
from DIRAC.Interfaces.API.Job                                   import Job

from DIRAC.RequestManagementSystem.Client.RequestContainer      import RequestContainer

from DIRAC.Core.Utilities.ModuleFactory                         import ModuleFactory

import string, re, time, types, os

class TaskBase( object ):

  def __init__( self, transClient = None, logger = None ):

    if not transClient:
      from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    if not logger:
      from DIRAC import gLogger
      self.log = gLogger.getSubLogger( 'TaskBase' )
    else:
      self.log = logger

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '' ):
    return S_ERROR( "Not implemented" )

  def submitTransformationTasks( self, taskDict ):
    return S_ERROR( "Not implemented" )

  def submitTasksToExternal( self, task ):
    return S_ERROR( "Not implemented" )

  def updateDBAfterTaskSubmission( self, taskDict ):
    updated = 0
    startTime = time.time()
    for taskID in sortList( taskDict.keys() ):
      transID = taskDict[taskID]['TransformationID']
      if taskDict[taskID]['Success']:
        res = self.transClient.setTaskStatusAndWmsID( transID, taskID, 'Submitted', str( taskDict[taskID]['ExternalID'] ) )
        if not res['OK']:
          self.log.warn( "updateDBAfterSubmission: Failed to update task status after submission" , "%s %s" % ( taskDict[taskID]['ExternalID'], res['Message'] ) )
        updated += 1
    self.log.info( "updateDBAfterSubmission: Updated %d tasks in %.1f seconds" % ( updated, time.time() - startTime ) )
    return S_OK()

  def updateTransformationReservedTasks( self, taskDicts ):
    return S_ERROR( "Not implemented" )

  def getSubmittedTaskStatus( self, taskDicts ):
    return S_ERROR( "Not implemented" )

  def getSubmittedFileStatus( self, fileDicts ):
    return S_ERROR( "Not implemented" )

class RequestTasks( TaskBase ):

  def __init__( self, transClient = None, logger = None, requestClient = None ):

    if not logger:
      from DIRAC import gLogger
      logger = gLogger.getSubLogger( 'RequestTasks' )

    super( RequestTasks, self ).__init__( transClient, logger )

    if not requestClient:
      from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
      self.requestClient = RequestClient()
    else:
      self.requestClient = requestClient

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '' ):
    requestType = 'transfer'
    requestOperation = 'replicateAndRegister'
    try:
      requestType, requestOperation = transBody.split( ';' )
    except:
      pass
    for taskID in sortList( taskDict.keys() ):
      paramDict = taskDict[taskID]
      transID = paramDict['TransformationID']
      oRequest = RequestContainer( init = False )
      subRequestIndex = oRequest.initiateSubRequest( requestType )['Value']
      attributeDict = {'Operation':requestOperation, 'TargetSE':paramDict['TargetSE']}
      oRequest.setSubRequestAttributes( subRequestIndex, requestType, attributeDict )
      files = []
      for lfn in paramDict['InputData'].split( ';' ):
        files.append( {'LFN':lfn} )
      oRequest.setSubRequestFiles( subRequestIndex, requestType, files )
      requestName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      oRequest.setRequestAttributes( {'RequestName':requestName} )
      taskDict[taskID]['TaskObject'] = oRequest.toXML()['Value']
    return S_OK( taskDict )

  def submitTransformationTasks( self, taskDict ):
    submitted = 0
    failed = 0
    startTime = time.time()
    for taskID in sortList( taskDict.keys() ):
      if not taskDict[taskID]['TaskObject']:
        taskDict[taskID]['Success'] = False
        failed += 1
        continue
      res = self.submitTaskToExternal( taskDict[taskID]['TaskObject'] )
      if res['OK']:
        taskDict[taskID]['ExternalID'] = res['Value']
        taskDict[taskID]['Success'] = True
        submitted += 1
      else:
        self.log.error( "Failed to submit task to RMS", res['Message'] )
        taskDict[taskID]['Success'] = False
        failed += 1
    self.log.info( 'submitTasks: Submitted %d tasks to RMS in %.1f seconds' % ( submitted, time.time() - startTime ) )
    if failed:
      self.log.info( 'submitTasks: Failed to submit %d tasks to RMS.' % ( failed ) )
    return S_OK( taskDict )

  def submitTaskToExternal( self, request ):
    if type( request ) in types.StringTypes:
      oRequest = RequestContainer( request )
      name = oRequest.getRequestName()['Value']
    elif type( request ) == types.InstanceType:
      name = request.getRequestName()['Value']
      request = request.toXML()['Value']
    else:
      return S_ERROR( "Request should be string or request object" )
    return self.requestClient.setRequest( name, request )

  def updateTransformationReservedTasks( self, taskDicts ):
    taskNameIDs = {}
    noTasks = []
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['TaskID']
      taskName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      res = self.requestClient.getRequestInfo( taskName, 'RequestManagement/centralURL' )
      if res['OK']:
        taskNameIDs[taskName] = res['Value'][0]
      elif re.search( "Failed to retrieve RequestID for Request", res['Message'] ):
        noTasks.append( taskName )
      else:
        self.log.warn( "Failed to get requestID for request", res['Message'] )
    return S_OK( {'NoTasks':noTasks, 'TaskNameIDs':taskNameIDs} )

  def getSubmittedTaskStatus( self, taskDicts ):
    updateDict = {}
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['TaskID']
      oldStatus = taskDict['ExternalStatus']
      taskName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      res = self.requestClient.getRequestStatus( taskName, 'RequestManagement/centralURL' )
      newStatus = ''
      if res['OK']:
        newStatus = res['Value']['RequestStatus']
      elif re.search( "Failed to retrieve RequestID for Request", res['Message'] ):
        newStatus = 'Failed'
      else:
        self.log.info( "getSubmittedTaskStatus: Failed to get requestID for request", res['Message'] )
      if newStatus and ( newStatus != oldStatus ):
        if not updateDict.has_key( newStatus ):
          updateDict[newStatus] = []
        updateDict[newStatus].append( taskID )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = fileDict['TaskID']
      taskName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      if not taskFiles.has_key( taskName ):
        taskFiles[taskName] = {}
      taskFiles[taskName][fileDict['LFN']] = fileDict['Status']

    updateDict = {}
    for taskName in sortList( taskFiles.keys() ):
      lfnDict = taskFiles[taskName]
      res = self.requestClient.getRequestFileStatus( taskName, lfnDict.keys(), 'RequestManagement/centralURL' )
      if not res['OK']:
        self.log.warn( "getSubmittedFileStatus: Failed to get files status for request", res['Message'] )
        continue
      for lfn, newStatus in res['Value'].items():
        if newStatus == lfnDict[lfn]:
          pass
        elif newStatus == 'Done':
          updateDict[lfn] = 'Processed'
        elif newStatus == 'Failed':
          updateDict[lfn] = 'Problematic'
    return S_OK( updateDict )

class WorkflowTasks( TaskBase ):

  def __init__( self, transClient = None, logger = None, submissionClient = None, jobMonitoringClient = None,
                outputDataModule = None ):

    if not logger:
      from DIRAC import gLogger
      logger = gLogger.getSubLogger( 'WorkflowTasks' )

    super( WorkflowTasks, self ).__init__( transClient, logger )

    if not submissionClient:
      from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
      self.submissionClient = WMSClient()
    else:
      self.submissionClient = submissionClient

    if not jobMonitoringClient:
      from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
      self.jobMonitoringClient = JobMonitoringClient()
    else:
      self.jobMonitoringClient = jobMonitoringClient

    if not outputDataModule:
      #FIXME: LHCb specific
      self.outputDataModule = gConfig.getValue( "/DIRAC/VOPolicy/OutputDataModule", "LHCbDIRAC.Core.Utilities.OutputDataPolicy" )
    else:
      self.outputDataModule = outputDataModule

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '', job = None ):
    if ( not owner ) or ( not ownerGroup ):
      from DIRAC.Core.Security.ProxyInfo import getProxyInfo
      res = getProxyInfo( False, False )
      if not res['OK']:
        return res
      proxyInfo = res['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']

    if not job:
      oJob = Job( transBody )
    else:
      oJob = job( transBody )

    for taskNumber in sortList( taskDict.keys() ):
      paramsDict = taskDict[taskNumber]
      transID = paramsDict['TransformationID']
      self.log.verbose( 'Setting job owner:group to %s:%s' % ( owner, ownerGroup ) )
      oJob.setOwner( owner )
      oJob.setOwnerGroup( ownerGroup )
      transGroup = str( transID ).zfill( 8 )
      self.log.verbose( 'Adding default transformation group of %s' % ( transGroup ) )
      oJob.setJobGroup( transGroup )
      constructedName = str( transID ).zfill( 8 ) + '_' + str( taskNumber ).zfill( 8 )
      self.log.verbose( 'Setting task name to %s' % constructedName )
      oJob.setName( constructedName )
      oJob._setParamValue( 'PRODUCTION_ID', str( transID ).zfill( 8 ) )
      oJob._setParamValue( 'JOB_ID', str( taskNumber ).zfill( 8 ) )
      inputData = None
      for paramName, paramValue in paramsDict.items():
        self.log.verbose( 'TransID: %s, TaskID: %s, ParamName: %s, ParamValue: %s' % ( transID, taskNumber, paramName, paramValue ) )
        if paramName == 'InputData':
          if paramValue:
            self.log.verbose( 'Setting input data to %s' % paramValue )
            oJob.setInputData( paramValue )
        elif paramName == 'Site':
          if paramValue:
            self.log.verbose( 'Setting allocated site to: %s' % ( paramValue ) )
            oJob.setDestination( paramValue )
        elif paramValue:
          self.log.verbose( 'Setting %s to %s' % ( paramName, paramValue ) )
          oJob._addJDLParameter( paramName, paramValue )

      hospitalTrans = [int( x ) for x in gConfig.getValue( "/Operations/Hospital/Transformations", [] )]
      if int( transID ) in hospitalTrans:
        hospitalSite = gConfig.getValue( "/Operations/Hospital/HospitalSite", 'DIRAC.JobDebugger.ch' )
        hospitalCEs = gConfig.getValue( "/Operations/Hospital/HospitalCEs", [] )
        oJob.setType( 'Hospital' )
        oJob.setDestination( hospitalSite )
        oJob.setInputDataPolicy( 'download', dataScheduling = False )
        if hospitalCEs:
          oJob._addJDLParameter( 'GridRequiredCEs', hospitalCEs )
      taskDict[taskNumber]['TaskObject'] = ''
      res = self.getOutputData( {'Job':oJob._toXML(), 'TransformationID':transID, 'TaskID':taskNumber, 'InputData':inputData},
                                moduleLocation = self.outputDataModule )
      if not res ['OK']:
        self.log.error( "Failed to generate output data", res['Message'] )
        continue
      for name, output in res['Value'].items():
        oJob._addJDLParameter( name, string.join( output, ';' ) )
      taskDict[taskNumber]['TaskObject'] = Job( oJob._toXML() )
    return S_OK( taskDict )

  def getOutputData( self, paramDict, moduleLocation ):
    moduleFactory = ModuleFactory()

    moduleInstance = moduleFactory.getModule( moduleLocation, paramDict )
    if not moduleInstance['OK']:
      return moduleInstance
    module = moduleInstance['Value']
    return module.execute()

  def submitTransformationTasks( self, taskDict ):
    submitted = 0
    failed = 0
    startTime = time.time()
    for taskID in sortList( taskDict.keys() ):
      if not taskDict[taskID]['TaskObject']:
        taskDict[taskID]['Success'] = False
        failed += 1
        continue
      res = self.submitTaskToExternal( taskDict[taskID]['TaskObject'] )
      if res['OK']:
        taskDict[taskID]['ExternalID'] = res['Value']
        taskDict[taskID]['Success'] = True
        submitted += 1
      else:
        self.log.error( "Failed to submit task to WMS", res['Message'] )
        taskDict[taskID]['Success'] = False
        failed += 1
    self.log.info( 'submitTransformationTasks: Submitted %d tasks to WMS in %.1f seconds' % ( submitted, time.time() - startTime ) )
    if failed:
      self.log.info( 'submitTransformationTasks: Failed to submit %d tasks to WMS.' % ( failed ) )
    return S_OK( taskDict )

  def submitTaskToExternal( self, job ):
    if type( job ) in types.StringTypes:
      try:
        job = Job( job )
      except Exception, x:
        self.log.exception( "Failed to create job object", '', x )
        return S_ERROR( "Failed to create job object" )
    elif type( job ) == types.InstanceType:
      pass
    else:
      self.log.error( "No valid job description found" )
      return S_ERROR( "No valid job description found" )
    workflowFile = open( "jobDescription.xml", 'w' )
    workflowFile.write( job._toXML() )
    workflowFile.close()
    jdl = job._toJDL()
    res = self.submissionClient.submitJob( jdl )
    os.remove( "jobDescription.xml" )
    return res

  def updateTransformationReservedTasks( self, taskDicts ):
    taskNames = []
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['TaskID']
      taskName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      taskNames.append( taskName )
    res = self.jobMonitoringClient.getJobs( {'JobName':taskNames} )
    if not ['OK']:
      self.log.info( "updateTransformationReservedTasks: Failed to get task from WMS", res['Message'] )
      return res
    taskNameIDs = {}
    allAccounted = True
    for wmsID in res['Value']:
      res = self.jobMonitoringClient.getJobPrimarySummary( int( wmsID ) )
      if not res['OK']:
        self.log.warn( "updateTransformationReservedTasks: Failed to get task summary from WMS", res['Message'] )
        allAccounted = False
        continue
      jobName = res['Value']['JobName']
      taskNameIDs[jobName] = int( wmsID )
    noTask = []
    if allAccounted:
      for taskName in taskNames:
        if not ( taskName in taskNameIDs.keys() ):
          noTask.append( taskName )
    return S_OK( {'NoTasks':noTask, 'TaskNameIDs':taskNameIDs} )

  def getSubmittedTaskStatus( self, taskDicts ):
    wmsIDs = []
    for taskDict in taskDicts:
      wmsID = int( taskDict['ExternalID'] )
      wmsIDs.append( wmsID )
    res = self.jobMonitoringClient.getJobsStatus( wmsIDs )
    if not res['OK']:
      self.log.warn( "Failed to get job status from the WMS system" )
      return res
    updateDict = {}
    statusDict = res['Value']
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['TaskID']
      wmsID = int( taskDict['ExternalID'] )
      if not wmsID:
        continue
      oldStatus = taskDict['ExternalStatus']
      newStatus = "Removed"
      if wmsID in statusDict.keys():
        newStatus = statusDict[wmsID]['Status']
      if oldStatus != newStatus:
        if newStatus == "Removed":
          self.log.verbose( 'Production/Job %d/%d removed from WMS while it is in %s status' % ( transID, taskID, oldStatus ) )
          newStatus = "Failed"
        self.log.verbose( 'Setting job status for Production/Job %d/%d to %s' % ( transID, taskID, newStatus ) )
        if not updateDict.has_key( newStatus ):
          updateDict[newStatus] = []
        updateDict[newStatus].append( taskID )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskDicts = []
    taskFiles = {}
    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = fileDict['TaskID']
      taskName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      if not taskFiles.has_key( taskName ):
        taskFiles[taskName] = {}
      taskFiles[taskName][fileDict['LFN']] = fileDict['Status']
    res = self.updateTransformationReservedTasks( fileDicts )
    if not res['OK']:
      self.log.warn( "Failed to obtain taskIDs for files" )
      return res
    noTasks = res['Value']['NoTasks']
    taskNameIDs = res['Value']['TaskNameIDs']
    updateDict = {}
    for taskName in noTasks:
      for lfn, oldStatus in taskFiles[taskName].items():
        if oldStatus != 'Unused':
          updateDict[lfn] = 'Unused'
    res = self.jobMonitoringClient.getJobsStatus( taskNameIDs.values() )
    if not res['OK']:
      self.log.warn( "Failed to get job status from the WMS system" )
      return res
    statusDict = res['Value']
    for taskName, wmsID in taskNameIDs.items():
      newFileStatus = ''
      if statusDict.has_key( wmsID ):
        jobStatus = statusDict[wmsID]['Status']
        if jobStatus in ['Done', 'Completed']:
          newFileStatus = 'Processed'
        elif jobStatus in ['Failed']:
          newFileStatus = 'Unused'
      if newFileStatus:
        for lfn, oldFileStatus in taskFiles[taskName].items():
          if newFileStatus != oldFileStatus:
            updateDict[lfn] = newFileStatus
    return S_OK( updateDict )

