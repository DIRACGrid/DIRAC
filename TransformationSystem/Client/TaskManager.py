""" TaskManager contains WorkflowsTasks and RequestTasks modules, for managing jobs and requests tasks
"""
__RCSID__ = "$Id$"

COMPONENT_NAME = 'TaskManager'

import time, types, os

from DIRAC                                                      import gConfig, S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.List                                  import fromChar
from DIRAC.Core.Utilities.ModuleFactory                         import ModuleFactory
from DIRAC.RequestManagementSystem.Client.RequestContainer      import RequestContainer
from DIRAC.Core.Utilities.SiteSEMapping                         import getSitesForSE

class TaskBase( object ):

  def __init__( self, transClient=None, logger=None ):

    if not transClient:
      from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    if not logger:
      self.log = gLogger.getSubLogger( 'TaskBase' )
    else:
      self.log = logger

  def prepareTransformationTasks( self, transBody, taskDict, owner='', ownerGroup='' ):
    return S_ERROR( "Not implemented" )

  def submitTransformationTasks( self, taskDict ):
    return S_ERROR( "Not implemented" )

  def submitTasksToExternal( self, task ):
    return S_ERROR( "Not implemented" )

  def updateDBAfterTaskSubmission( self, taskDict ):
    updated = 0
    startTime = time.time()
    for taskID in sorted( taskDict ):
      transID = taskDict[taskID]['TransformationID']
      if taskDict[taskID]['Success']:
        res = self.transClient.setTaskStatusAndWmsID( transID, taskID, 'Submitted',
                                                      str( taskDict[taskID]['ExternalID'] ) )
        if not res['OK']:
          self.log.error( "updateDBAfterSubmission: Failed to update task status after submission" ,
                         "%s %s" % ( taskDict[taskID]['ExternalID'], res['Message'] ) )
        updated += 1
    self.log.info( "updateDBAfterSubmission: Updated %d tasks in %.1f seconds" % ( updated, time.time() - startTime ) )
    return S_OK()

  def updateTransformationReservedTasks( self, taskDicts ):
    return S_ERROR( "Not implemented" )

  def getSubmittedTaskStatus( self, taskDicts ):
    return S_ERROR( "Not implemented" )

  def getSubmittedFileStatus( self, fileDicts ):
    return S_ERROR( "Not implemented" )

  def __taskName( self, transID, taskID ):
    return str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )

class RequestTasks( TaskBase ):

  def __init__( self, transClient=None, logger=None, requestClient=None ):

    if not logger:
      logger = gLogger.getSubLogger( 'RequestTasks' )

    super( RequestTasks, self ).__init__( transClient, logger )

    if not requestClient:
      from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
      self.requestClient = RequestClient()
    else:
      self.requestClient = requestClient

  def prepareTransformationTasks( self, transBody, taskDict, owner='', ownerGroup='' ):
    requestType = 'transfer'
    requestOperation = 'replicateAndRegister'
    try:
      requestType, requestOperation = transBody.split( ';' )
    except:
      pass
    for taskID in taskDict:
      paramDict = taskDict[taskID]
      transID = paramDict['TransformationID']
      oRequest = RequestContainer( init=False )
      subRequestIndex = oRequest.initiateSubRequest( requestType )['Value']
      attributeDict = {'Operation':requestOperation, 'TargetSE':paramDict['TargetSE']}
      oRequest.setSubRequestAttributes( subRequestIndex, requestType, attributeDict )
      files = [{'LFN':lfn} for lfn in paramDict['InputData'].split( ';' )]
      oRequest.setSubRequestFiles( subRequestIndex, requestType, files )
      oRequest.setRequestAttributes( {'RequestName':self.__taskName( transID, taskID )} )
      oRequest.setCreationtime()
      taskDict[taskID]['TaskObject'] = oRequest.toXML()['Value']
    return S_OK( taskDict )

  def submitTransformationTasks( self, taskDict ):
    submitted = 0
    failed = 0
    startTime = time.time()
    for taskID in taskDict:
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
    """ Submits a request using RequestClient
    """
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
      taskName = self.__taskName( taskDict['TransformationID'], taskDict['TaskID'] )
      res = self.requestClient.getRequestInfo( taskName, 'RequestManagement/centralURL' )
      if res['OK']:
        taskNameIDs[taskName] = res['Value'][0]
      elif "Failed to retrieve RequestID for Request" in res['Message']:
        noTasks.append( taskName )
      else:
        self.log.error( "Failed to get request info for request %s" % taskName, res['Message'] )
    return S_OK( {'NoTasks':noTasks, 'TaskNameIDs':taskNameIDs} )

  def getSubmittedTaskStatus( self, taskDicts ):
    updateDict = {}
    for taskDict in taskDicts:
      taskID = taskDict['TaskID']
      taskName = self.__taskName( taskDict['TransformationID'], taskID )
      res = self.requestClient.getRequestStatus( taskName, 'RequestManagement/centralURL' )
      newStatus = ''
      if res['OK']:
        newStatus = res['Value']['RequestStatus']
      elif "Failed to retrieve RequestID for Request" in res['Message']:
        # Unimportant: just the request is created but not submitted
        self.log.verbose( "getSubmittedFileStatus: Failed to get task status for request %s" % taskName, res['Message'] )
        newStatus = 'Failed'
      else:
        self.log.error( "getSubmittedTaskStatus: Failed to get task status for request %s" % taskName, res['Message'] )
      if newStatus and ( newStatus != taskDict['ExternalStatus'] ):
        updateDict.setdefault( newStatus, [] ).append( taskID )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    for fileDict in fileDicts:
      taskFiles.setdefault( self.__taskName( fileDict['TransformationID'], fileDict['TaskID'] ), {} )[fileDict['LFN']] = fileDict['Status']

    updateDict = {}
    for taskName in sorted( taskFiles ):
      lfnDict = taskFiles[taskName]
      res = self.requestClient.getRequestFileStatus( taskName, lfnDict.keys(), 'RequestManagement/centralURL' )
      if not res['OK']:
        if "Failed to retrieve RequestID for Request" in res['Message'] :
          # Unimportant: just the request is created but not submitted
          self.log.verbose( "getSubmittedFileStatus: Failed to get files status for request %s" % taskName, res['Message'] )
        else:
          self.log.error( "getSubmittedFileStatus: Failed to get files status for request %s" % taskName, res['Message'] )
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
  """ Handles jobs
  """

  def __init__( self, transClient=None, logger=None, submissionClient=None, jobMonitoringClient=None,
                outputDataModule=None, jobClass=None, opsH=None ):
    """ Generates some default objects.
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works:
        VOs can pass in their job class extension, if present
    """

    if not logger:
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
      self.outputDataModule = gConfig.getValue( "/DIRAC/VOPolicy/OutputDataModule", "" )
    else:
      self.outputDataModule = outputDataModule

    if not jobClass:
      from DIRAC.Interfaces.API.Job import Job
      self.jobClass = Job
    else:
      self.jobClass = jobClass

    if not opsH:
      from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
      self.opsH = Operations()
    else:
      self.opsH = opsH


  def prepareTransformationTasks( self, transBody, taskDict, owner='', ownerGroup='' ):
    """ Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works.
    """
    if ( not owner ) or ( not ownerGroup ):
      from DIRAC.Core.Security.ProxyInfo import getProxyInfo
      res = getProxyInfo( False, False )
      if not res['OK']:
        return res
      proxyInfo = res['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']

    oJob = self.jobClass( transBody )

    for taskID in sorted( taskDict ):
      paramsDict = taskDict[taskID]
      transID = paramsDict['TransformationID']
      self.log.verbose( 'Setting job owner:group to %s:%s' % ( owner, ownerGroup ) )
      oJob.setOwner( owner )
      oJob.setOwnerGroup( ownerGroup )
      transGroup = str( transID ).zfill( 8 )
      self.log.verbose( 'Adding default transformation group of %s' % ( transGroup ) )
      oJob.setJobGroup( transGroup )
      constructedName = self.__taskName( transID, taskID )
      self.log.verbose( 'Setting task name to %s' % constructedName )
      oJob.setName( constructedName )
      oJob._setParamValue( 'PRODUCTION_ID', transGroup )
      oJob._setParamValue( 'JOB_ID', str( taskID ).zfill( 8 ) )
      inputData = None

      self.log.debug( 'TransID: %s, TaskID: %s, paramsDict: %s' % ( transID, taskID, str( paramsDict ) ) )

      #These helper functions do the real job
      sites = self._handleDestination( paramsDict )
      if not sites:
        self.log.error( 'Could not get a list a sites', ', '.join( sites ) )
        taskDict[taskID]['TaskObject'] = ''
        continue
      else:
        self.log.verbose( 'Setting Site: ', str( sites ) )
        res = oJob.setDestination( sites )
        if not res['OK']:
          self.log.error( 'Could not set the site: %s' % res['Message'] )
          continue

      self._handleInputs( oJob, paramsDict )
      self._handleRest( oJob, paramsDict )

      hospitalTrans = [int( x ) for x in self.opsH.getValue( "Hospital/Transformations", [] )]
      if int( transID ) in hospitalTrans:
        self._handleHospital( oJob )

      taskDict[taskID]['TaskObject'] = ''
      res = self.getOutputData( {'Job':oJob._toXML(), 'TransformationID':transID,
                                 'TaskID':taskID, 'InputData':inputData},
                                moduleLocation=self.outputDataModule )
      if not res ['OK']:
        self.log.error( "Failed to generate output data", res['Message'] )
        continue
      for name, output in res['Value'].items():
        oJob._addJDLParameter( name, ';'.join( output ) )
      taskDict[taskID]['TaskObject'] = self.jobClass( oJob._toXML() )
    return S_OK( taskDict )

  #############################################################################

  def _handleDestination( self, paramsDict ):
    """ Handle Sites and TargetSE in the parameters
    """

    sites = fromChar( paramsDict.get( 'Site', 'ANY' ) )
    if 'TargetSE' not in paramsDict:
      return sites
    seList = fromChar( paramsDict['TargetSE'] )

    #from now on we know there is some TargetSE requested
    seSites = []
    for se in seList:
      res = getSitesForSE( se )
      if not res['OK']:
        self.log.warn( 'Could not get Sites associated to SE', res['Message'] )
      else:
        thisSESites = res['Value']
        if not thisSESites:
          continue
        seSites += [site for site in thisSESites if site not in seSites]

    # Now we need to make the AND with the sites, if defined
    if sites == ['ANY']:
      return seSites
    else:
      # Need to get the AND
      return [site for site in seSites if site in sites]

  def _handleInputs( self, oJob, paramsDict ):
    """ set job inputs (+ metadata)
    """
    if 'InputData' in paramsDict:
      self.log.verbose( 'Setting input data to %s' % paramsDict['InputData'] )
      oJob.setInputData( paramsDict['InputData'], runNumber=paramsDict['RunNumber'] )

  def _handleRest( self, oJob, paramsDict ):
    """ add as JDL parameters all the other parameters that are not for inputs or destination
    """
    for paramName, paramValue in paramsDict.items():
      if paramName not in ( 'InputData', 'Site', 'TargetSE' ):
        if paramValue:
          self.log.verbose( 'Setting %s to %s' % ( paramName, paramValue ) )
          oJob._addJDLParameter( paramName, paramValue )

  def _handleHospital( self, oJob ):
    """ Optional handle of hospital jobs
    """
    oJob.setType( 'Hospital' )
    oJob.setInputDataPolicy( 'download', dataScheduling=False )
    hospitalSite = self.opsH.getValue( "Hospital/HospitalSite", 'DIRAC.JobDebugger.ch' )
    oJob.setDestination( hospitalSite )
    hospitalCEs = self.opsH.getValue( "Hospital/HospitalCEs", [] )
    if hospitalCEs:
      oJob._addJDLParameter( 'GridRequiredCEs', hospitalCEs )

  #############################################################################

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
    for taskID in sorted( taskDict ):
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
    self.log.info( 'submitTransformationTasks: Submitted %d tasks to WMS in %.1f seconds' % ( submitted,
                                                                                              time.time() - startTime ) )
    if failed:
      self.log.info( 'submitTransformationTasks: Failed to submit %d tasks to WMS.' % ( failed ) )
    return S_OK( taskDict )

  def submitTaskToExternal( self, job ):
    """ Submits a job to the WMS.
    """
    if type( job ) in types.StringTypes:
      try:
        oJob = self.jobClass( job )
      except Exception, x:
        self.log.exception( "Failed to create job object", '', x )
        return S_ERROR( "Failed to create job object" )
    elif type( job ) == types.InstanceType:
      oJob = job
    else:
      self.log.error( "No valid job description found" )
      return S_ERROR( "No valid job description found" )
    xmlFile = "jobDescription.xml"
    workflowFile = open( xmlFile, 'w' )
    workflowFile.write( oJob._toXML() )
    workflowFile.close()
    jdl = oJob._toJDL( xmlFile )
    res = self.submissionClient.submitJob( jdl )
    os.remove( "jobDescription.xml" )
    return res

  def updateTransformationReservedTasks( self, taskDicts ):
    taskNames = [self.__taskName( taskDict['TransformationID'], taskDict['TaskID'] ) for taskDict in taskDicts]
    res = self.jobMonitoringClient.getJobs( {'JobName':taskNames} )
    if not ['OK']:
      self.log.info( "updateTransformationReservedTasks: Failed to get task from WMS", res['Message'] )
      return res
    taskNameIDs = {}
    allAccounted = True
    for wmsID in res['Value']:
      res = self.jobMonitoringClient.getJobPrimarySummary( int( wmsID ) )
      if not res['OK']:
        self.log.error( "updateTransformationReservedTasks: Failed to get task summary from WMS", res['Message'] )
        allAccounted = False
        continue
      jobName = res['Value']['JobName']
      taskNameIDs[jobName] = int( wmsID )
    noTask = []
    if allAccounted:
      noTask = [taskName for taskName in taskNames if taskName not in taskNameIDs]
    return S_OK( {'NoTasks':noTask, 'TaskNameIDs':taskNameIDs} )

  def getSubmittedTaskStatus( self, taskDicts ):
    wmsIDs = [int( taskDict['ExternalID'] ) for taskDict in taskDicts]
    res = self.jobMonitoringClient.getJobsStatus( wmsIDs )
    if not res['OK']:
      self.log.error( "Failed to get job status from the WMS system" )
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
      newStatus = statusDict.get( wmsID, {} ).get( 'Status', 'Removed' )
      if oldStatus != newStatus:
        if newStatus == "Removed":
          self.log.verbose( 'Production/Job %d/%d removed from WMS while it is in %s status' % ( transID,
                                                                                                 taskID,
                                                                                                 oldStatus ) )
          newStatus = "Failed"
        self.log.verbose( 'Setting job status for Production/Job %d/%d to %s' % ( transID, taskID, newStatus ) )
        updateDict.setdefault( newStatus, [] ).append( taskID )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    for fileDict in fileDicts:
      taskFiles.setdefault( self.__taskName( fileDict['TransformationID'], fileDict['TaskID'] ), {} )[fileDict['LFN']] = fileDict['Status']
    res = self.updateTransformationReservedTasks( fileDicts )
    if not res['OK']:
      self.log.error( "Failed to obtain taskIDs for %s files" % len( fileDicts ), res['Message'] )
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
      self.log.error( "Failed to get job status from the WMS system" )
      return res
    statusDict = res['Value']
    for taskName, wmsID in taskNameIDs.items():
      newFileStatus = ''
      if wmsID in statusDict :
        jobStatus = statusDict[wmsID]['Status']
        if jobStatus in ( 'Done', 'Completed' ):
          newFileStatus = 'Processed'
        elif jobStatus in ( 'Failed' ):
          newFileStatus = 'Unused'
        if newFileStatus:
          for lfn, oldFileStatus in taskFiles[taskName].items():
            if newFileStatus != oldFileStatus:
              updateDict[lfn] = newFileStatus
    return S_OK( updateDict )

