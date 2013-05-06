""" TaskManager contains WorkflowsTasks and RequestTasks modules, for managing jobs and requests tasks
"""
__RCSID__ = "$Id$"

COMPONENT_NAME = 'TaskManager'

import re, time, types, os, copy

from DIRAC                                                      import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo                              import getProxyInfo
from DIRAC.Core.Utilities.List                                  import fromChar
from DIRAC.Core.Utilities.ModuleFactory                         import ModuleFactory
from DIRAC.Interfaces.API.Job                                   import Job
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestContainer      import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient         import RequestClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient            import WMSClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient  import JobMonitoringClient
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations        import Operations

class TaskBase( object ):

  def __init__( self, transClient = None, logger = None ):

    if not transClient:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    if not logger:
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
    """ Sets tasks status after the submission to "Submitted", in case of success
    """
    updated = 0
    startTime = time.time()
    for taskID in sorted( taskDict ):
      transID = taskDict[taskID]['TransformationID']
      if taskDict[taskID]['Success']:
        res = self.transClient.setTaskStatusAndWmsID( transID, taskID, 'Submitted',
                                                      str( taskDict[taskID]['ExternalID'] ) )
        if not res['OK']:
          self.log.warn( "updateDBAfterSubmission: Failed to update task status after submission" ,
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

class RequestTasks( TaskBase ):

  def __init__( self, transClient = None, logger = None, requestClient = None ):

    if not logger:
      logger = gLogger.getSubLogger( 'RequestTasks' )

    super( RequestTasks, self ).__init__( transClient, logger )

    if not requestClient:
      self.requestClient = RequestClient()
    else:
      self.requestClient = requestClient

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '' ):
    requestType = 'transfer'
    requestOperation = 'replicateAndRegister'
    if transBody:
      try:
        requestType, requestOperation = transBody.split( ';' )
      except AttributeError:
        pass
    for taskID in sorted( taskDict ):
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
      transID = taskDict['TransformationID']
      taskID = taskDict['TaskID']
      taskName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      res = self.requestClient.getRequestInfo( taskName )
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
      res = self.requestClient.getRequestStatus( taskName )
      newStatus = ''
      if res['OK']:
        newStatus = res['Value']['RequestStatus']
      elif re.search( "Failed to retrieve RequestID for Request", res['Message'] ):
        newStatus = 'Failed'
      else:
        self.log.info( "getSubmittedTaskStatus: Failed to get requestID for request", res['Message'] )
      if newStatus and ( newStatus != oldStatus ):
        if newStatus not in updateDict:
          updateDict[newStatus] = []
        updateDict[newStatus].append( taskID )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = fileDict['TaskID']
      taskName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      if taskName not in taskFiles:
        taskFiles[taskName] = {}
      taskFiles[taskName][fileDict['LFN']] = fileDict['Status']

    updateDict = {}
    for taskName in sorted( taskFiles ):
      lfnDict = taskFiles[taskName]
      res = self.requestClient.getRequestFileStatus( taskName, lfnDict.keys() )
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
  """ Handles jobs
  """

  def __init__( self, transClient = None, logger = None, submissionClient = None, jobMonitoringClient = None,
                outputDataModule = None, jobClass = None, opsH = None ):
    """ Generates some default objects.
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works:
        VOs can pass in their job class extension, if present
    """

    if not logger:
      logger = gLogger.getSubLogger( 'WorkflowTasks' )

    super( WorkflowTasks, self ).__init__( transClient, logger )

    if not submissionClient:
      self.submissionClient = WMSClient()
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
      self.outputDataModule = self.opsH.getValue( "Transformations/OutputDataModule", "" )
    else:
      self.outputDataModule = outputDataModule


  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '' ):
    """ Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works.
    """
    if ( not owner ) or ( not ownerGroup ):
      res = getProxyInfo( False, False )
      if not res['OK']:
        return res
      proxyInfo = res['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']


    for taskNumber in sorted( taskDict ):
      oJob = self.jobClass( transBody )
      paramsDict = taskDict[taskNumber]
      site = oJob.workflow.findParameter( 'Site' ).getValue()
      paramsDict['Site'] = site
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

      self.log.debug( 'TransID: %s, TaskID: %s, paramsDict: %s' % ( transID, taskNumber, str( paramsDict ) ) )

      # These helper functions do the real job
      sites = self._handleDestination( paramsDict )
      if not sites:
        self.log.error( 'Could not get a list a sites' )
        taskDict[taskNumber]['TaskObject'] = ''
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

      taskDict[taskNumber]['TaskObject'] = ''
      if self.outputDataModule:
        res = self.getOutputData( {'Job':oJob._toXML(), 'TransformationID':transID,
                                   'TaskID':taskNumber, 'InputData':inputData},
                                  moduleLocation = self.outputDataModule )
        if not res ['OK']:
          self.log.error( "Failed to generate output data", res['Message'] )
          continue
        for name, output in res['Value'].items():
          oJob._addJDLParameter( name, ';'.join( output ) )
      taskDict[taskNumber]['TaskObject'] = self.jobClass( oJob._toXML() )
    return S_OK( taskDict )

  #############################################################################

  def _handleDestination( self, paramsDict, getSitesForSE = None ):
    """ Handle Sites and TargetSE in the parameters
    """

    try:
      sites = ['ANY']
      if paramsDict['Site']:
        # 'Site' comes from the XML and therefore is ; separated
        sites = fromChar( paramsDict['Site'], sepChar = ';' )
    except KeyError:
      pass

    try:
      seList = ['Unknown']
      if paramsDict['TargetSE']:
        seList = fromChar( paramsDict['TargetSE'] )
    except KeyError:
      pass

    if not seList or seList == ['Unknown']:
      return sites

    # from now on we know there is some TargetSE requested
    if not getSitesForSE:
      from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE

    seSites = []
    for se in seList:
      res = getSitesForSE( se )
      if not res['OK']:
        self.log.warn( 'Could not get Sites associated to SE', res['Message'] )
      else:
        thisSESites = res['Value']
        if not thisSESites:
          continue
        if seSites == []:
          seSites = copy.deepcopy( thisSESites )
        else:
          # We make an OR of the possible sites
          for nSE in list( thisSESites ):
            if nSE not in seSites:
              seSites.append( nSE )

    # Now we need to make the AND with the sites, if defined
    if sites == ['ANY']:
      return seSites
    else:
      # Need to get the AND
      for nSE in list( seSites ):
        if nSE not in sites:
          seSites.remove( nSE )

      return seSites


  def _handleInputs( self, oJob, paramsDict ):
    """ set job inputs (+ metadata)
    """
    inputData = paramsDict.get( 'InputData' )
    if inputData:
      self.log.verbose( 'Setting input data to %s' % inputData )
      oJob.setInputData( inputData )

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
    oJob.setInputDataPolicy( 'download', dataScheduling = False )
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
    """ Submit jobs one by one
    """
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
      self.log.error( 'submitTransformationTasks: Failed to submit %d tasks to WMS.' % ( failed ) )
    return S_OK( taskDict )

  def submitTaskToExternal( self, job ):
    """ Submits a single job to the WMS.
    """
    if type( job ) in types.StringTypes:
      try:
        oJob = self.jobClass( job )
      except Exception, x:
        self.log.exception( "Failed to create job object", '', x )
        return S_ERROR( "Failed to create job object" )
    elif isinstance( job, self.jobClass ):
      oJob = job
    else:
      self.log.error( "No valid job description found" )
      return S_ERROR( "No valid job description found" )
    workflowFile = open( "jobDescription.xml", 'w' )
    workflowFile.write( oJob._toXML() )
    workflowFile.close()
    jdl = oJob._toJDL()
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
        if taskName not in taskNameIDs:
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
      if wmsID in statusDict:
        newStatus = statusDict[wmsID]['Status']
      if oldStatus != newStatus:
        if newStatus == "Removed":
          self.log.verbose( 'Production/Job %d/%d removed from WMS while it is in %s status' % ( transID,
                                                                                                 taskID,
                                                                                                 oldStatus ) )
          newStatus = "Failed"
        self.log.verbose( 'Setting job status for Production/Job %d/%d to %s' % ( transID, taskID, newStatus ) )
        if newStatus not in updateDict:
          updateDict[newStatus] = []
        updateDict[newStatus].append( taskID )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = fileDict['TaskID']
      taskName = str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )
      if taskName not in taskFiles:
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
      if wmsID in statusDict:
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

