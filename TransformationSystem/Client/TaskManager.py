''' TaskManager contains WorkflowsTasks and RequestTasks modules, for managing jobs and requests tasks
'''
__RCSID__ = "$Id$"

COMPONENT_NAME = 'TaskManager'

import time, types, os

from DIRAC                                                      import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo                              import getProxyInfo
from DIRAC.Core.Utilities.List                                  import fromChar
from DIRAC.Core.Utilities.ModuleFactory                         import ModuleFactory
from DIRAC.Interfaces.API.Job                                   import Job
from DIRAC.RequestManagementSystem.Client.ReqClient             import ReqClient
from DIRAC.RequestManagementSystem.Client.Request               import Request
from DIRAC.RequestManagementSystem.Client.Operation             import Operation
from DIRAC.RequestManagementSystem.Client.File                  import File
from DIRAC.RequestManagementSystem.private.RequestValidator     import gRequestValidator
from DIRAC.WorkloadManagementSystem.Client.WMSClient            import WMSClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient  import JobMonitoringClient
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations        import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry          import getDNForUsername

# FIXME: This should disappear!
from DIRAC.RequestManagementSystem.Client.RequestClient         import RequestClient

def _requestName( transID, taskID ):
  return str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )

class TaskBase( object ):
  ''' The other classes inside here inherits from this one.
  '''

  def __init__( self, transClient = None, logger = None ):

    if not transClient:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    if not logger:
      self.log = gLogger.getSubLogger( 'TaskBase' )
    else:
      self.log = logger

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '', ownerDN = '' ):
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

  def __init__( self, transClient = None, logger = None, requestClient = None, requestClass = None, ):
    """ c'tor

        the requestClass is by default Request.
        If extensions want to use an extended type, they can pass it as a parameter.
        This is the same behavior as WorfkloTasks and jobClass
    """

    if not logger:
      logger = gLogger.getSubLogger( 'RequestTasks' )

    super( RequestTasks, self ).__init__( transClient, logger )

    if not requestClient:
      self.requestClient = ReqClient()
    else:
      self.requestClient = requestClient

    if not requestClass:
      self.requestClass = Request
    else:
      self.requestClass = requestClass

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '', ownerDN = '' ):
    """ Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
    """
    if ( not owner ) or ( not ownerGroup ):
      res = getProxyInfo( False, False )
      if not res['OK']:
        return res
      proxyInfo = res['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']

    if not ownerDN:
      res = getDNForUsername( owner )
      if not res['OK']:
        return res
      ownerDN = res['Value'][0]

    requestOperation = 'ReplicateAndRegister'
    if transBody:
      try:
        _requestType, requestOperation = transBody.split( ';' )
      except AttributeError:
        pass

    for taskID in sorted( taskDict ):
      paramDict = taskDict[taskID]
      if paramDict['InputData']:
        transID = paramDict['TransformationID']

        oRequest = Request()
        transfer = Operation()
        transfer.Type = requestOperation
        transfer.TargetSE = paramDict['TargetSE']

        if type( paramDict['InputData'] ) == type( [] ):
          files = paramDict['InputData']
        elif type( paramDict['InputData'] ) == type( '' ):
          files = paramDict['InputData'].split( ';' )
        for lfn in files:
          trFile = File()
          trFile.LFN = lfn

          transfer.addFile( trFile )

        oRequest.addOperation( transfer )
        oRequest.RequestName = _requestName( transID, taskID )
        oRequest.OwnerDN = ownerDN
        oRequest.OwnerGroup = ownerGroup

      isValid = gRequestValidator.validate( oRequest )
      if not isValid['OK']:
        return isValid

      taskDict[taskID]['TaskObject'] = oRequest

    return S_OK( taskDict )

  def submitTransformationTasks( self, taskDict ):
    """ Submit requests one by one
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
        self.log.error( "Failed to submit task to RMS", res['Message'] )
        taskDict[taskID]['Success'] = False
        failed += 1
    self.log.info( 'submitTasks: Submitted %d tasks to RMS in %.1f seconds' % ( submitted, time.time() - startTime ) )
    if failed:
      self.log.warn( 'submitTasks: But at the same time failed to submit %d tasks to RMS.' % ( failed ) )
    return S_OK( taskDict )

  def submitTaskToExternal( self, oRequest ):
    """ Submits a request using ReqClient
    """
    if isinstance( oRequest, self.requestClass ):
      return self.requestClient.putRequest( oRequest )
    else:
      return S_ERROR( "Request should be a Request object" )

  def updateTransformationReservedTasks( self, taskDicts ):
    requestNameIDs = {}
    noTasks = []
    for taskDict in taskDicts:
      requestName = _requestName( taskDict['TransformationID'], taskDict['TaskID'] )

      # FIXME: trying to see if it is in the old system
      reqID = self.__getRequestIDOLDSystem( requestName )
      # FIXME: and in the new, if it is not in the previous one
      if not reqID:
        reqID = self.__getRequestID( requestName )

      if reqID:
        requestNameIDs[requestName] = reqID
      else:
        noTasks.append( requestName )
    return S_OK( {'NoTasks':noTasks, 'TaskNameIDs':requestNameIDs} )

  def __getRequestIDOLDSystem( self, requestName ):
    """ for the OLD RMS
    """
    # FIXME: this should disappear
    try:
      res = RequestClient().getRequestInfo( requestName )
      if res['OK']:
        return res['Value'][0]
      else:
        return 0
    except RuntimeError:
      return 0

  def __getRequestID( self, requestName ):
    """ Getting the info from the new RMS
    """
    # FIXME: this should stay
    res = self.requestClient.getRequestInfo( requestName )
    if res['OK']:
      return res['Value'][0]
    else:
      return 0

  def getSubmittedTaskStatus( self, taskDicts ):
    updateDict = {}

    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['TaskID']
      oldStatus = taskDict['ExternalStatus']
      requestName = _requestName( transID, taskID )

      # FIXME: trying to see if it is in the old system
      newStatus = self.__getRequestStatusOLDSystem( requestName )
      # FIXME: and in the new, if it is not in the previous one
      if not newStatus:
        newStatus = self.__getRequestStatus( requestName )

      if not newStatus:
        self.log.info( "getSubmittedTaskStatus: Failed to get requestID for request" )
      elif newStatus != oldStatus:
        updateDict.setdefault( newStatus, [] ).append( taskDict['TaskID'] )
    return S_OK( updateDict )

  def __getRequestStatusOLDSystem( self, requestName ):
    """ for the OLD RMS
    """
    # FIXME: this should disappear
    try:
      res = RequestClient().getRequestStatus( requestName )
      if res['OK']:
        return res['Value']['RequestStatus']
      else:
        return ''
    except RuntimeError:
      return ''

  def __getRequestStatus( self, requestName ):
    """ Getting the Request status from the new RMS
    """
    # FIXME: this should stay
    res = self.requestClient.getRequestStatus( requestName )
    if res['OK']:
      return res['Value']
    else:
      return ''

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    submittedTasks = {}
    # Don't try and get status of not submitted tasks!
    for fileDict in fileDicts:
      submittedTasks.setdefault( fileDict['TransformationID'], set() ).add( int( fileDict['TaskID'] ) )
    for transID in submittedTasks:
      res = self.transClient.getTransformationTasks( { 'TransformationID':transID, 'TaskID': list( submittedTasks[transID] )} )
      if not res['OK']:
        return res
      for taskDict in res['Value']:
        taskID = taskDict['TaskID']
        if taskDict['ExternalStatus'] == 'Created':
          submittedTasks[transID].remove( taskID )

    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = int( fileDict['TaskID'] )
      if taskID in submittedTasks[transID]:
        requestName = _requestName( transID, taskID )
        taskFiles.setdefault( requestName, {} )[fileDict['LFN']] = fileDict['Status']

    updateDict = {}
    for requestName in sorted( taskFiles ):
      lfnDict = taskFiles[requestName]

      # FIXME: trying to see if it is in the old system
      statusDict = self.__getRequestFileStatusOLDSystem( requestName, lfnDict.keys() )
      # FIXME: and in the new, if it is not in the previous one
      if not statusDict:
        statusDict = self.__getRequestFileStatus( requestName, lfnDict.keys() )

      if not statusDict:
        self.log.warn( "getSubmittedFileStatus: Failed to get files status for request" )
        continue

      for lfn, newStatus in statusDict.items():
        if newStatus == lfnDict[lfn]:
          pass
        elif newStatus == 'Done':
          updateDict[lfn] = 'Processed'
        elif newStatus == 'Failed':
          updateDict[lfn] = 'Problematic'
    return S_OK( updateDict )

  def __getRequestFileStatusOLDSystem( self, requestName, lfns ):
    """ for the OLD RMS
    """
    # FIXME: this should disappear
    try:
      res = RequestClient().getRequestFileStatus( requestName, lfns )
      if res['OK']:
        return res['Value']
      else:
        return {}
    except RuntimeError:
      return {}

  def __getRequestFileStatus( self, requestName, lfns ):
    """ Getting the Request status from the new RMS
    """
    # FIXME: this should stay
    res = self.requestClient.getRequestFileStatus( requestName, lfns )
    if res['OK']:
      return res['Value']
    else:
      return {}


class WorkflowTasks( TaskBase ):
  ''' Handles jobs
  '''

  def __init__( self, transClient = None, logger = None, submissionClient = None, jobMonitoringClient = None,
                outputDataModule = None, jobClass = None, opsH = None ):
    ''' Generates some default objects.
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works:
        VOs can pass in their job class extension, if present
    '''

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


  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '', ownerDN = '' ):
    ''' Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works.
    '''
    if ( not owner ) or ( not ownerGroup ):
      res = getProxyInfo( False, False )
      if not res['OK']:
        return res
      proxyInfo = res['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']

    if not ownerDN:
      res = getDNForUsername( owner )
      if not res['OK']:
        return res
      ownerDN = res['Value'][0]

    for taskNumber in sorted( taskDict ):
      oJob = self.jobClass( transBody )
      paramsDict = taskDict[taskNumber]
      site = oJob.workflow.findParameter( 'Site' ).getValue()
      paramsDict['Site'] = site
      transID = paramsDict['TransformationID']
      self.log.verbose( 'Setting job owner:group to %s:%s' % ( owner, ownerGroup ) )
      oJob.setOwner( owner )
      oJob.setOwnerGroup( ownerGroup )
      oJob.setOwnerDN( ownerDN )
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
    ''' Handle Sites and TargetSE in the parameters
    '''

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

    seSites = set()
    for se in seList:
      res = getSitesForSE( se )
      if not res['OK']:
        self.log.warn( 'Could not get Sites associated to SE', res['Message'] )
      else:
        thisSESites = res['Value']
        if thisSESites:
          # We make an OR of the possible sites
          seSites.update( thisSESites )

    # Now we need to make the AND with the sites, if defined
    if sites != ['ANY']:
      # Need to get the AND
      seSites &= set( sites )

    return list( seSites )


  def _handleInputs( self, oJob, paramsDict ):
    """ set job inputs (+ metadata)
    """
    inputData = paramsDict.get( 'InputData' )
    if inputData:
      self.log.verbose( 'Setting input data to %s' % inputData )
      oJob.setInputData( inputData )

  def _handleRest( self, oJob, paramsDict ):
    ''' add as JDL parameters all the other parameters that are not for inputs or destination
    '''
    for paramName, paramValue in paramsDict.items():
      if paramName not in ( 'InputData', 'Site', 'TargetSE' ):
        if paramValue:
          self.log.verbose( 'Setting %s to %s' % ( paramName, paramValue ) )
          oJob._addJDLParameter( paramName, paramValue )

  def _handleHospital( self, oJob ):
    ''' Optional handle of hospital jobs
    '''
    oJob.setType( 'Hospital' )
    oJob.setInputDataPolicy( 'download', dataScheduling = False )
    hospitalSite = self.opsH.getValue( "Hospital/HospitalSite", 'DIRAC.JobDebugger.ch' )
    oJob.setDestination( hospitalSite )
    hospitalCEs = self.opsH.getValue( "Hospital/HospitalCEs", [] )
    if hospitalCEs:
      oJob._addJDLParameter( 'GridCE', hospitalCEs )

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
    requestNames = []
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['TaskID']
      requestName = _requestName( transID, taskID )
      requestNames.append( requestName )
    res = self.jobMonitoringClient.getJobs( {'JobName':requestNames} )
    if not ['OK']:
      self.log.info( "updateTransformationReservedTasks: Failed to get task from WMS", res['Message'] )
      return res
    requestNameIDs = {}
    allAccounted = True
    for wmsID in res['Value']:
      res = self.jobMonitoringClient.getJobPrimarySummary( int( wmsID ) )
      if not res['OK']:
        self.log.warn( "updateTransformationReservedTasks: Failed to get task summary from WMS", res['Message'] )
        allAccounted = False
        continue
      jobName = res['Value']['JobName']
      requestNameIDs[jobName] = int( wmsID )
    noTask = [requestName for requestName in requestNames if requestName not in requestNameIDs] if allAccounted else []
    return S_OK( {'NoTasks':noTask, 'TaskNameIDs':requestNameIDs} )

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
        updateDict.setdefault( newStatus, [] ).append( taskID )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = fileDict['TaskID']
      requestName = _requestName( transID, taskID )
      taskFiles.setdefault( requestName, {} )[fileDict['LFN']] = fileDict['Status']
    res = self.updateTransformationReservedTasks( fileDicts )
    if not res['OK']:
      self.log.warn( "Failed to obtain taskIDs for files" )
      return res
    noTasks = res['Value']['NoTasks']
    requestNameIDs = res['Value']['TaskNameIDs']
    updateDict = {}
    for requestName in noTasks:
      for lfn, oldStatus in taskFiles[requestName].items():
        if oldStatus != 'Unused':
          updateDict[lfn] = 'Unused'
    res = self.jobMonitoringClient.getJobsStatus( requestNameIDs.values() )
    if not res['OK']:
      self.log.warn( "Failed to get job status from the WMS system" )
      return res
    statusDict = res['Value']
    for requestName, wmsID in requestNameIDs.items():
      newFileStatus = ''
      if wmsID in statusDict:
        jobStatus = statusDict[wmsID]['Status']
        if jobStatus in ['Done', 'Completed']:
          newFileStatus = 'Processed'
        elif jobStatus in ['Failed']:
          newFileStatus = 'Unused'
      if newFileStatus:
        for lfn, oldFileStatus in taskFiles[requestName].items():
          if newFileStatus != oldFileStatus:
            updateDict[lfn] = newFileStatus
    return S_OK( updateDict )

