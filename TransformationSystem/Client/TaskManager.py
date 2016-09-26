""" TaskManager contains WorkflowsTasks and RequestTasks modules, for managing jobs and requests tasks
"""
import time
import StringIO
import json

from DIRAC                                                      import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo                              import getProxyInfo
from DIRAC.Core.Utilities.List                                  import fromChar
from DIRAC.Core.Utilities.ModuleFactory                         import ModuleFactory
from DIRAC.Core.Utilities.DErrno                                import ETSDATA, ETSUKN
from DIRAC.Interfaces.API.Job                                   import Job
from DIRAC.RequestManagementSystem.Client.ReqClient             import ReqClient
from DIRAC.RequestManagementSystem.Client.Request               import Request
from DIRAC.RequestManagementSystem.Client.Operation             import Operation
from DIRAC.RequestManagementSystem.Client.File                  import File
from DIRAC.RequestManagementSystem.private.RequestValidator     import RequestValidator
from DIRAC.WorkloadManagementSystem.Client.WMSClient            import WMSClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient  import JobMonitoringClient
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations        import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry          import getDNForUsername
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities import TransformationAgentsUtilities


__RCSID__ = "$Id$"

COMPONENT_NAME = 'TaskManager'


def _requestName( transID, taskID ):
  return str( transID ).zfill( 8 ) + '_' + str( taskID ).zfill( 8 )

class TaskBase( TransformationAgentsUtilities ):
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

    self.pluginLocation = 'DIRAC.TransformationSystem.Client.TaskManagerPlugin'

    self.transInThread = {}
    self.debug = False

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '', ownerDN = '',
                                  bulkSubmissionFlag = False):
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

  def __init__( self, transClient = None, logger = None, requestClient = None,
                requestClass = None, requestValidator = None ):
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

    if not requestValidator:
      self.requestValidator = RequestValidator()
    else:
      self.requestValidator = requestValidator


  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '', ownerDN = '',
                                  bulkSubmissionFlag = False):
    """ Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
    """
    if not taskDict:
      return S_OK({})

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

    try:
      transJson = json.loads(transBody)
      self._multiOperationsBody( transJson, taskDict, ownerDN, ownerGroup )
    except ValueError: ##json couldn't load
      self._singleOperationsBody( transBody, taskDict, ownerDN, ownerGroup )

    return S_OK( taskDict )

  def _multiOperationsBody( self, transJson, taskDict, ownerDN, ownerGroup ):
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

    for taskID in sorted( taskDict ):
      paramDict = taskDict[taskID]
      if not paramDict.get('InputData'):
        self.log.error( "Error creating request for task", "%s, No input data" % taskID )
        taskDict.pop( taskID )
        continue
      files = []

      transID = paramDict['TransformationID']
      oRequest = Request()
      if isinstance( paramDict['InputData'], list ):
        files = paramDict['InputData']
      elif isinstance( paramDict['InputData'], basestring ):
        files = paramDict['InputData'].split( ';' )

      # create the operations from the json structure
      for operationTuple in transJson:
        op = Operation()
        op.Type = operationTuple[0]
        for parameter, value in operationTuple[1].iteritems():
          setattr( op, parameter, value )

        for lfn in files:
          opFile = File()
          opFile.LFN = lfn
          op.addFile( opFile )

        oRequest.addOperation( op )

      self._assignRequestToTask( oRequest, taskDict, transID, taskID, ownerDN, ownerGroup )

  def _singleOperationsBody(self, transBody, taskDict, ownerDN, ownerGroup ):
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
        _requestType, requestOperation = transBody.split( ';' )
      except AttributeError:
        pass

    # Do not remove sorted, we might pop elements in the loop
    for taskID in sorted( taskDict ):
      paramDict = taskDict[taskID]

      transID = paramDict['TransformationID']

      oRequest = Request()
      transfer = Operation()
      transfer.Type = requestOperation
      transfer.TargetSE = paramDict['TargetSE']

      # If there are input files
      if paramDict.get('InputData'):
        if isinstance( paramDict['InputData'], list ):
          files = paramDict['InputData']
        elif isinstance( paramDict['InputData'], basestring ):
          files = paramDict['InputData'].split( ';' )
        for lfn in files:
          trFile = File()
          trFile.LFN = lfn

          transfer.addFile( trFile )

      oRequest.addOperation( transfer )
      self._assignRequestToTask( oRequest, taskDict, transID, taskID, ownerDN, ownerGroup )

  def _assignRequestToTask( self, oRequest, taskDict, transID, taskID, ownerDN, ownerGroup ):
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

    oRequest.RequestName = _requestName( transID, taskID )
    oRequest.OwnerDN = ownerDN
    oRequest.OwnerGroup = ownerGroup

    isValid = self.requestValidator.validate( oRequest )
    if not isValid['OK']:
      self.log.error( "Error creating request for task", "%s %s" % ( taskID, isValid ) )
      # This works because we loop over a copy of the keys !
      taskDict.pop( taskID )
      return
    taskDict[taskID]['TaskObject'] = oRequest
    return


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
        self._logError( "Failed to submit task to RMS", res['Message'] )
        taskDict[taskID]['Success'] = False
        failed += 1
    self._logInfo( 'submitTasks: Submitted %d tasks to RMS in %.1f seconds' % ( submitted, time.time() - startTime ) )
    if failed:
      self._logWarn( 'submitTasks: But at the same time failed to submit %d tasks to RMS.' % ( failed ) )
    return S_OK( taskDict )

  def submitTaskToExternal( self, oRequest ):
    """ Submits a request using ReqClient
    """
    if isinstance( oRequest, self.requestClass ):
      return self.requestClient.putRequest( oRequest, useFailoverProxy = False, retryMainService = 2 )
    else:
      return S_ERROR( "Request should be a Request object" )

  def updateTransformationReservedTasks( self, taskDicts ):
    requestNameIDs = {}
    noTasks = []
    for taskDict in taskDicts:
      requestName = _requestName( taskDict['TransformationID'], taskDict['TaskID'] )

      reqID = taskDict['ExternalID']

      if reqID:
        requestNameIDs[requestName] = reqID
      else:
        noTasks.append( requestName )
    return S_OK( {'NoTasks':noTasks, 'TaskNameIDs':requestNameIDs} )


  def getSubmittedTaskStatus( self, taskDicts ):
    updateDict = {}

    for taskDict in taskDicts:
      oldStatus = taskDict['ExternalStatus']

      newStatus = self.requestClient.getRequestStatus( taskDict['ExternalID'] )
      if not newStatus['OK']:
        log = self._logVerbose if 'not exist' in newStatus['Message'] else self.log.warn
        log( "getSubmittedTaskStatus: Failed to get requestID for request", '%s' % newStatus['Message'] )
      else:
        newStatus = newStatus['Value']
        if newStatus != oldStatus:
          updateDict.setdefault( newStatus, [] ).append( taskDict['TaskID'] )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    submittedTasks = {}
    externalIds = {}
    # Don't try and get status of not submitted tasks!
    for fileDict in fileDicts:
      submittedTasks.setdefault( fileDict['TransformationID'], set() ).add( int( fileDict['TaskID'] ) )
    for transID in submittedTasks:
      res = self.transClient.getTransformationTasks( { 'TransformationID':transID, 'TaskID': list( submittedTasks[transID] )} )
      if not res['OK']:
        return res
      for taskDict in res['Value']:
        taskID = taskDict['TaskID']
        externalIds[taskID] = taskDict['ExternalID']
        if taskDict['ExternalStatus'] == 'Created':
          submittedTasks[transID].remove( taskID )

    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = int( fileDict['TaskID'] )
      if taskID in submittedTasks[transID]:
        taskFiles.setdefault( externalIds[taskID], [] ).append( fileDict['LFN'] )

    updateDict = {}
    for requestID in sorted( taskFiles ):
      lfnList = taskFiles[requestID]
      statusDict = self.requestClient.getRequestFileStatus( requestID, lfnList )
      if not statusDict['OK']:
        log = self._logVerbose if 'not exist' in statusDict['Message'] else self.log.warn
        log( "getSubmittedFileStatus: Failed to get files status for request", '%s' % statusDict['Message'] )
        continue

      for lfn, newStatus in statusDict['Value'].items():
        if newStatus == 'Done':
          updateDict[lfn] = 'Processed'
        elif newStatus == 'Failed':
          updateDict[lfn] = 'Problematic'
    return S_OK( updateDict )



class WorkflowTasks( TaskBase ):
  """ Handles jobs
  """

  def __init__( self, transClient = None, logger = None, submissionClient = None, jobMonitoringClient = None,
                outputDataModule = None, jobClass = None, opsH = None, destinationPlugin = None ):
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

    if not destinationPlugin:
      self.destinationPlugin = self.opsH.getValue( 'Transformations/DestinationPlugin', 'BySE' )
    else:
      self.destinationPlugin = destinationPlugin

    self.destinationPlugin_o = None

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '',
                                  ownerDN = '', bulkSubmissionFlag = False ):
    """ Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works.

    :param transBody: transformation job template
    :param taskDict: dictionary of per task parameters
    :param owner: owner of the transformation
    :param ownerGroup: group of the owner of the transformation
    :param ownerDN: DN of the owner of the transformation
    :return:  S_OK/S_ERROR with updated taskDict
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

    if bulkSubmissionFlag:
      return self.__prepareTransformationTasksBulk( transBody, taskDict, owner, ownerGroup, ownerDN )
    else:
      return self.__prepareTransformationTasks( transBody, taskDict, owner, ownerGroup, ownerDN )

  def __prepareTransformationTasksBulk( self, transBody, taskDict, owner, ownerGroup, ownerDN ):
    """ Prepare transformation tasks with a single job object for bulk submission
    """

    transID = taskDict[taskDict.keys()[0]]['TransformationID']

    # Prepare the bulk Job object with common parameters
    oJob = self.jobClass( transBody )

    self._logVerbose( 'Setting job owner:group to %s:%s' % ( owner, ownerGroup ), transID = transID )
    oJob.setOwner( owner )
    oJob.setOwnerGroup( ownerGroup )
    oJob.setOwnerDN( ownerDN )

    jobType = oJob.workflow.findParameter( 'JobType' ).getValue()
    transGroup = str( transID ).zfill( 8 )

    oJob._setParamValue( 'PRODUCTION_ID', str( transID ).zfill( 8 ) )
    oJob.setType( jobType )
    self._logVerbose( 'Adding default transformation group of %s' % ( transGroup ), transID = transID )
    oJob.setJobGroup( transGroup )

    if int( transID ) in [int( x ) for x in self.opsH.getValue( "Hospital/Transformations", [] )]:
      self._handleHospital( oJob )

    # Collect per job parameters sequences
    paramSeqDict = {}
    for taskNumber in sorted( taskDict ):
      seqDict = {}
      paramsDict = taskDict[taskNumber]

      # Handle destination site
      site = oJob.workflow.findParameter( 'Site' ).getValue()
      sites = self._handleDestination( paramsDict )
      if not sites:
        self._logError( 'Could not get a list a sites', transID = transID )
        return S_ERROR( ETSUKN, "Can not evaluate destination site" )
      else:
        self._logVerbose( 'Setting Site: ', str( sites ), transID = transID )
        seqDict['Site'] = sites

      constructedName = str( transID ).zfill( 8 ) + '_' + str( taskNumber ).zfill( 8 )
      self._logVerbose( 'Setting task name to %s' % constructedName, transID = transID )
      seqDict['JobName'] = transGroup
      seqDict['JOB_ID'] = str( taskNumber ).zfill( 8 )

      self._logDebug( 'TransID: %s, TaskID: %s, paramsDict: %s' % ( transID, taskNumber, str( paramsDict ) ), transID = transID )

      # Handle Input Data
      inputData = paramsDict.get( 'InputData' )
      if inputData:
        self._logVerbose( 'Setting input data to %s' % inputData )
        seqDict['InputData'] = inputData
      elif paramSeqDict.get( 'InputData' ) is not None:
        return S_ERROR( ETSDATA, "Invalid mixture of jobs with and without input data" )

      for paramName, paramValue in paramsDict.items():
        if paramName not in ( 'InputData', 'Site', 'TargetSE' ):
          if paramValue:
            self._logVerbose( 'Setting %s to %s' % ( paramName, paramValue ) )
            seqDict[paramName] = paramValue

      if self.outputDataModule:
        res = self.getOutputData( {'Job':oJob._toXML(), 'TransformationID':transID,
                                   'TaskID':taskNumber, 'InputData':inputData},
                                  moduleLocation = self.outputDataModule )
        if not res ['OK']:
          self._logError( "Failed to generate output data", res['Message'], transID = transID )
          continue
        for name, output in res['Value'].items():
          seqDict[name] = ';'.join( output )

      for pName in seqDict:
        paramSeqDict.setdefault( pName, [] )
        paramSeqDict[pName].append( seqDict[pName] )

    for paramName, paramSeq in paramSeqDict.iteritems():
      if paramName in [ 'JOB_ID', 'PRODUCTION_ID', 'InputData' ]:
        oJob.setParameterSequence( paramName, paramSeq, addToWorkflow=paramName )
      else:
        oJob.setParameterSequence( paramName, paramSeq )

    taskDict['BulkJobObject'] = oJob
    return S_OK( taskDict )

  def __prepareTransformationTasks( self, transBody, taskDict, owner, ownerGroup, ownerDN ):
    """ Prepare transformation tasks with a job object per task
    """

    for taskNumber in sorted( taskDict ):
      oJob = self.jobClass( transBody )
      paramsDict = taskDict[taskNumber]
      site = oJob.workflow.findParameter( 'Site' ).getValue()
      paramsDict['Site'] = site
      jobType = oJob.workflow.findParameter( 'JobType' ).getValue()
      paramsDict['JobType'] = jobType
      transID = paramsDict['TransformationID']
      self._logVerbose( 'Setting job owner:group to %s:%s' % ( owner, ownerGroup ), transID = transID )
      oJob.setOwner( owner )
      oJob.setOwnerGroup( ownerGroup )
      oJob.setOwnerDN( ownerDN )
      transGroup = str( transID ).zfill( 8 )
      self._logVerbose( 'Adding default transformation group of %s' % ( transGroup ), transID = transID )
      oJob.setJobGroup( transGroup )
      constructedName = str( transID ).zfill( 8 ) + '_' + str( taskNumber ).zfill( 8 )
      self._logVerbose( 'Setting task name to %s' % constructedName, transID = transID )
      oJob.setName( constructedName )
      oJob._setParamValue( 'PRODUCTION_ID', str( transID ).zfill( 8 ) )
      oJob._setParamValue( 'JOB_ID', str( taskNumber ).zfill( 8 ) )
      inputData = None

      self._logDebug( 'TransID: %s, TaskID: %s, paramsDict: %s' % ( transID, taskNumber, str( paramsDict ) ), transID = transID )

      # These helper functions do the real job
      sites = self._handleDestination( paramsDict )
      if not sites:
        self._logError( 'Could not get a list a sites', transID = transID )
        taskDict[taskNumber]['TaskObject'] = ''
        continue
      else:
        self._logVerbose( 'Setting Site: ', str( sites ), transID = transID )
        res = oJob.setDestination( sites )
        if not res['OK']:
          self._logError( 'Could not set the site: %s' % res['Message'], transID = transID )
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
          self._logError( "Failed to generate output data", res['Message'], transID = transID )
          continue
        for name, output in res['Value'].items():
          oJob._addJDLParameter( name, ';'.join( output ) )
      taskDict[taskNumber]['TaskObject'] = oJob
    return S_OK( taskDict )

  #############################################################################

  def _handleDestination( self, paramsDict ):
    """ Handle Sites and TargetSE in the parameters
    """

    try:
      sites = ['ANY']
      if paramsDict['Site']:
        # 'Site' comes from the XML and therefore is ; separated
        sites = fromChar( paramsDict['Site'], sepChar = ';' )
    except KeyError:
      pass

    if self.destinationPlugin_o:
      destinationPlugin_o = self.destinationPlugin_o
    else:
      res = self.__generatePluginObject( self.destinationPlugin )
      if not res['OK']:
        self._logFatal( "Could not generate a destination plugin object" )
        return res
      destinationPlugin_o = res['Value']

    destinationPlugin_o.setParameters( paramsDict )
    destSites = destinationPlugin_o.run()
    if not destSites:
      return sites

    # Now we need to make the AND with the sites, if defined
    if sites != ['ANY']:
      # Need to get the AND
      destSites &= set( sites )

    return list( destSites )

  def _handleInputs( self, oJob, paramsDict ):
    """ set job inputs (+ metadata)
    """
    inputData = paramsDict.get( 'InputData' )
    if inputData:
      self._logVerbose( 'Setting input data to %s' % inputData )
      oJob.setInputData( inputData )

  def _handleRest( self, oJob, paramsDict ):
    """ add as JDL parameters all the other parameters that are not for inputs or destination
    """
    for paramName, paramValue in paramsDict.items():
      if paramName not in ( 'InputData', 'Site', 'TargetSE' ):
        if paramValue:
          self._logVerbose( 'Setting %s to %s' % ( paramName, paramValue ) )
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
      oJob._addJDLParameter( 'GridCE', hospitalCEs )


  def __generatePluginObject( self, plugin ):
    """ This simply instantiates the TaskManagerPlugin class with the relevant plugin name
    """
    try:
      plugModule = __import__( self.pluginLocation, globals(), locals(), ['TaskManagerPlugin'] )
    except ImportError, e:
      self._logException( "Failed to import 'TaskManagerPlugin' %s: %s" % ( plugin, e ) )
      return S_ERROR()
    try:
      plugin_o = getattr( plugModule, 'TaskManagerPlugin' )( '%s' % plugin, operationsHelper = self.opsH )
      return S_OK( plugin_o )
    except AttributeError, e:
      self._logException( "Failed to create %s(): %s." % ( plugin, e ) )
      return S_ERROR()


  #############################################################################

  def getOutputData( self, paramDict, moduleLocation ):
    moduleFactory = ModuleFactory()

    moduleInstance = moduleFactory.getModule( moduleLocation, paramDict )
    if not moduleInstance['OK']:
      return moduleInstance
    module = moduleInstance['Value']
    return module.execute()

  def submitTransformationTasks( self, taskDict ):

    if 'BulkJobObject' in taskDict:
      return self.__submitTransformationTasksBulk( taskDict )
    else:
      return self.__submitTransformationTasks( taskDict )

  def __submitTransformationTasksBulk( self, taskDict ):
    """ Submit jobs in one go with one parametric job
    """
    startTime = time.time()
    transID = taskDict[taskDict.keys()[0]]['TransformationID']
    oJob = taskDict.pop( 'BulkJobObject' )
    if oJob is None:
      self._logError( 'submitTransformationTasksBulk: no bulk Job object found', transID = transID )
      return S_ERROR( ETSUKN, 'No bulk job object provided for submission' )

    result = self.submitTaskToExternal( oJob )
    if not result['OK']:
      return result

    jobIDList = result['Value']
    if len( jobIDList ) != len( taskDict ):
      return S_ERROR( ETSUKN, 'Submitted less number of jobs than requested tasks' )

    for ind, taskID in enumerate( sorted( taskDict ) ):
      taskDict[taskID]['ExternalID'] = jobIDList[ind]
      taskDict[taskID]['Success'] = True

    submitted = len( jobIDList )
    self._logInfo( 'submitTransformationTasksBulk: Submitted %d tasks to WMS in %.1f seconds' % ( submitted,
                                                                                                  time.time() - startTime ),
                   transID = transID )
    return S_OK( taskDict )

  def __submitTransformationTasks( self, taskDict ):
    """ Submit jobs one by one
    """
    submitted = 0
    failed = 0
    startTime = time.time()
    transID = None
    for taskID in sorted( taskDict ):
      transID = taskDict[taskID]['TransformationID']
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
        self._logError( "Failed to submit task to WMS", res['Message'], transID = transID )
        taskDict[taskID]['Success'] = False
        failed += 1
    self._logInfo( 'submitTransformationTasks: Submitted %d tasks to WMS in %.1f seconds' % ( submitted,
                                                                                              time.time() - startTime ),
                   transID = transID )
    if failed:
      self._logError( 'submitTransformationTasks: Failed to submit %d tasks to WMS.' % ( failed ), transID = transID )
    return S_OK( taskDict )

  def submitTaskToExternal( self, job ):
    """ Submits a single job to the WMS.
    """
    if isinstance( job, basestring ):
      try:
        oJob = self.jobClass( job )
      except Exception as x:
        self._logException( "Failed to create job object", '', x )
        return S_ERROR( "Failed to create job object" )
    elif isinstance( job, self.jobClass ):
      oJob = job
    else:
      self._logError( "No valid job description found" )
      return S_ERROR( "No valid job description found" )

    workflowFileObject = StringIO.StringIO( oJob._toXML() )
    jdl = oJob._toJDL( jobDescriptionObject = workflowFileObject )
    return self.submissionClient.submitJob( jdl, workflowFileObject )

  def updateTransformationReservedTasks( self, taskDicts ):
    requestNames = []
    transID = None
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      taskID = taskDict['TaskID']
      requestName = _requestName( transID, taskID )
      requestNames.append( requestName )
    res = self.jobMonitoringClient.getJobs( {'JobName':requestNames} )
    if not res['OK']:
      self._logError( "updateTransformationReservedTasks: Failed to get task from WMS", res['Message'], transID = transID )
      return res
    requestNameIDs = {}
    allAccounted = True
    for wmsID in res['Value']:
      res = self.jobMonitoringClient.getJobPrimarySummary( int( wmsID ) )
      if not res['OK']:
        self._logWarn( "updateTransformationReservedTasks: Failed to get task summary from WMS", res['Message'], transID = transID )
        allAccounted = False
        continue
      jobName = res['Value']['JobName']
      requestNameIDs[jobName] = int( wmsID )
    noTask = [requestName for requestName in requestNames if requestName not in requestNameIDs] if allAccounted else []
    return S_OK( {'NoTasks':noTask, 'TaskNameIDs':requestNameIDs} )

  def getSubmittedTaskStatus( self, taskDicts ):
    wmsIDs = []
    transID = None
    for taskDict in taskDicts:
      transID = taskDict['TransformationID']
      wmsID = int( taskDict['ExternalID'] )
      wmsIDs.append( wmsID )
    res = self.jobMonitoringClient.getJobsStatus( wmsIDs )
    if not res['OK']:
      self._logWarn( "Failed to get job status from the WMS system", transID = transID )
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
          self._logVerbose( 'Production/Job %d/%d removed from WMS while it is in %s status' % ( transID,
                                                                                                 taskID,
                                                                                                 oldStatus ), transID = transID )
          newStatus = "Failed"
        self._logVerbose( 'Setting job status for Production/Job %d/%d to %s' % ( transID, taskID, newStatus ), transID = transID )
        updateDict.setdefault( newStatus, [] ).append( taskID )
    return S_OK( updateDict )

  def getSubmittedFileStatus( self, fileDicts ):
    taskFiles = {}
    transID = None
    for fileDict in fileDicts:
      transID = fileDict['TransformationID']
      taskID = fileDict['TaskID']
      requestName = _requestName( transID, taskID )
      taskFiles.setdefault( requestName, {} )[fileDict['LFN']] = fileDict['Status']
    res = self.updateTransformationReservedTasks( fileDicts )
    if not res['OK']:
      self._logWarn( "Failed to obtain taskIDs for files", transID = transID )
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
      self._logWarn( "Failed to get job status from the WMS system", transID = transID )
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
