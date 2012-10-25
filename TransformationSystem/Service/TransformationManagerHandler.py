""" DISET request handler base class for the TransformationDB."""
__RCSID__ = "$Id$"

from DIRAC                                               import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                     import RequestHandler
from DIRAC.TransformationSystem.DB.TransformationDB      import TransformationDB
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from types import StringType, ListType, DictType, IntType, LongType, StringTypes, TupleType

transTypes = list( StringTypes ) + [IntType, LongType]

class TransformationManagerHandlerBase( RequestHandler ):

  def _parseRes( self, res ):
    if not res['OK']:
      gLogger.error( res['Message'] )
    return res

  def setDatabase( self, oDatabase ):
    global database
    database = oDatabase

  types_getName = []
  def export_getName( self ):
    res = database.getName()
    return self._parseRes( res )

  types_getCounters = [StringType, ListType, DictType]
  def export_getCounters( self, table, attrList, condDict, older = None, newer = None, timeStamp = None ):
    res = database.getCounters( table, attrList, condDict, older = older, newer = newer, timeStamp = timeStamp )
    return self._parseRes( res )

  ####################################################################
  #
  # These are the methods to manipulate the transformations table
  #

  types_addTransformation = [ StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_addTransformation( self, transName, description, longDescription, transType, plugin, agentType, fileMask,
                                    transformationGroup = 'General',
                                    groupSize = 1,
                                    inheritedFrom = 0,
                                    body = '',
                                    maxTasks = 0,
                                    eventsPerTask = 0,
                                    addFiles = True ):
#    authorDN = self._clientTransport.peerCredentials['DN']
#    authorGroup = self._clientTransport.peerCredentials['group']
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    authorGroup = credDict[ 'group' ]
    res = database.addTransformation( transName, description, longDescription, authorDN, authorGroup, transType, plugin,
                                      agentType, fileMask,
                                      transformationGroup = transformationGroup,
                                      groupSize = groupSize,
                                      inheritedFrom = inheritedFrom,
                                      body = body,
                                      maxTasks = maxTasks,
                                      eventsPerTask = eventsPerTask,
                                      addFiles = addFiles )
    if res['OK']:
      gLogger.info( "Added transformation %d" % res['Value'] )
    return self._parseRes( res )

  types_deleteTransformation = [transTypes]
  def export_deleteTransformation( self, transName ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    #authorDN = self._clientTransport.peerCredentials['DN']
    res = database.deleteTransformation( transName, author = authorDN )
    return self._parseRes( res )

  types_cleanTransformation = [transTypes]
  def export_cleanTransformation( self, transName ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    #authorDN = self._clientTransport.peerCredentials['DN']
    res = database.cleanTransformation( transName, author = authorDN )
    return self._parseRes( res )

  types_setTransformationParameter = [transTypes, StringTypes]
  def export_setTransformationParameter( self, transName, paramName, paramValue ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    #authorDN = self._clientTransport.peerCredentials['DN']
    res = database.setTransformationParameter( transName, paramName, paramValue, author = authorDN )
    return self._parseRes( res )

  types_deleteTransformationParameter = [transTypes, StringTypes]
  def export_deleteTransformationParameter( self, transName, paramName ):
    #credDict = self.getRemoteCredentials()
    #authorDN = credDict[ 'DN' ]
    #authorDN = self._clientTransport.peerCredentials['DN']
    res = database.deleteTransformationParameter( transName, paramName )
    return self._parseRes( res )

  types_getTransformations = []
  def export_getTransformations( self, condDict = {}, older = None, newer = None, timeStamp = 'CreationDate',
                                 orderAttribute = None, limit = None, extraParams = False, offset = None ):
    res = database.getTransformations( condDict = condDict,
                                  older = older,
                                  newer = newer,
                                  timeStamp = timeStamp,
                                  orderAttribute = orderAttribute,
                                  limit = limit,
                                  extraParams = extraParams,
                                  offset = offset )
    return self._parseRes( res )

  types_getTransformation = [transTypes]
  def export_getTransformation( self, transName, extraParams = False ):
    res = database.getTransformation( transName, extraParams = extraParams )
    return self._parseRes( res )

  types_getTransformationParameters = [transTypes, [ListType, TupleType]]
  def export_getTransformationParameters( self, transName, parameters ):
    res = database.getTransformationParameters( transName, parameters )
    return self._parseRes( res )

  types_getTransformationWithStatus = [list( StringTypes ) + [ListType, TupleType]]
  def export_getTransformationWithStatus( self, status ):
    res = database.getTransformationWithStatus( status )
    return self._parseRes( res )

  ####################################################################
  #
  # These are the methods to manipulate the TransformationFiles tables
  #

  types_addFilesToTransformation = [transTypes, [ListType, TupleType]]
  def export_addFilesToTransformation( self, transName, lfns ):
    res = database.addFilesToTransformation( transName, lfns )
    return self._parseRes( res )

  types_addTaskForTransformation = [transTypes]
  def export_addTaskForTransformation( self, transName, lfns = [], se = 'Unknown' ):
    res = database.addTaskForTransformation( transName, lfns = lfns, se = se )
    return self._parseRes( res )

  types_setFileStatusForTransformation = [transTypes, StringTypes, ListType]
  def export_setFileStatusForTransformation( self, transName, status, lfns, force = False ):
    res = database.setFileStatusForTransformation( transName, status, lfns, force )
    return self._parseRes( res )

  types_setFileUsedSEForTransformation = [transTypes, StringTypes, ListType]
  def export_setFileUsedSEForTransformation( self, transName, usedSE, lfns ):
    res = database.setFileUsedSEForTransformation( transName, usedSE, lfns )
    return self._parseRes( res )

  types_getTransformationStats = [transTypes]
  def export_getTransformationStats( self, transName ):
    res = database.getTransformationStats( transName )
    return self._parseRes( res )

  types_getTransformationFilesCount = [transTypes, StringTypes]
  def export_getTransformationFilesCount( self, transName, field, selection = {} ):
    res = database.getTransformationFilesCount( transName, field, selection = selection )
    return self._parseRes( res )

  types_getTransformationFiles = []
  def export_getTransformationFiles( self, condDict = {}, older = None, newer = None, timeStamp = 'LastUpdate',
                                     orderAttribute = None, limit = None, offset = None ):
    res = database.getTransformationFiles( condDict = condDict, older = older, newer = newer, timeStamp = timeStamp,
                                           orderAttribute = orderAttribute, limit = limit, offset = offset,
                                           connection = False )
    return self._parseRes( res )

  ####################################################################
  #
  # These are the methods to manipulate the TransformationTasks table
  #

  types_getTransformationTasks = []
  def export_getTransformationTasks( self, condDict = {}, older = None, newer = None, timeStamp = 'CreationTime',
                                     orderAttribute = None, limit = None, inputVector = False, offset = None ):
    res = database.getTransformationTasks( condDict = condDict, older = older, newer = newer, timeStamp = timeStamp,
                                           orderAttribute = orderAttribute, limit = limit, inputVector = inputVector,
                                           offset = offset )
    return self._parseRes( res )

  types_setTaskStatus = [transTypes, [ListType, IntType, LongType], StringTypes]
  def export_setTaskStatus( self, transName, taskID, status ):
    res = database.setTaskStatus( transName, taskID, status )
    return self._parseRes( res )

  types_setTaskStatusAndWmsID = [ transTypes, [LongType, IntType], StringType, StringType]
  def export_setTaskStatusAndWmsID( self, transName, taskID, status, taskWmsID ):
    res = database.setTaskStatusAndWmsID( transName, taskID, status, taskWmsID )
    return self._parseRes( res )

  types_getTransformationTaskStats = [transTypes]
  def export_getTransformationTaskStats( self, transName ):
    res = database.getTransformationTaskStats( transName )
    return self._parseRes( res )

  types_deleteTasks = [transTypes, [LongType, IntType], [LongType, IntType]]
  def export_deleteTasks( self, transName, taskMin, taskMax ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    #authorDN = self._clientTransport.peerCredentials['DN']
    res = database.deleteTasks( transName, taskMin, taskMax, author = authorDN )
    return self._parseRes( res )

  types_extendTransformation = [transTypes, [LongType, IntType]]
  def export_extendTransformation( self, transName, nTasks ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    #authorDN = self._clientTransport.peerCredentials['DN']
    res = database.extendTransformation( transName, nTasks, author = authorDN )
    return self._parseRes( res )

  types_getTasksToSubmit = [transTypes, [LongType, IntType]]
  def export_getTasksToSubmit( self, transName, numTasks, site = '' ):
    """ Get information necessary for submission for a given number of tasks for a given transformation """
    res = database.getTransformation( transName )
    if not res['OK']:
      return self._parseRes( res )
    transDict = res['Value']
    status = transDict['Status']
    submitDict = {}
    if status in ['Active', 'Completing', 'Flush']:
      res = database.getTasksForSubmission( transName, numTasks = numTasks, site = site, statusList = ['Created'] )
      if not res['OK']:
        return self._parseRes( res )
      tasksDict = res['Value']
      for taskID, taskDict in tasksDict.items():
        res = database.reserveTask( transName, long( taskID ) )
        if not res['OK']:
          return self._parseRes( res )
        else:
          submitDict[taskID] = taskDict
    transDict['JobDictionary'] = submitDict
    return S_OK( transDict )

  ####################################################################
  #
  # These are the methods for TransformationInputDataQuery table
  #

  types_createTransformationInputDataQuery = [ [LongType, IntType, StringType], DictType ]
  def export_createTransformationInputDataQuery( self, transName, queryDict ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    #authorDN = self._clientTransport.peerCredentials['DN']
    res = database.createTransformationInputDataQuery( transName, queryDict, author = authorDN )
    return self._parseRes( res )

  types_deleteTransformationInputDataQuery = [ [LongType, IntType, StringType] ]
  def export_deleteTransformationInputDataQuery( self, transName ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    #authorDN = self._clientTransport.peerCredentials['DN']
    res = database.deleteTransformationInputDataQuery( transName, author = authorDN )
    return self._parseRes( res )

  types_getTransformationInputDataQuery = [ [LongType, IntType, StringType] ]
  def export_getTransformationInputDataQuery( self, transName ):
    res = database.getTransformationInputDataQuery( transName )
    return self._parseRes( res )

  ####################################################################
  #
  # These are the methods for transformation logging manipulation
  #

  types_getTransformationLogging = [transTypes]
  def export_getTransformationLogging( self, transName ):
    res = database.getTransformationLogging( transName )
    return self._parseRes( res )

  ####################################################################
  #
  # These are the methods for transformation additional parameters
  #

  types_getAdditionalParameters = [transTypes]
  def export_getAdditionalParameters( self, transName ):
    res = database.getAdditionalParameters( transName )
    return self._parseRes( res )

  ####################################################################
  #
  # These are the methods for file manipulation
  #

  types_getFileSummary = [ListType, transTypes]
  def export_getFileSummary( self, lfns, transName ):
    res = database.getFileSummary( lfns, transName )
    return self._parseRes( res )

  types_addDirectory = [StringType]
  def export_addDirectory( self, path, force = False ):
    res = database.addDirectory( path, force = force )
    return self._parseRes( res )

  types_exists = [ListType]
  def export_exists( self, lfns ):
    res = database.exists( lfns )
    return self._parseRes( res )

  types_addFile = [ListType]
  def export_addFile( self, fileTuples, force = False ):
    res = database.addFile( fileTuples, force = force )
    return self._parseRes( res )

  types_removeFile = [ListType]
  def export_removeFile( self, lfns ):
    res = database.removeFile( lfns )
    return self._parseRes( res )

  ####################################################################
  #
  # These are the methods for replica manipulation
  #

  types_addReplica = [ListType]
  def export_addReplica( self, replicaTuples, force = False ):
    res = database.addReplica( replicaTuples, force = force )
    return self._parseRes( res )

  types_removeReplica = [ListType]
  def export_removeReplica( self, replicaTuples ):
    res = database.removeReplica( replicaTuples )
    return self._parseRes( res )

  types_getReplicas = [ListType]
  def export_getReplicas( self, lfns ):
    res = database.getReplicas( lfns )
    return self._parseRes( res )

  types_getReplicaStatus = [ListType]
  def export_getReplicaStatus( self, replicaTuples ):
    res = database.getReplicaStatus( replicaTuples )
    return self._parseRes( res )

  types_setReplicaStatus = [ListType]
  def export_setReplicaStatus( self, replicaTuples ):
    res = database.setReplicaStatus( replicaTuples )
    return self._parseRes( res )

  types_setReplicaHost = [ListType]
  def export_setReplicaHost( self, replicaTuples ):
    res = database.setReplicaHost( replicaTuples )
    return self._parseRes( res )

  ####################################################################
  #
  # These are the methods used for web monitoring
  #

  #TODO Get rid of this (talk to Matvey)
  types_getDistinctAttributeValues = [StringTypes, DictType]
  def export_getDistinctAttributeValues( self, attribute, selectDict ):
    res = database.getTableDistinctAttributeValues( 'Transformations', [attribute], selectDict )
    if not res['OK']:
      return self._parseRes( res )
    return S_OK( res['Value'][attribute] )

  types_getTableDistinctAttributeValues = [StringTypes, ListType, DictType]
  def export_getTableDistinctAttributeValues( self, table, attributes, selectDict ):
    res = database.getTableDistinctAttributeValues( table, attributes, selectDict )
    return self._parseRes( res )

  types_getTransformationStatusCounters = []
  def export_getTransformationStatusCounters( self ):
    res = database.getCounters( 'Transformations', ['Status'], {} )
    if not res['OK']:
      return self._parseRes( res )
    statDict = {}
    for attrDict, count in res['Value']:
      statDict[attrDict['Status']] = count
    return S_OK( statDict )

  types_getTransformationSummary = []
  def export_getTransformationSummary( self ):
    """ Get the summary of the currently existing transformations """
    res = database.getTransformations()
    if not res['OK']:
      return self._parseRes( res )
    transList = res['Value']
    resultDict = {}
    for transDict in transList:
      transID = transDict['TransformationID']
      res = database.getTransformationTaskStats( transID )
      if not res['OK']:
        gLogger.warn( 'Failed to get job statistics for transformation %d' % transID )
        continue
      transDict['JobStats'] = res['Value']
      res = database.getTransformationStats( transID )
      if not res['OK']:
        transDict['NumberOfFiles'] = -1
      else:
        transDict['NumberOfFiles'] = res['Value']['Total']
      resultDict[transID] = transDict
    return S_OK( resultDict )

  types_getTabbedSummaryWeb = [StringTypes, DictType, DictType, ListType, IntType, IntType]
  def export_getTabbedSummaryWeb( self, table, requestedTables, selectDict, sortList, startItem, maxItems ):
    tableDestinations = {  'Transformations'      : { 'TransformationFiles' : ['TransformationID'],
                                                      'TransformationTasks' : ['TransformationID']           },

                           'TransformationFiles'  : { 'Transformations'     : ['TransformationID'],
                                                      'TransformationTasks' : ['TransformationID', 'TaskID']  },

                           'TransformationTasks'  : { 'Transformations'     : ['TransformationID'],
                                                      'TransformationFiles' : ['TransformationID', 'TaskID']  } }

    tableSelections = {    'Transformations'      : ['TransformationID', 'AgentType', 'Type', 'TransformationGroup',
                                                     'Plugin'],
                           'TransformationFiles'  : ['TransformationID', 'TaskID', 'Status', 'UsedSE', 'TargetSE'],
                           'TransformationTasks'  : ['TransformationID', 'TaskID', 'ExternalStatus', 'TargetSE'] }

    tableTimeStamps = {    'Transformations'      : 'CreationDate',
                           'TransformationFiles'  : 'LastUpdate',
                           'TransformationTasks'  : 'CreationTime' }

    tableStatusColumn = {   'Transformations'      : 'Status',
                           'TransformationFiles'  : 'Status',
                           'TransformationTasks'  : 'ExternalStatus' }

    resDict = {}
    res = self.__getTableSummaryWeb( table, selectDict, sortList, startItem, maxItems,
                                     selectColumns = tableSelections[table], timeStamp = tableTimeStamps[table],
                                     statusColumn = tableStatusColumn[table] )
    if not res['OK']:
      gLogger.error( "Failed to get Summary for table", "%s %s" % ( table, res['Message'] ) )
      return self._parseRes( res )
    resDict[table] = res['Value']
    selections = res['Value']['Selections']
    tableSelection = {}
    for destination in tableDestinations[table].keys():
      tableSelection[destination] = {}
      for parameter in tableDestinations[table][destination]:
        tableSelection[destination][parameter] = selections.get( parameter, [] )

    for table, paramDict in requestedTables.items():
      sortList = paramDict.get( 'SortList', [] )
      startItem = paramDict.get( 'StartItem', 0 )
      maxItems = paramDict.get( 'MaxItems', 50 )
      res = self.__getTableSummaryWeb( table, tableSelection[table], sortList, startItem, maxItems,
                                       selectColumns = tableSelections[table], timeStamp = tableTimeStamps[table],
                                       statusColumn = tableStatusColumn[table] )
      if not res['OK']:
        gLogger.error( "Failed to get Summary for table", "%s %s" % ( table, res['Message'] ) )
        return self._parseRes( res )
      resDict[table] = res['Value']
    return S_OK( resDict )

  types_getTransformationsSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationsSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    return self.__getTableSummaryWeb( 'Transformations', selectDict, sortList, startItem, maxItems,
                                      selectColumns = ['TransformationID', 'AgentType', 'Type', 'Group', 'Plugin'],
                                      timeStamp = 'CreationDate', statusColumn = 'Status' )

  types_getTransformationTasksSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationTasksSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    return self.__getTableSummaryWeb( 'TransformationTasks', selectDict, sortList, startItem, maxItems,
                                      selectColumns = ['TransformationID', 'ExternalStatus', 'TargetSE'],
                                      timeStamp = 'CreationTime', statusColumn = 'ExternalStatus' )

  types_getTransformationFilesSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationFilesSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    return self.__getTableSummaryWeb( 'TransformationFiles', selectDict, sortList, startItem, maxItems,
                                      selectColumns = ['TransformationID', 'Status', 'UsedSE', 'TargetSE'],
                                      timeStamp = 'LastUpdate', statusColumn = 'Status' )

  def __getTableSummaryWeb( self, table, selectDict, sortList, startItem, maxItems, selectColumns = [],
                            timeStamp = None, statusColumn = 'Status' ):
    fromDate = selectDict.get( 'FromDate', None )
    if fromDate:
      del selectDict['FromDate']
    #if not fromDate:
    #  fromDate = last_update
    toDate = selectDict.get( 'ToDate', None )
    if toDate:
      del selectDict['ToDate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None
    # Get the columns that match the selection
    fcn = None
    fcnName = "get%s" % table
    if hasattr( database, fcnName ) and callable( getattr( database, fcnName ) ):
      fcn = getattr( database, fcnName )
    if not fcn:
      return S_ERROR( "Unable to invoke database.%s, it isn't a member function of database" % fcnName )
    res = fcn( condDict = selectDict, older = toDate, newer = fromDate, timeStamp = timeStamp,
               orderAttribute = orderAttribute )
    if not res['OK']:
      return self._parseRes( res )

    # The full list of columns in contained here
    allRows = res['Records']
    # Prepare the standard structure now within the resultDict dictionary
    resultDict = {}
    # Create the total records entry
    resultDict['TotalRecords'] = len( allRows )
    # Create the ParameterNames entry
    resultDict['ParameterNames'] = res['ParameterNames']
    # Find which element in the tuple contains the requested status
    if not statusColumn in resultDict['ParameterNames']:
      return S_ERROR( "Provided status column not present" )
    statusColumnIndex = resultDict['ParameterNames'].index( statusColumn )

    # Get the rows which are within the selected window
    if resultDict['TotalRecords'] == 0:
      return S_OK( resultDict )
    ini = startItem
    last = ini + maxItems
    if ini >= resultDict['TotalRecords']:
      return S_ERROR( 'Item number out of range' )
    if last > resultDict['TotalRecords']:
      last = resultDict['TotalRecords']
    selectedRows = allRows[ini:last]
    resultDict['Records'] = selectedRows

    # Generate the status dictionary
    statusDict = {}
    for row in selectedRows:
      status = row[statusColumnIndex]
      statusDict[status] = statusDict.setdefault( status, 0 ) + 1
    resultDict['Extras'] = statusDict

    # Obtain the distinct values of the selection parameters
    res = database.getTableDistinctAttributeValues( table, selectColumns, selectDict, older = toDate, newer = fromDate )
    distinctSelections = zip( selectColumns, [] )
    if res['OK']:
      distinctSelections = res['Value']
    resultDict['Selections'] = distinctSelections

    return S_OK( resultDict )

  types_getTransformationSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ Get the summary of the transformation information for a given page in the generic format """

    # Obtain the timing information from the selectDict
    last_update = selectDict.get( 'CreationDate', None )
    if last_update:
      del selectDict['CreationDate']
    fromDate = selectDict.get( 'FromDate', None )
    if fromDate:
      del selectDict['FromDate']
    if not fromDate:
      fromDate = last_update
    toDate = selectDict.get( 'ToDate', None )
    if toDate:
      del selectDict['ToDate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    # Get the transformations that match the selection
    res = database.getTransformations( condDict = selectDict, older = toDate, newer = fromDate,
                                       orderAttribute = orderAttribute )
    if not res['OK']:
      return self._parseRes( res )

    # Prepare the standard structure now within the resultDict dictionary
    resultDict = {}
    trList = res['Records']
    # Create the total records entry
    nTrans = len( trList )
    resultDict['TotalRecords'] = nTrans
    # Create the ParameterNames entry
    paramNames = res['ParameterNames']
    resultDict['ParameterNames'] = paramNames
    # Add the job states to the ParameterNames entry
    taskStateNames = ['Created', 'Running', 'Submitted', 'Failed', 'Waiting', 'Done', 'Completed', 'Stalled',
                      'Killed', 'Staging', 'Checking', 'Rescheduled']
    resultDict['ParameterNames'] += ['Jobs_' + x for x in taskStateNames]
    # Add the file states to the ParameterNames entry
    fileStateNames = ['PercentProcessed', 'Processed', 'Unused', 'Assigned', 'Total', 'Problematic',
                      'ApplicationCrash', 'MaxReset']
    resultDict['ParameterNames'] += ['Files_' + x for x in fileStateNames]

    # Get the transformations which are within the selected window
    if nTrans == 0:
      return S_OK( resultDict )
    ini = startItem
    last = ini + maxItems
    if ini >= nTrans:
      return S_ERROR( 'Item number out of range' )
    if last > nTrans:
      last = nTrans
    transList = trList[ini:last]

    statusDict = {}
    # Add specific information for each selected transformation
    for trans in transList:
      transDict = dict( zip( paramNames, trans ) )

      # Update the status counters
      status = transDict['Status']
      statusDict[status] = statusDict.setdefault( status, 0 ) + 1

      # Get the statistics on the number of jobs for the transformation
      transID = transDict['TransformationID']
      res = database.getTransformationTaskStats( transID )
      taskDict = {}
      if res['OK'] and res['Value']:
        taskDict = res['Value']
      for state in taskStateNames:
        trans.append( taskDict.get( state, 0 ) )

      # Get the statistics for the number of files for the transformation
      fileDict = {}
      transType = transDict['Type']
      extendableTranfs = Operations().getValue( "Production/%s/ExtendableTransfTypes" % database.__class__.__name__,
                                                'mcsimulation' )
      if transType.lower() in extendableTranfs:
        fileDict['PercentProcessed'] = '-'
      else:
        res = database.getTransformationStats( transID )
        if res['OK']:
          fileDict = res['Value']
          if fileDict['Total'] == 0:
            fileDict['PercentProcessed'] = 0
          else:
            processed = fileDict.get( 'Processed', 0 )
            fileDict['PercentProcessed'] = "%.1f" % ( int( processed * 1000. / fileDict['Total'] ) / 10. )
      for state in fileStateNames:
        trans.append( fileDict.get( state, 0 ) )

    resultDict['Records'] = transList
    resultDict['Extras'] = statusDict
    return S_OK( resultDict )

  ###########################################################################

database = False
def initializeTransformationManagerHandler( serviceInfo ):
  global database
  database = TransformationDB( 'TransformationDB', 'Transformation/TransformationDB' )
  return S_OK()

class TransformationManagerHandler( TransformationManagerHandlerBase ):

  def __init__( self, *args, **kargs ):
    self.setDatabase( database )
    TransformationManagerHandlerBase.__init__( self, *args, **kargs )
