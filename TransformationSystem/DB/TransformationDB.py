""" DIRAC Transformation DB

    Transformation database is used to collect and serve the necessary information
    in order to automate the task of job preparation for high level transformations.
    This class is typically used as a base class for more specific data processing
    databases
"""

import re
import threading
import json

from DIRAC                                                import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                   import DB
from DIRAC.Resources.Catalog.FileCatalog                  import FileCatalog
from DIRAC.Core.Security.ProxyInfo                        import getProxyInfo
from DIRAC.Core.Utilities.List                            import stringListToString, intListToString, breakListIntoChunks
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations

__RCSID__ = "$Id$"

MAX_ERROR_COUNT = 10

#############################################################################

class TransformationDB( DB ):
  """ TransformationDB class
  """

  def __init__( self, dbname = None, dbconfig = None, dbIn = None ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """

    if not dbname:
      dbname = 'TransformationDB'
    if not dbconfig:
      dbconfig = 'Transformation/TransformationDB'

    if not dbIn:
      DB.__init__( self, dbname, dbconfig )

    self.lock = threading.Lock()
    self.filters = ()
    res = self.__updateFilters()
    if not res['OK']:
      gLogger.fatal( "Failed to create filters" )

    self.allowedStatusForTasks = ( 'Unused', 'ProbInFC' )


    self.TRANSPARAMS = [  'TransformationID',
                          'TransformationName',
                          'Description',
                          'LongDescription',
                          'CreationDate',
                          'LastUpdate',
                          'AuthorDN',
                          'AuthorGroup',
                          'Type',
                          'Plugin',
                          'AgentType',
                          'Status',
                          'FileMask',
                          'TransformationGroup',
                          'GroupSize',
                          'InheritedFrom',
                          'Body',
                          'MaxNumberOfTasks',
                          'EventsPerTask',
                          'TransformationFamily']

    self.mutable = [      'TransformationName',
                          'Description',
                          'LongDescription',
                          'AgentType',
                          'Status',
                          'MaxNumberOfTasks',
                          'TransformationFamily',
                          'Body']  # for the moment include TransformationFamily

    self.TRANSFILEPARAMS = ['TransformationID',
                            'FileID',
                            'Status',
                            'TaskID',
                            'TargetSE',
                            'UsedSE',
                            'ErrorCount',
                            'LastUpdate',
                            'InsertedTime']

    self.TRANSFILETASKPARAMS = ['TransformationID',
                                'FileID',
                                'TaskID']

    self.TASKSPARAMS = [  'TaskID',
                          'TransformationID',
                          'ExternalStatus',
                          'ExternalID',
                          'TargetSE',
                          'CreationTime',
                          'LastUpdateTime']

    self.ADDITIONALPARAMETERS = ['TransformationID',
                                 'ParameterName',
                                 'ParameterValue',
                                 'ParameterType'
                                ]


    # This is here to ensure full compatibility between different versions of the MySQL DB schema
    self.isTransformationTasksInnoDB = True
    res = self._query( "SELECT Engine FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'TransformationTasks'" )
    if not res['OK']:
      raise RuntimeError( res['Message'] )
    else:
      engine = res['Value'][0][0]
      if engine.lower() != 'innodb':
        self.isTransformationTasksInnoDB = False

  def getName( self ):
    """  Get the database name
    """
    return self.dbName

  ###########################################################################
  #
  # These methods manipulate the Transformations table
  #

  def addTransformation( self, transName, description, longDescription, authorDN, authorGroup, transType,
                         plugin, agentType, fileMask,
                         transformationGroup = 'General',
                         groupSize = 1,
                         inheritedFrom = 0,
                         body = '',
                         maxTasks = 0,
                         eventsPerTask = 0,
                         addFiles = True,
                         connection = False ):
    """ Add new transformation definition including its input streams
    """
    connection = self.__getConnection( connection )
    res = self._getTransformationID( transName, connection = connection )
    if res['OK']:
      return S_ERROR( "Transformation with name %s already exists with TransformationID = %d" % ( transName,
                                                                                                  res['Value'] ) )
    elif res['Message'] != "Transformation does not exist":
      return res
    self.lock.acquire()
    res = self._escapeString( body )
    if not res['OK']:
      return S_ERROR( "Failed to parse the transformation body" )
    body = res['Value']
    req = "INSERT INTO Transformations (TransformationName,Description,LongDescription, \
                                        CreationDate,LastUpdate,AuthorDN,AuthorGroup,Type,Plugin,AgentType,\
                                        FileMask,Status,TransformationGroup,GroupSize,\
                                        InheritedFrom,Body,MaxNumberOfTasks,EventsPerTask)\
                                VALUES ('%s','%s','%s',\
                                        UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s','%s','%s','%s','%s',\
                                        '%s','New','%s',%d,\
                                        %d,%s,%d,%d);" % \
                                      ( transName, description, longDescription,
                                        authorDN, authorGroup, transType, plugin, agentType,
                                        fileMask, transformationGroup, groupSize,
                                        inheritedFrom, body, maxTasks, eventsPerTask )
    res = self._update( req, connection )
    if not res['OK']:
      self.lock.release()
      return res
    transID = res['lastRowId']
    self.lock.release()
    # If the transformation has an input data specification
    if fileMask:
      self.filters.append( ( transID, json.loads( fileMask ) ) )

    if inheritedFrom:
      res = self._getTransformationID( inheritedFrom, connection = connection )
      if not res['OK']:
        gLogger.error( "Failed to get ID for parent transformation, now deleting", res['Message'] )
        return self.deleteTransformation( transID, connection = connection )
      originalID = res['Value']
      # FIXME: this is not the right place to change status information, and in general the whole should not be here
      res = self.setTransformationParameter( originalID, 'Status', 'Completing',
                                             author = authorDN, connection = connection )
      if not res['OK']:
        gLogger.error( "Failed to update parent transformation status: now deleting", res['Message'] )
        return self.deleteTransformation( transID, connection = connection )
      res = self.setTransformationParameter( originalID, 'AgentType', 'Automatic',
                                             author = authorDN, connection = connection )
      if not res['OK']:
        gLogger.error( "Failed to update parent transformation agent type, now deleting", res['Message'] )
        return self.deleteTransformation( transID, connection = connection )
      message = 'Creation of the derived transformation (%d)' % transID
      self.__updateTransformationLogging( originalID, message, authorDN, connection = connection )
      res = self.getTransformationFiles( condDict = {'TransformationID':originalID}, connection = connection )
      if not res['OK']:
        gLogger.error( "Could not get transformation files, now deleting", res['Message'] )
        return self.deleteTransformation( transID, connection = connection )
      if res['Records']:
        res = self.__insertExistingTransformationFiles( transID, res['Records'], connection = connection )
        if not res['OK']:
          gLogger.error( "Could not insert files, now deleting", res['Message'] )
          return self.deleteTransformation( transID, connection = connection )

    ### Add files to the DataFiles table ##################
    self.fc = FileCatalog()
    if addFiles and fileMask:
      mqDict = json.loads( fileMask )
      res = self.fc.findFilesByMetadata( mqDict )
      filesToAdd = res['Value']
      gLogger.notice( 'filesToAdd', filesToAdd )
      if filesToAdd:
        connection = self.__getConnection( connection )
        res = self.__addDataFiles( filesToAdd, connection = connection )
        if not res['OK']:
          return res
        lfnFileIDs = res['Value']
         # Add the files to the transformations
        fileIDs = []
        for lfn in filesToAdd:
          if lfnFileIDs.has_key( lfn ):
            fileIDs.append( lfnFileIDs[lfn] )
        res = self.__addFilesToTransformation( transID, fileIDs, connection = connection )
        if not res['OK']:
          gLogger.error( "Failed to add files to transformation", "%s %s" % ( transID, res['Message'] ) )

    message = "Created transformation %d" % transID
  ##############################
    self.__updateTransformationLogging( transID, message, authorDN, connection = connection )
    return S_OK( transID )

  def getTransformations( self, condDict = None, older = None, newer = None, timeStamp = 'LastUpdate',
                          orderAttribute = None, limit = None, extraParams = False, offset = None, connection = False ):
    """ Get parameters of all the Transformations with support for the web standard structure """
    connection = self.__getConnection( connection )
    req = "SELECT %s FROM Transformations %s" % ( intListToString( self.TRANSPARAMS ),
                                                  self.buildCondition( condDict, older, newer, timeStamp,
                                                                       orderAttribute, limit, offset = offset ) )
    res = self._query( req, connection )
    if not res['OK']:
      return res
    if condDict is None:
      condDict = {}
    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = [str( item ) if not isinstance( item, ( long, int ) ) else item for item in row]
      transDict = dict( zip( self.TRANSPARAMS, row ) )
      webList.append( rList )
      if extraParams:
        res = self.__getAdditionalParameters( transDict['TransformationID'], connection = connection )
        if not res['OK']:
          return res
        transDict.update( res['Value'] )
      resultList.append( transDict )
    result = S_OK( resultList )
    result['Records'] = webList
    result['ParameterNames'] = self.TRANSPARAMS
    return result

  def getTransformation( self, transName, extraParams = False, connection = False ):
    """Get Transformation definition and parameters of Transformation identified by TransformationID
    """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.getTransformations( condDict = {'TransformationID':transID}, extraParams = extraParams,
                                   connection = connection )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR( "Transformation %s did not exist" % transName )
    return S_OK( res['Value'][0] )

  def getTransformationParameters( self, transName, parameters, connection = False ):
    """ Get the requested parameters for a supplied transformation """
    if isinstance( parameters, basestring ):
      parameters = [parameters]
    extraParams = bool( set( parameters ) - set( self.TRANSPARAMS ) )
    res = self.getTransformation( transName, extraParams = extraParams, connection = connection )
    if not res['OK']:
      return res
    transParams = res['Value']
    paramDict = {}
    for reqParam in parameters:
      if reqParam not in transParams:
        return S_ERROR( "Parameter %s not defined for transformation" % reqParam )
      paramDict[reqParam] = transParams[reqParam]
    if len( paramDict ) == 1:
      return S_OK( paramDict[reqParam] )
    return S_OK( paramDict )

  def getTransformationWithStatus( self, status, connection = False ):
    """ Gets a list of the transformations with the supplied status """
    req = "SELECT TransformationID FROM Transformations WHERE Status = '%s';" % status
    res = self._query( req, conn = connection )
    if not res['OK']:
      return res
    transIDs = [tupleIn[0] for tupleIn in res['Value']]
    return S_OK( transIDs )

  def getTableDistinctAttributeValues( self, table, attributes, selectDict, older = None, newer = None,
                                       timeStamp = None, connection = False ):
    tableFields = { 'Transformations'      : self.TRANSPARAMS,
                    'TransformationTasks'  : self.TASKSPARAMS,
                    'TransformationFiles'  : self.TRANSFILEPARAMS}
    possibleFields = tableFields.get( table, [] )
    return self.__getTableDistinctAttributeValues( table, possibleFields, attributes, selectDict, older, newer,
                                                   timeStamp, connection = connection )

  def __getTableDistinctAttributeValues( self, table, possible, attributes, selectDict, older, newer,
                                         timeStamp, connection = False ):
    connection = self.__getConnection( connection )
    attributeValues = {}
    for attribute in attributes:
      if possible and ( not attribute in  possible ):
        return S_ERROR( 'Requested attribute (%s) does not exist in table %s' % ( attribute, table ) )
      res = self.getDistinctAttributeValues( table, attribute, condDict = selectDict, older = older, newer = newer,
                                             timeStamp = timeStamp, connection = connection )
      if not res['OK']:
        return S_ERROR( 'Failed to serve values for attribute %s in table %s' % ( attribute, table ) )
      attributeValues[attribute] = res['Value']
    return S_OK( attributeValues )

  def __updateTransformationParameter( self, transID, paramName, paramValue, connection = False ):
    if paramName not in self.mutable:
      return S_ERROR( "Can not update the '%s' transformation parameter" % paramName )
    if paramName == 'Body':
      res = self._escapeString( paramValue )
      if not res['OK']:
        return S_ERROR( "Failed to parse parameter value" )
      paramValue = res['Value']
      req = "UPDATE Transformations SET %s=%s, LastUpdate=UTC_TIMESTAMP() WHERE TransformationID=%d" % ( paramName,
                                                                                                         paramValue,
                                                                                                         transID )
      return self._update( req, connection )
    req = "UPDATE Transformations SET %s='%s', LastUpdate=UTC_TIMESTAMP() WHERE TransformationID=%d" % ( paramName,
                                                                                                         paramValue,
                                                                                                         transID )
    return self._update( req, connection )

  def _getTransformationID( self, transName, connection = False ):
    """ Method returns ID of transformation with the name=<name> """
    try:
      transName = long( transName )
      cmd = "SELECT TransformationID from Transformations WHERE TransformationID=%d;" % transName
    except:
      if not isinstance( transName, basestring ):
        return S_ERROR( "Transformation should be ID or name" )
      cmd = "SELECT TransformationID from Transformations WHERE TransformationName='%s';" % transName
    res = self._query( cmd, connection )
    if not res['OK']:
      gLogger.error( "Failed to obtain transformation ID for transformation", "%s: %s" % ( transName, res['Message'] ) )
      return res
    elif not res['Value']:
      gLogger.verbose( "Transformation %s does not exist" % ( transName ) )
      return S_ERROR( "Transformation does not exist" )
    return S_OK( res['Value'][0][0] )

  def __deleteTransformation( self, transID, connection = False ):
    req = "DELETE FROM Transformations WHERE TransformationID=%d;" % transID
    return self._update( req, connection )

  def __updateFilters( self, connection = False ):
    """ Get filters for all defined input streams in all the transformations.
        If transID argument is given, get filters only for this transformation.
    """
    resultList = []
    req = "SELECT TransformationID,FileMask FROM Transformations;"
    res = self._query( req, connection )
    if not res['OK']:
      return res
    for transID, mask in res['Value']:
      if mask:
        resultList.append( ( transID, json.loads( mask ) ) )
    self.filters = resultList
    return S_OK( resultList )

  def __filterFile( self, lfn, filters = None ):
    """Pass the input file through a supplied filter or those currently active """
    result = []
    if filters:
      for transID, refilter in filters:
        if refilter.search( lfn ):
          result.append( transID )
    else:
      for transID, refilter in self.filters:
        if refilter.search( lfn ):
          result.append( transID )
    return result

  ###########################################################################
  #
  # These methods manipulate the AdditionalParameters tables
  #
  def setTransformationParameter( self, transName, paramName, paramValue, author = '', connection = False ):
    """ Add a parameter for the supplied transformations """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    message = ''
    if paramName in self.TRANSPARAMS:
      res = self.__updateTransformationParameter( transID, paramName, paramValue, connection = connection )
      if res['OK']:
        pv = self._escapeString( paramValue )
        if not pv['OK']:
          return S_ERROR( "Failed to parse parameter value" )
        paramValue = pv['Value']
        message = '%s updated to %s' % ( paramName, paramValue )
    else:
      res = self.__addAdditionalTransformationParameter( transID, paramName, paramValue, connection = connection )
      if res['OK']:
        message = 'Added additional parameter %s' % paramName
    if message:
      self.__updateTransformationLogging( transID, message, author, connection = connection )
    return res

  def getAdditionalParameters( self, transName, connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    return self.__getAdditionalParameters( transID, connection = connection )

  def deleteTransformationParameter( self, transName, paramName, author = '', connection = False ):
    """ Delete a parameter from the additional parameters table """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if paramName in self.TRANSPARAMS:
      return S_ERROR( "Can not delete core transformation parameter" )
    res = self.__deleteTransformationParameters( transID, parameters = [paramName], connection = connection )
    if not res['OK']:
      return res
    self.__updateTransformationLogging( transID, 'Removed additional parameter %s' % paramName, author,
                                        connection = connection )
    return res

  def __addAdditionalTransformationParameter( self, transID, paramName, paramValue, connection = False ):
    req = "DELETE FROM AdditionalParameters WHERE TransformationID=%d AND ParameterName='%s'" % ( transID, paramName )
    res = self._update( req, connection )
    if not res['OK']:
      return res
    res = self._escapeString( paramValue )
    if not res['OK']:
      return S_ERROR( "Failed to parse parameter value" )
    paramValue = res['Value']
    paramType = 'StringType'
    if isinstance( paramValue, ( long, int ) ):
      paramType = 'IntType'
    req = "INSERT INTO AdditionalParameters (%s) VALUES (%s,'%s',%s,'%s');" % ( ', '.join( self.ADDITIONALPARAMETERS ),
                                                                                transID, paramName,
                                                                                paramValue, paramType )
    return self._update( req, connection )

  def __getAdditionalParameters( self, transID, connection = False ):
    req = "SELECT %s FROM AdditionalParameters WHERE TransformationID = %d" % ( ', '.join( self.ADDITIONALPARAMETERS ),
                                                                               transID )
    res = self._query( req, connection )
    if not res['OK']:
      return res
    paramDict = {}
    for _transID, parameterName, parameterValue, parameterType in res['Value']:
      if parameterType in ( 'IntType', 'LongType' ):
        parameterValue = int( parameterValue )
      paramDict[parameterName] = parameterValue
    return S_OK( paramDict )

  def __deleteTransformationParameters( self, transID, parameters = None, connection = False ):
    """ Remove the parameters associated to a transformation """
    if parameters is None:
      parameters = []
    req = "DELETE FROM AdditionalParameters WHERE TransformationID=%d" % transID
    if parameters:
      req = "%s AND ParameterName IN (%s);" % ( req, stringListToString( parameters ) )
    return self._update( req, connection )

  ###########################################################################
  #
  # These methods manipulate the TransformationFiles table
  #

  def addFilesToTransformation( self, transName, lfns, connection = False ):
    """ Add a list of LFNs to the transformation directly """
    gLogger.info( "TransformationDB.addFilesToTransformation: Attempting to add %s files." % lfns )
    gLogger.info( "TransformationDB.addFilesToTransformation: to Transformations: %s" % transName )
    if not lfns:
      return S_ERROR( 'Zero length LFN list' )
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getFileIDsForLfns( lfns, connection = connection )
    if not res['OK']:
      return res
    fileIDs, _lfnFilesIDs = res['Value']
    failed = {}
    successful = {}
    missing = []
    fileIDsValues = set( fileIDs.values() )
    for lfn in lfns:
      if lfn not in fileIDsValues:
        missing.append( lfn )
    if missing:
      res = self.__addDataFiles( missing, connection = connection )
      if not res['OK']:
        return res
      for lfn, fileID in res['Value'].items():
        fileIDs[fileID] = lfn
    # must update the fileIDs
    if fileIDs:
      res = self.__addFilesToTransformation( transID, fileIDs.keys(), connection = connection )
      if not res['OK']:
        return res
      for fileID in fileIDs:
        lfn = fileIDs[fileID]
        successful[lfn] = "Added" if fileID in res['Value'] else "Present"
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def getTransformationFiles( self, condDict = None, older = None, newer = None, timeStamp = 'LastUpdate',
                              orderAttribute = None, limit = None, offset = None, connection = False ):
    """ Get files for the supplied transformations with support for the web standard structure """
    connection = self.__getConnection( connection )
    req = "SELECT %s FROM TransformationFiles" % ( intListToString( self.TRANSFILEPARAMS ) )
    originalFileIDs = {}
    if condDict is None:
      condDict = {}
    if condDict or older or newer:
      lfns = condDict.pop( 'LFN', None )
      if lfns:
        if isinstance( lfns, basestring ):
          lfns = [lfns]
        res = self.__getFileIDsForLfns( lfns, connection = connection )
        if not res['OK']:
          return res
        originalFileIDs, _ignore = res['Value']
        condDict['FileID'] = originalFileIDs.keys()

      for val in condDict.itervalues():
        if not val:
          return S_OK( [] )

      req = "%s %s" % ( req, self.buildCondition( condDict, older, newer, timeStamp, orderAttribute, limit,
                                                  offset = offset ) )
    res = self._query( req, connection )
    if not res['OK']:
      return res

    transFiles = res['Value']
    fileIDs = [int( row[1] ) for row in transFiles]
    webList = []
    resultList = []
    if not fileIDs:
      originalFileIDs = {}
    else:
      if not originalFileIDs:
        res = self.__getLfnsForFileIDs( fileIDs, connection = connection )
        if not res['OK']:
          return res
        originalFileIDs = res['Value'][1]
      for row in transFiles:
        lfn = originalFileIDs[row[1]]
        # Prepare the structure for the web
        fDict = {'LFN': lfn}
        fDict.update( dict( zip( self.TRANSFILEPARAMS, row ) ) )
        # Note: the line below is returning "None" if the item is None... This seems to work but is ugly...
        rList = [lfn] + [str( item ) if not isinstance( item, ( long, int ) ) else item for item in row]
        webList.append( rList )
        resultList.append( fDict )
    result = S_OK( resultList )
    result['Records'] = webList
    result['ParameterNames'] = ['LFN'] + self.TRANSFILEPARAMS
    return result

  def getFileSummary( self, lfns, connection = False ):
    """ Get file status summary in all the transformations """
    connection = self.__getConnection( connection )
    condDict = {'LFN':lfns}
    res = self.getTransformationFiles( condDict = condDict, connection = connection )
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      resDict.setdefault( fileDict['LFN'], {} )[fileDict['TransformationID']] = fileDict
    failedDict = dict.fromkeys( set( lfns ) - set( resDict ), 'Did not exist in the Transformation database' )
    return S_OK( {'Successful':resDict, 'Failed':failedDict} )

  def setFileStatusForTransformation( self, transID, fileStatusDict = None, connection = False ):
    """ Set file status for the given transformation, based on
        fileStatusDict {fileID_A: 'statusA', fileID_B: 'statusB', ...}

        The ErrorCount is incremented automatically here
    """
    if not fileStatusDict:
      return S_OK()

    # Building the request with "ON DUPLICATE KEY UPDATE"
    req = "INSERT INTO TransformationFiles (TransformationID, FileID, Status, ErrorCount, LastUpdate) VALUES "

    updatesList = ["(%d, %d, '%s', 0, UTC_TIMESTAMP())" % ( transID, fileID, status ) for fileID, status in fileStatusDict.items()]
    req += ','.join( updatesList )
    req += " ON DUPLICATE KEY UPDATE Status=VALUES(Status),ErrorCount=ErrorCount+1,LastUpdate=VALUES(LastUpdate)"

    return self._update( req, connection )


  def getTransformationStats( self, transName, connection = False ):
    """ Get number of files in Transformation Table for each status """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.getCounters( 'TransformationFiles', ['TransformationID', 'Status'], {'TransformationID':transID} )
    if not res['OK']:
      return res
    statusDict = dict( [( attrDict['Status'], count ) for attrDict, count in res['Value'] if '-' not in attrDict['Status']] )
    statusDict['Total'] = sum( statusDict.values() )
    return S_OK( statusDict )

  def getTransformationFilesCount( self, transName, field, selection = None, connection = False ):
    """ Get the number of files in the TransformationFiles table grouped by the supplied field """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    if selection is None:
      selection = {}
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    selection['TransformationID'] = transID
    if field not in self.TRANSFILEPARAMS:
      return S_ERROR( "Supplied field not in TransformationFiles table" )
    res = self.getCounters( 'TransformationFiles', ['TransformationID', field], selection )
    if not res['OK']:
      return res
    countDict = dict( [( attrDict[field], count ) for attrDict, count in res['Value']] )
    countDict['Total'] = sum( countDict.values() )
    return S_OK( countDict )

  def __addFilesToTransformation( self, transID, fileIDs, connection = False ):
    req = "SELECT FileID from TransformationFiles"
    req = req + " WHERE TransformationID = %d AND FileID IN (%s);" % ( transID, intListToString( fileIDs ) )
    res = self._query( req, connection )
    if not res['OK']:
      return res
    for tupleIn in res['Value']:
      fileIDs.remove( tupleIn[0] )
    if not fileIDs:
      return S_OK( [] )
    req = "INSERT INTO TransformationFiles (TransformationID,FileID,LastUpdate,InsertedTime) VALUES"
    for fileID in fileIDs:
      req = "%s (%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP())," % ( req, transID, fileID )
    req = req.rstrip( ',' )
    res = self._update( req, connection )
    if not res['OK']:
      return res
    return S_OK( fileIDs )

  def __insertExistingTransformationFiles( self, transID, fileTuplesList, connection = False ):
    """ Inserting already transformation files in TransformationFiles table (e.g. for deriving transformations)
    """
    gLogger.info( "Inserting %d files in TransformationFiles" % len( fileTuplesList ) )

    # splitting in various chunks, in case it is too big
    for fileTuples in breakListIntoChunks( fileTuplesList, 10000 ):

      gLogger.verbose( "Adding first %d files in TransformationFiles (out of %d)" % ( len( fileTuples ),
                                                                                      len( fileTuplesList ) ) )
      req = "INSERT INTO TransformationFiles (TransformationID,Status,TaskID,FileID,TargetSE,UsedSE,LastUpdate) VALUES"
      candidates = False

      for ft in fileTuples:
        _lfn, originalID, fileID, status, taskID, targetSE, usedSE, _errorCount, _lastUpdate, _insertTime = ft[:10]
        if status not in ( 'Removed', ):
          candidates = True
          if not re.search( '-', status ):
            status = "%s-inherited" % status
            if taskID:
              # Should be readable up to 999,999 tasks: that field is an int(11) in the DB, not a string
              taskID = 1000000 * int( originalID ) + int( taskID )
          req = "%s (%d,'%s','%d',%d,'%s','%s',UTC_TIMESTAMP())," % ( req, transID, status, taskID,
                                                                      fileID, targetSE, usedSE )
      if not candidates:
        continue

      req = req.rstrip( "," )
      res = self._update( req, connection )
      if not res['OK']:
        return res

    return S_OK()

  def __assignTransformationFile( self, transID, taskID, se, fileIDs, connection = False ):
    """ Make necessary updates to the TransformationFiles table for the newly created task
    """
    req = "UPDATE TransformationFiles SET TaskID='%d',UsedSE='%s',Status='Assigned',LastUpdate=UTC_TIMESTAMP()"
    req = ( req + " WHERE TransformationID = %d AND FileID IN (%s);" ) % ( taskID, se, transID, intListToString( fileIDs ) )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to assign file to task", res['Message'] )
    fileTuples = []
    for fileID in fileIDs:
      fileTuples.append( ( "(%d,%d,%d)" % ( transID, fileID, taskID ) ) )
    req = "INSERT INTO TransformationFileTasks (TransformationID,FileID,TaskID) VALUES %s" % ','.join( fileTuples )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to assign file to task", res['Message'] )
    return res

  def __setTransformationFileStatus( self, fileIDs, status, connection = False ):
    req = "UPDATE TransformationFiles SET Status = '%s' WHERE FileID IN (%s);" % ( status, intListToString( fileIDs ) )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to update file status", res['Message'] )
    return res

  def __setTransformationFileUsedSE( self, fileIDs, usedSE, connection = False ):
    req = "UPDATE TransformationFiles SET UsedSE = '%s' WHERE FileID IN (%s);" % ( usedSE, intListToString( fileIDs ) )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to update file usedSE", res['Message'] )
    return res

  def __resetTransformationFile( self, transID, taskID, connection = False ):
    req = "UPDATE TransformationFiles SET TaskID=NULL, UsedSE='Unknown', Status='Unused'\
     WHERE TransformationID = %d AND TaskID=%d;" % ( transID, taskID )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to reset transformation file", res['Message'] )
    return res

  def __deleteTransformationFiles( self, transID, connection = False ):
    """ Remove the files associated to a transformation """
    req = "DELETE FROM TransformationFiles WHERE TransformationID = %d;" % transID
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to delete transformation files", res['Message'] )
    return res

  ###########################################################################
  #
  # These methods manipulate the TransformationFileTasks table
  #

  def __deleteTransformationFileTask( self, transID, taskID, connection = False ):
    ''' Delete the file associated to a given task of a given transformation
        from the TransformationFileTasks table for transformation with TransformationID and TaskID
    '''
    req = "DELETE FROM TransformationFileTasks WHERE TransformationID=%d AND TaskID=%d" % ( transID, taskID )
    return self._update( req, connection )

  def __deleteTransformationFileTasks( self, transID, connection = False ):
    ''' Remove all associations between files, tasks and a transformation '''
    req = "DELETE FROM TransformationFileTasks WHERE TransformationID = %d;" % transID
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to delete transformation files/task history", res['Message'] )
    return res

  ###########################################################################
  #
  # These methods manipulate the TransformationTasks table
  #

  def getTransformationTasks( self, condDict = None, older = None, newer = None, timeStamp = 'CreationTime',
                              orderAttribute = None, limit = None, inputVector = False,
                              offset = None, connection = False ):
    connection = self.__getConnection( connection )
    req = "SELECT %s FROM TransformationTasks %s" % ( intListToString( self.TASKSPARAMS ),
                                                      self.buildCondition( condDict, older, newer, timeStamp,
                                                                           orderAttribute, limit, offset = offset ) )
    res = self._query( req, connection )
    if not res['OK']:
      return res
    if condDict is None:
      condDict = {}
    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = [str( item ) if not isinstance( item, ( long, int ) ) else item for item in row]
      taskDict = dict ( zip( self.TASKSPARAMS, row ) )
      webList.append( rList )
      if inputVector:
        taskDict['InputVector'] = ''
        taskID = taskDict['TaskID']
        transID = taskDict['TransformationID']
        res = self.getTaskInputVector( transID, taskID )
        if res['OK']:
          if taskID in res['Value']:
            taskDict['InputVector'] = res['Value'][taskID]
        else:
          return res
      resultList.append( taskDict )
    result = S_OK( resultList )
    result['Records'] = webList
    result['ParameterNames'] = self.TASKSPARAMS
    return result

  def getTasksForSubmission( self, transName, numTasks = 1, site = '', statusList = None,
                             older = None, newer = None, connection = False ):
    """ Select tasks with the given status (and site) for submission """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    if statusList is None:
      statusList = ['Created']
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    condDict = {"TransformationID":transID}
    if statusList:
      condDict["ExternalStatus"] = statusList
    if site:
      numTasks = 0
    res = self.getTransformationTasks( condDict = condDict, older = older, newer = newer,
                                       timeStamp = 'CreationTime', orderAttribute = None, limit = numTasks,
                                       inputVector = True, connection = connection )
    if not res['OK']:
      return res
    tasks = res['Value']

    # Now prepare the tasks
    resultDict = {}

    for taskDict in tasks:
      if len( resultDict ) >= numTasks:
        break
      taskDict['Status'] = taskDict.pop( 'ExternalStatus' )
      taskDict['InputData'] = taskDict.pop( 'InputVector' )
      taskDict.pop( 'LastUpdateTime' )
      taskDict.pop( 'CreationTime' )
      taskDict.pop( 'ExternalID' )
      taskID = taskDict['TaskID']
      resultDict[taskID] = taskDict
      if site:
        resultDict[taskID]['Site'] = site

    return S_OK( resultDict )

  def deleteTasks( self, transName, taskIDbottom, taskIDtop, author = '', connection = False ):
    """ Delete tasks with taskID range in transformation """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    for taskID in range( taskIDbottom, taskIDtop + 1 ):
      res = self.__removeTransformationTask( transID, taskID, connection = connection )
      if not res['OK']:
        return res
    message = "Deleted tasks from %d to %d" % ( taskIDbottom, taskIDtop )
    self.__updateTransformationLogging( transID, message, author, connection = connection )
    return res

  def reserveTask( self, transName, taskID, connection = False ):
    """ Reserve the taskID from transformation for submission """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__checkUpdate( "TransformationTasks", "ExternalStatus", "Reserved", {"TransformationID":transID,
                                                                                    "TaskID":taskID},
                              connection = connection )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR( 'Failed to set Reserved status for job %d - already Reserved' % int( taskID ) )
    # The job is reserved, update the time stamp
    res = self.setTaskStatus( transID, taskID, 'Reserved', connection = connection )
    if not res['OK']:
      return S_ERROR( 'Failed to set Reserved status for job %d - failed to update the time stamp' % int( taskID ) )
    return S_OK()

  def setTaskStatusAndWmsID( self, transName, taskID, status, taskWmsID, connection = False ):
    """ Set status and ExternalID for job with taskID in production with transformationID
    """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__setTaskParameterValue( transID, taskID, 'ExternalStatus', status, connection = connection )
    if not res['OK']:
      return res
    return self.__setTaskParameterValue( transID, taskID, 'ExternalID', taskWmsID, connection = connection )

  def setTaskStatus( self, transName, taskID, status, connection = False ):
    """ Set status for job with taskID in production with transformationID """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if not isinstance( taskID, list ):
      taskIDList = [taskID]
    else:
      taskIDList = list( taskID )
    for taskID in taskIDList:
      res = self.__setTaskParameterValue( transID, taskID, 'ExternalStatus', status, connection = connection )
      if not res['OK']:
        return res
    return S_OK()

  def getTransformationTaskStats( self, transName = '', connection = False ):
    """ Returns dictionary with number of jobs per status for the given production.
    """
    connection = self.__getConnection( connection )
    if transName:
      res = self._getTransformationID( transName, connection = connection )
      if not res['OK']:
        gLogger.error( "Failed to get ID for transformation", res['Message'] )
        return res
      res = self.getCounters( 'TransformationTasks', ['ExternalStatus'], {'TransformationID':res['Value']},
                              connection = connection )
    else:
      res = self.getCounters( 'TransformationTasks', ['ExternalStatus', 'TransformationID'], {},
                              connection = connection )
    if not res['OK']:
      return res
    statusDict = {}
    total = 0
    for attrDict, count in res['Value']:
      status = attrDict['ExternalStatus']
      statusDict[status] = count
      total += count
    statusDict['TotalCreated'] = total
    return S_OK( statusDict )

  def __setTaskParameterValue( self, transID, taskID, paramName, paramValue, connection = False ):
    req = "UPDATE TransformationTasks SET %s='%s', LastUpdateTime=UTC_TIMESTAMP()" % ( paramName, paramValue )
    req = req + " WHERE TransformationID=%d AND TaskID=%d;" % ( transID, taskID )
    return self._update( req, connection )

  def __deleteTransformationTasks( self, transID, connection = False ):
    """ Delete all the tasks from the TransformationTasks table for transformation with TransformationID
    """
    req = "DELETE FROM TransformationTasks WHERE TransformationID=%d" % transID
    return self._update( req, connection )

  def __deleteTransformationTask( self, transID, taskID, connection = False ):
    """ Delete the task from the TransformationTasks table for transformation with TransformationID
    """
    req = "DELETE FROM TransformationTasks WHERE TransformationID=%d AND TaskID=%d" % ( transID, taskID )
    return self._update( req, connection )

  ####################################################################
  #
  # These methods manipulate the TransformationInputDataQuery table
  #

  def createTransformationInputDataQuery( self, transName, queryDict, author = '', connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    return self.__addInputDataQuery( transID, queryDict, author = author, connection = connection )

  def __addInputDataQuery( self, transID, queryDict, author = '', connection = False ):
    res = self.getTransformationInputDataQuery( transID, connection = connection )
    if res['OK']:
      return S_ERROR( "Input data query already exists for transformation" )
    if res['Message'] != 'No InputDataQuery found for transformation':
      return res
    for parameterName in sorted( queryDict ):
      parameterValue = queryDict[parameterName]
      if not parameterValue:
        continue
      parameterType = 'String'
      if isinstance( parameterValue, ( list, tuple ) ):
        if isinstance( parameterValue[0], ( long, int ) ):
          parameterType = 'Integer'
          parameterValue = [str( x ) for x in parameterValue]
        parameterValue = ';;;'.join( parameterValue )
      else:
        if isinstance( parameterValue, ( long, int ) ):
          parameterType = 'Integer'
          parameterValue = str( parameterValue )
        if isinstance( parameterValue, dict ):
          parameterType = 'Dict'
          parameterValue = str( parameterValue )
      res = self.insertFields( 'TransformationInputDataQuery', ['TransformationID', 'ParameterName',
                                                                'ParameterValue', 'ParameterType'],
                               [transID, parameterName, parameterValue, parameterType], conn = connection )
      if not res['OK']:
        message = 'Failed to add input data query'
        self.deleteTransformationInputDataQuery( transID, connection = connection )
        break
      else:
        message = 'Added input data query'
    self.__updateTransformationLogging( transID, message, author, connection = connection )
    return res

  def deleteTransformationInputDataQuery( self, transName, author = '', connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "DELETE FROM TransformationInputDataQuery WHERE TransformationID=%d;" % transID
    res = self._update( req, connection )
    if not res['OK']:
      return res
    if res['Value']:
      # Add information to the transformation logging
      message = 'Deleted input data query'
      self.__updateTransformationLogging( transID, message, author, connection = connection )
    return res

  def getTransformationInputDataQuery( self, transName, connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "SELECT ParameterName,ParameterValue,ParameterType FROM TransformationInputDataQuery"
    req = req + " WHERE TransformationID=%d;" % transID
    res = self._query( req, connection )
    if not res['OK']:
      return res
    queryDict = {}
    for parameterName, parameterValue, parameterType in res['Value']:
      if re.search( ';;;', str( parameterValue ) ):
        parameterValue = parameterValue.split( ';;;' )
        if parameterType == 'Integer':
          parameterValue = [int( x ) for x in parameterValue]
      elif parameterType == 'Integer':
        parameterValue = int( parameterValue )
      elif parameterType == 'Dict':
        parameterValue = eval( parameterValue )
      queryDict[parameterName] = parameterValue
    if not queryDict:
      return S_ERROR( "No InputDataQuery found for transformation" )
    return S_OK( queryDict )

  ###########################################################################
  #
  # These methods manipulate the TaskInputs table
  #

  def getTaskInputVector( self, transName, taskID, connection = False ):
    """ Get input vector for the given task """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if not isinstance( taskID, list ):
      taskIDList = [taskID]
    else:
      taskIDList = list( taskID )
    taskString = ','.join( ["'%s'" % x for x in taskIDList] )
    req = "SELECT TaskID,InputVector FROM TaskInputs WHERE TaskID in (%s) AND TransformationID='%d';" % ( taskString,
                                                                                                          transID )
    res = self._query( req )
    inputVectorDict = {}
    if not res['OK']:
      return res
    elif res['Value']:
      for row in res['Value']:
        inputVectorDict[row[0]] = row[1]
    return S_OK( inputVectorDict )

  def __insertTaskInputs( self, transID, taskID, lfns, connection = False ):
    vector = str.join( ';', lfns )
    fields = ['TransformationID', 'TaskID', 'InputVector']
    values = [transID, taskID, vector]
    res = self.insertFields( 'TaskInputs', fields, values, connection )
    if not res['OK']:
      gLogger.error( "Failed to add input vector to task %d" % taskID )
    return res

  def __deleteTransformationTaskInputs( self, transID, taskID = 0, connection = False ):
    """ Delete all the tasks inputs from the TaskInputs table for transformation with TransformationID
    """
    req = "DELETE FROM TaskInputs WHERE TransformationID=%d" % transID
    if taskID:
      req = "%s AND TaskID=%d" % ( req, int( taskID ) )
    return self._update( req, connection )

  ###########################################################################
  #
  # These methods manipulate the TransformationLog table
  #

  def __updateTransformationLogging( self, transName, message, authorDN, connection = False ):
    """ Update the Transformation log table with any modifications
    """
    if not authorDN:
      res = getProxyInfo( False, False )
      if res['OK']:
        authorDN = res['Value']['subject']
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "INSERT INTO TransformationLog (TransformationID,Message,Author,MessageDate)"
    req = req + " VALUES (%s,'%s','%s',UTC_TIMESTAMP());" % ( transID, message, authorDN )
    return self._update( req, connection )

  def getTransformationLogging( self, transName, connection = False ):
    """ Get logging info from the TransformationLog table
    """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "SELECT TransformationID, Message, Author, MessageDate FROM TransformationLog"
    req = req + " WHERE TransformationID=%s ORDER BY MessageDate;" % ( transID )
    res = self._query( req )
    if not res['OK']:
      return res
    transList = []
    for transID, message, authorDN, messageDate in res['Value']:
      transDict = {}
      transDict['TransformationID'] = transID
      transDict['Message'] = message
      transDict['AuthorDN'] = authorDN
      transDict['MessageDate'] = messageDate
      transList.append( transDict )
    return S_OK( transList )

  def __deleteTransformationLog( self, transID, connection = False ):
    """ Remove the entries in the transformation log for a transformation
    """
    req = "DELETE FROM TransformationLog WHERE TransformationID=%d;" % transID
    return self._update( req, connection )

  ###########################################################################
  #
  # These methods manipulate the DataFiles table
  #
  def __getAllFileIDs( self, connection = False ):
    """ Get all the fileIDs for the supplied list of lfns
    """
    req = "SELECT LFN,FileID FROM DataFiles;"
    res = self._query( req, connection )
    if not res['OK']:
      return res
    fids = {}
    lfns = {}
    for lfn, fileID in res['Value']:
      fids[fileID] = lfn
      lfns[lfn] = fileID
    return S_OK( ( fids, lfns ) )

  def __getFileIDsForLfns( self, lfns, connection = False ):
    """ Get file IDs for the given list of lfns
        warning: if the file is not present, we'll see no errors
    """
    req = "SELECT LFN,FileID FROM DataFiles WHERE LFN in (%s);" % ( stringListToString( lfns ) )
    res = self._query( req, connection )
    if not res['OK']:
      return res
    fids = {}
    lfns = {}
    for lfn, fileID in res['Value']:
      fids[fileID] = lfn
      lfns[lfn] = fileID
    return S_OK( ( fids, lfns ) )

  def __getLfnsForFileIDs( self, fileIDs, connection = False ):
    """ Get lfns for the given list of fileIDs
    """
    req = "SELECT LFN,FileID FROM DataFiles WHERE FileID in (%s);" % stringListToString( fileIDs )
    res = self._query( req, connection )
    if not res['OK']:
      return res
    fids = {}
    lfns = {}
    for lfn, fileID in res['Value']:
      fids[lfn] = fileID
      lfns[fileID] = lfn
    return S_OK( ( fids, lfns ) )

  def __addDataFiles( self, lfns, connection = False ):
    """ Add a file to the DataFiles table and retrieve the FileIDs
    """
    res = self.__getFileIDsForLfns( lfns, connection = connection )
    if not res['OK']:
      return res
    _fileIDs, lfnFileIDs = res['Value']
    for lfn in lfns:
      if lfn not in lfnFileIDs:
        req = "INSERT INTO DataFiles (LFN,Status) VALUES ('%s','New');" % lfn
        res = self._update( req, connection )
        if not res['OK']:
          return res
        lfnFileIDs[lfn] = res['lastRowId']
    return S_OK( lfnFileIDs )

  def __setDataFileStatus( self, fileIDs, status, connection = False ):
    """ Set the status of the supplied files
    """
    req = "UPDATE DataFiles SET Status = '%s' WHERE FileID IN (%s);" % ( status, intListToString( fileIDs ) )
    return self._update( req, connection )

  ###########################################################################
  #
  # These methods manipulate multiple tables
  #

  def addTaskForTransformation( self, transID, lfns = None, se = 'Unknown', connection = False ):
    """ Create a new task with the supplied files for a transformation.
    """
    res = self._getConnectionTransID( connection, transID )
    if not res['OK']:
      return res
    if lfns is None:
      lfns = []
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    # Be sure the all the supplied LFNs are known to the database for the supplied transformation
    fileIDs = []
    if lfns:
      res = self.getTransformationFiles( condDict = {'TransformationID':transID, 'LFN':lfns}, connection = connection )
      if not res['OK']:
        return res
      foundLfns = set()
      for fileDict in res['Value']:
        fileIDs.append( fileDict['FileID'] )
        lfn = fileDict['LFN']
        if fileDict['Status'] in self.allowedStatusForTasks:
          foundLfns.add( lfn )
        else:
          gLogger.error( "Supplied file not in %s status but %s" % ( self.allowedStatusForTasks, fileDict['Status'] ), lfn )
      unavailableLfns = set( lfns ) - foundLfns
      if unavailableLfns:
        gLogger.error( "Supplied files not found for transformation", sorted( unavailableLfns ) )
        return S_ERROR( "Not all supplied files available in the transformation database" )

    # Insert the task into the jobs table and retrieve the taskID
    self.lock.acquire()
    req = "INSERT INTO TransformationTasks(TransformationID, ExternalStatus, ExternalID, TargetSE,"
    req = req + " CreationTime, LastUpdateTime)"
    req = req + " VALUES (%s,'%s','%d','%s', UTC_TIMESTAMP(), UTC_TIMESTAMP());" % ( transID, 'Created', 0, se )
    res = self._update( req, connection )
    if not res['OK']:
      self.lock.release()
      gLogger.error( "Failed to publish task for transformation", res['Message'] )
      return res

    # With InnoDB, TaskID is computed by a trigger, which sets the local variable @last (per connection)
    # @last is the last insert TaskID. With multi-row inserts, will be the first new TaskID inserted.
    # The trigger TaskID_Generator must be present with the InnoDB schema (defined in TransformationDB.sql)
    if self.isTransformationTasksInnoDB:
      res = self._query( "SELECT @last;", connection )
    else:
      res = self._query( "SELECT LAST_INSERT_ID();", connection )

    self.lock.release()
    if not res['OK']:
      return res
    taskID = int( res['Value'][0][0] )
    gLogger.verbose( "Published task %d for transformation %d." % ( taskID, transID ) )
    # If we have input data then update their status, and taskID in the transformation table
    if lfns:
      res = self.__insertTaskInputs( transID, taskID, lfns, connection = connection )
      if not res['OK']:
        self.__removeTransformationTask( transID, taskID, connection = connection )
        return res
      res = self.__assignTransformationFile( transID, taskID, se, fileIDs, connection = connection )
      if not res['OK']:
        self.__removeTransformationTask( transID, taskID, connection = connection )
        return res
    return S_OK( taskID )

  def extendTransformation( self, transName, nTasks, author = '', connection = False ):
    """ Extend SIMULATION type transformation by nTasks number of tasks
    """
    connection = self.__getConnection( connection )
    res = self.getTransformation( transName, connection = connection )
    if not res['OK']:
      gLogger.error( "Failed to get transformation details", res['Message'] )
      return res
    transType = res['Value']['Type']
    transID = res['Value']['TransformationID']
    extendableProds = Operations().getValue( 'Transformations/ExtendableTransfTypes', ['Simulation', 'MCSimulation'] )
    if transType.lower() not in [ep.lower() for ep in extendableProds]:
      return S_ERROR( 'Can not extend non-SIMULATION type production' )
    taskIDs = []
    for _task in range( nTasks ):
      res = self.addTaskForTransformation( transID, connection = connection )
      if not res['OK']:
        return res
      taskIDs.append( res['Value'] )
    # Add information to the transformation logging
    message = 'Transformation extended by %d tasks' % nTasks
    self.__updateTransformationLogging( transName, message, author, connection = connection )
    return S_OK( taskIDs )

  def cleanTransformation( self, transName, author = '', connection = False ):
    """ Clean the transformation specified by name or id """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__deleteTransformationFileTasks( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformationFiles( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformationTaskInputs( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformationTasks( transID, connection = connection )
    if not res['OK']:
      return res

    self.__updateTransformationLogging( transID, "Transformation Cleaned", author, connection = connection )

    return S_OK( transID )

  def deleteTransformation( self, transName, author = '', connection = False ):
    """ Remove the transformation specified by name or id """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.cleanTransformation( transID, author = author, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformationLog( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformationParameters( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformation( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.__updateFilters()
    if not res['OK']:
      return res
    return S_OK()

  def __removeTransformationTask( self, transID, taskID, connection = False ):
    res = self.__deleteTransformationTaskInputs( transID, taskID, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformationFileTask( transID, taskID, connection = connection )
    if not res['OK']:
      return res
    res = self.__resetTransformationFile( transID, taskID, connection = connection )
    if not res['OK']:
      return res
    return self.__deleteTransformationTask( transID, taskID, connection = connection )

  def __checkUpdate( self, table, param, paramValue, selectDict = None, connection = False ):
    """ Check whether the update will perform an update """
    req = "UPDATE %s SET %s = '%s'" % ( table, param, paramValue )
    if selectDict:
      req = "%s %s" % ( req, self.buildCondition( selectDict ) )
    return self._update( req, connection )

  def __getConnection( self, connection ):
    if connection:
      return connection
    res = self._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn( "Failed to get MySQL connection", res['Message'] )
    return connection

  def _getConnectionTransID( self, connection, transName ):
    connection = self.__getConnection( connection )
    res = self._getTransformationID( transName, connection = connection )
    if not res['OK']:
      gLogger.error( "Failed to get ID for transformation", res['Message'] )
      return res
    transID = res['Value']
    resDict = {'Connection':connection, 'TransformationID':transID}
    return S_OK( resDict )

####################################################################################
#
#  This part should correspond to the DIRAC Standard File Catalog interface
#
####################################################################################

  def exists( self, lfns, connection = False ):
    """ Check the presence of the lfn in the TransformationDB DataFiles table
    """
    gLogger.info( "TransformationDB.exists: Attempting to determine existence of %s files." % len( lfns ) )
    res = self.__getFileIDsForLfns( lfns, connection = connection )
    if not res['OK']:
      return res
    fileIDs, _lfnFilesIDs = res['Value']
    failed = {}
    successful = {}
    fileIDsValues = set( fileIDs.values() )
    for lfn in lfns:
      successful[lfn] = ( lfn in fileIDsValues )
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def removeFile( self, lfns, connection = False ):
    """ Remove file specified by lfn from the ProcessingDB
    """
    gLogger.info( "TransformationDB.removeFile: Attempting to remove %s files." % len( lfns ) )
    failed = {}
    successful = {}
    connection = self.__getConnection( connection )
    if not lfns:
      return S_ERROR( "No LFNs supplied" )
    res = self.__getFileIDsForLfns( lfns, connection = connection )
    if not res['OK']:
      return res
    fileIDs, lfnFilesIDs = res['Value']
    for lfn in lfns:
      if lfn not in lfnFilesIDs:
        successful[lfn] = 'File does not exist'
    if fileIDs:
      res = self.__setTransformationFileStatus( fileIDs.keys(), 'Deleted', connection = connection )
      if not res['OK']:
        return res
      res = self.__setDataFileStatus( fileIDs.keys(), 'Deleted', connection = connection )
      if not res['OK']:
        return S_ERROR( "TransformationDB.removeFile: Failed to remove files." )
    for lfn in lfnFilesIDs:
      if lfn not in failed:
        successful[lfn] = True
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )