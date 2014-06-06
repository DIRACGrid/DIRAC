''' DIRAC Transformation DB

    Transformation database is used to collect and serve the necessary information
    in order to automate the task of job preparation for high level transformations.
    This class is typically used as a base class for more specific data processing
    databases
'''

import re, time, threading, copy
from types import IntType, LongType, StringTypes, ListType, TupleType, DictType

from DIRAC                                                import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                   import DB
from DIRAC.Resources.Catalog.FileCatalog                  import FileCatalog
from DIRAC.Core.Security.ProxyInfo                        import getProxyInfo
from DIRAC.Core.Utilities.List                            import stringListToString, intListToString, sortList, breakListIntoChunks
from DIRAC.Core.Utilities.Shifter                         import setupShifterProxyInEnv
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations
from DIRAC.Core.Utilities.Subprocess                      import pythonCall

__RCSID__ = "$Id$"

MAX_ERROR_COUNT = 10

#############################################################################

class TransformationDB( DB ):
  """ TransformationDB class
  """

  def __init__( self, maxQueueSize = 10, dbIn = None ):
    ''' The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    '''

    if not dbIn:
      DB.__init__( self, 'TransformationDB', 'Transformation/TransformationDB', maxQueueSize )

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
                          'TransformationFamily']  # for the moment include TransformationFamily

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

    default_task_statuses = ['Created', 'Submitted', 'Checking', 'Staging', 'Waiting', 'Running',
                             'Done', 'Completed', 'Killed', 'Stalled', 'Failed', 'Rescheduled']
    default_file_statuses = ['Unused', 'Assigned', 'Processed', 'Problematic']
    self.tasksStatuses = default_task_statuses + Operations().getValue( 'Transformations/TasksStates', [] )
    self.fileStatuses = default_file_statuses + Operations().getValue( 'Transformations/FilesStatuses', [] )

    result = self.__initializeDB()
    if not result[ 'OK' ]:
      self.log.fatal( "Cannot initialize TransformationDB!", result[ 'Message' ] )

  def _generateTables( self ):
    """ _generateTables
    
    Method that returns a dictionary with all the tables to be created. It also
    makes easier its extension by DIRAC plugins.
    """
    
    retVal = self._query( "SHOW tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesD = {}

    if 'AdditionalParameters' not in tablesInDB:
      tablesD[ 'AdditionalParameters' ] = {'Fields': {'ParameterName': 'VARCHAR(32) NOT NULL',
                                                      'ParameterType': "VARCHAR(32) DEFAULT 'StringType'",
                                                      'ParameterValue': 'LONGBLOB NOT NULL',
                                                      'TransformationID': 'INTEGER NOT NULL'},
                                           'PrimaryKey': ['TransformationID', 'ParameterName'],
                                           'Engine': 'InnoDB'
                                           }

    if 'DataFiles' not in tablesInDB:
      tablesD['DataFiles'] = {'Fields': {'FileID': 'INTEGER NOT NULL AUTO_INCREMENT',
                                         'LFN': 'VARCHAR(255) UNIQUE',
                                         'Status': "VARCHAR(32) DEFAULT 'AprioriGood'"},
                              'Indexes': {'Status': ['Status']},
                              'PrimaryKey': ['FileID'],
                              'Engine': 'InnoDB'
                              }

    if 'Replicas' not in tablesInDB:
      tablesD['Replicas'] = {'Fields': {'FileID': 'INTEGER NOT NULL',
                                        'PFN': 'VARCHAR(255)',
                                        'SE': 'VARCHAR(32)',
                                        'Status': "VARCHAR(32) DEFAULT 'AprioriGood'"},
                             'Indexes': {'Status': ['Status']},
                             'PrimaryKey': ['FileID', 'SE'],
                             'Engine': 'InnoDB'
                             }

    if 'TaskInputs' not in tablesInDB:
      tablesD['TaskInputs'] = {'Fields': {'InputVector': 'BLOB',
                                          'TaskID': 'INTEGER NOT NULL',
                                          'TransformationID': 'INTEGER NOT NULL'},
                               'PrimaryKey': ['TransformationID', 'TaskID'],
                               'Engine': 'InnoDB'
                               }

    if 'TransformationFileTasks' not in tablesInDB:
      tablesD['TransformationFileTasks'] = {'Fields': {'FileID': 'INTEGER NOT NULL',
                                                       'TaskID': 'INTEGER NOT NULL',
                                                       'TransformationID': 'INTEGER NOT NULL'},
                                            'PrimaryKey': ['TransformationID', 'FileID', 'TaskID'],
                                            'Engine': 'InnoDB'
                                            }

    if 'TransformationFiles' not in tablesInDB:
      tablesD['TransformationFiles'] = {'Fields': { 'TransformationID': 'INTEGER NOT NULL',
                                                    'FileID': 'INTEGER NOT NULL',
                                                    'TaskID': 'INTEGER',
                                                    'ErrorCount': 'INT(4) NOT NULL DEFAULT 0',
                                                    'InsertedTime': 'DATETIME',
                                                    'LastUpdate': 'DATETIME',
                                                    'Status': 'VARCHAR(32) DEFAULT "Unused"',
                                                    'TargetSE': 'VARCHAR(255) DEFAULT "Unknown"',
                                                    'UsedSE': 'VARCHAR(255) DEFAULT "Unknown"'},
                                         'Indexes': {'Status': ['Status'],
                                                     'TransformationID': ['TransformationID']},
                                         'PrimaryKey': ['TransformationID', 'FileID'],
                                         'Engine': 'InnoDB'
                                         }

    if 'TransformationInputDataQuery' not in tablesInDB:
      tablesD['TransformationInputDataQuery'] = {'Fields': {'ParameterName': 'VARCHAR(512) NOT NULL',
                                                            'ParameterType': 'VARCHAR(8) NOT NULL',
                                                            'ParameterValue': 'BLOB NOT NULL',
                                                            'TransformationID': 'INTEGER NOT NULL'},
                                                 'PrimaryKey': ['TransformationID', 'ParameterName'],
                                                 'Engine': 'InnoDB'
                                                 }

    if 'TransformationLog' not in tablesInDB:
      tablesD['TransformationLog'] = {'Fields': {'Author': 'VARCHAR(255) NOT NULL DEFAULT "Unknown"',
                                                 'Message': 'VARCHAR(255) NOT NULL',
                                                 'MessageDate': 'DATETIME NOT NULL',
                                                 'TransformationID': 'INTEGER NOT NULL'},
                                      'Indexes': {'MessageDate': ['MessageDate'],
                                                  'TransformationID': ['TransformationID']},
                                      'Engine': 'InnoDB'
                                      }

    if 'TransformationTasks' not in tablesInDB:
      ## The engine of that table must stay MyISAM, because the addTaskToTransformation needs 
      # that when inserting a row, the LAST_INSERT_ID returns the last task ID for 
      # the given transformation. This only works because TaskID is NOT an INDEX
      # and because the engine is MyISAM.
      tablesD['TransformationTasks'] = {'Fields': {'CreationTime': 'DATETIME NOT NULL',
                                                   'ExternalID': "char(16) DEFAULT ''",
                                                   'ExternalStatus': "char(16) DEFAULT 'Created'",
                                                   'LastUpdateTime': 'DATETIME NOT NULL',
                                                   'TargetSE': "char(255) DEFAULT 'Unknown'",
                                                   'TaskID': 'INTEGER NOT NULL AUTO_INCREMENT',
                                                   'TransformationID': 'INTEGER NOT NULL'},
                                        'Indexes': {'ExternalStatus': ['ExternalStatus']},
                                        'PrimaryKey': ['TransformationID', 'TaskID'],
                                        'Engine': 'MyISAM'
                                        },
    
    if 'Transformations' not in tablesInDB:
      tablesD['Transformations'] = {'Fields': {'AgentType': "CHAR(32) DEFAULT 'Manual'",
                                               'AuthorDN': 'VARCHAR(255) NOT NULL',
                                               'AuthorGroup': 'VARCHAR(255) NOT NULL',
                                               'Body': 'LONGBLOB',
                                               'CreationDate': 'DATETIME',
                                               'Description': 'VARCHAR(255)',
                                               'EventsPerTask': 'INT NOT NULL DEFAULT 0',
                                               'FileMask': 'VARCHAR(255)',
                                               'GroupSize': 'INT NOT NULL DEFAULT 1',
                                               'InheritedFrom': 'INTEGER DEFAULT 0',
                                               'LastUpdate': 'DATETIME',
                                               'LongDescription': 'BLOB',
                                               'MaxNumberOfTasks': 'INT NOT NULL DEFAULT 0',
                                               'Plugin': "CHAR(32) DEFAULT 'None'",
                                               'Status': "CHAR(32) DEFAULT 'New'",
                                               'TransformationFamily': "varchar(64) default '0'",
                                               'TransformationGroup': "varchar(64) NOT NULL default 'General'",
                                               'TransformationID': 'INTEGER NOT NULL AUTO_INCREMENT',
                                               'TransformationName': 'VARCHAR(255) NOT NULL',
                                               'Type': "CHAR(32) DEFAULT 'Simulation'"},
                                    'Indexes': {'TransformationName': ['TransformationName']},
                                    'PrimaryKey': ['TransformationID'],
                                    'Engine': 'InnoDB'
                                    }

    if 'TransformationCounters' not in tablesInDB:
      tablesD['TransformationCounters'] = {'Fields': {'TransformationID' : "INTEGER NOT NULL"},
                                           'PrimaryKey': ['TransformationID'],
                                           'Engine': 'InnoDB'
                                           }
      ##Get from the CS the list of columns names
      for status in self.tasksStatuses + self.fileStatuses:
        tablesD['TransformationCounters']['Fields'][status] = 'INTEGER DEFAULT 0'
        
    return S_OK( tablesD )        

  def __initializeDB( self ):
    ''' Initialize: create tables if needed
    '''

    tablesToBeCreated = self._generateTables()
    if not tablesToBeCreated[ 'OK' ]:
      return tablesToBeCreated
    tablesToBeCreated = tablesToBeCreated[ 'Value' ]

    if tablesToBeCreated:
      gLogger.verbose( "Creating tables %s" % ( ', '.join( tablesToBeCreated.keys() ) ) )
      res = self._createTables( tablesToBeCreated )
      if not res['OK']:
        return res

    #Get the available counters
    retVal = self._query( "EXPLAIN TransformationCounters" )
    if not retVal[ 'OK' ]:
      return retVal
    TSCounterFields = [ t[0] for t in retVal[ 'Value' ] ]
    for status in list( set( self.tasksStatuses + self.fileStatuses ) - set( TSCounterFields ) ):
      altertable = "ALTER TABLE TransformationCounters ADD COLUMN `%s` INTEGER DEFAULT 0" % status
      retVal = self._update( altertable )
      if not retVal['OK']:
        return retVal

    return S_OK()

    # This is here to ensure full compatibility between different versions of the MySQL DB schema
    self.isTransformationTasksInnoDB = True
    res = self._query( "SELECT Engine FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'TransformationTasks'" )
    if not res['OK']:
      raise RuntimeError, res['Message']
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
    ''' Add new transformation definition including its input streams
    '''
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
      self.filters.append( ( transID, re.compile( fileMask ) ) )

    if inheritedFrom:
      res = self._getTransformationID( inheritedFrom, connection = connection )
      if not res['OK']:
        gLogger.error( "Failed to get ID for parent transformation: %s, now deleting" % res['Message'] )
        return self.deleteTransformation( transID, connection = connection )
      originalID = res['Value']
      # FIXME: this is not the right place to change status information, and in general the whole should not be here
      res = self.setTransformationParameter( originalID, 'Status', 'Completing',
                                             author = authorDN, connection = connection )
      if not res['OK']:
        gLogger.error( "Failed to update parent transformation status: %s, now deleting" % res['Message'] )
        return self.deleteTransformation( transID, connection = connection )
      message = 'Creation of the derived transformation (%d)' % transID
      self.__updateTransformationLogging( originalID, message, authorDN, connection = connection )
      res = self.getTransformationFiles( condDict = {'TransformationID':originalID}, connection = connection )
      if not res['OK']:
        gLogger.error( "Could not get transformation files: %s, now deleting" % res['Message'] )
        return self.deleteTransformation( transID, connection = connection )
      if res['Records']:
        res = self.__insertExistingTransformationFiles( transID, res['Records'], connection = connection )
        if not res['OK']:
          gLogger.error( "Could not insert files: %s, now deleting" % res['Message'] )
          return self.deleteTransformation( transID, connection = connection )
    if addFiles and fileMask:
      self.__addExistingFiles( transID, connection = connection )
    message = "Created transformation %d" % transID
    self.__updateTransformationLogging( transID, message, authorDN, connection = connection )
    return S_OK( transID )

  def getTransformations( self, condDict = {}, older = None, newer = None, timeStamp = 'LastUpdate',
                          orderAttribute = None, limit = None, extraParams = False, offset = None, connection = False ):
    ''' Get parameters of all the Transformations with support for the web standard structure '''
    connection = self.__getConnection( connection )
    req = "SELECT %s FROM Transformations %s" % ( intListToString( self.TRANSPARAMS ),
                                                  self.buildCondition( condDict, older, newer, timeStamp,
                                                                       orderAttribute, limit, offset = offset ) )
    res = self._query( req, connection )
    if not res['OK']:
      return res
    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = []
      transDict = {}
      count = 0
      for item in row:
        transDict[self.TRANSPARAMS[count]] = item
        count += 1
        if type( item ) not in [IntType, LongType]:
          rList.append( str( item ) )
        else:
          rList.append( item )
      webList.append( rList )
      if extraParams:
        res = self.__getAdditionalParameters( transDict['TransformationID'], connection = connection )
        if not res['OK']:
          return res
        transDict.update( res['Value'] )
      resultList.append( transDict )
    result = S_OK( resultList )
    result['Records'] = webList
    result['ParameterNames'] = copy.copy( self.TRANSPARAMS )
    return result

  def getTransformation( self, transName, extraParams = False, connection = False ):
    '''Get Transformation definition and parameters of Transformation identified by TransformationID
    '''
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
    ''' Get the requested parameters for a supplied transformation '''
    if type( parameters ) in StringTypes:
      parameters = [parameters]
    extraParams = False
    for param in parameters:
      if not param in self.TRANSPARAMS:
        extraParams = True
    res = self.getTransformation( transName, extraParams = extraParams, connection = connection )
    if not res['OK']:
      return res
    transParams = res['Value']
    paramDict = {}
    for reqParam in parameters:
      if not reqParam in transParams.keys():
        return S_ERROR( "Parameter %s not defined for transformation" % reqParam )
      paramDict[reqParam] = transParams[reqParam]
    if len( paramDict ) == 1:
      return S_OK( paramDict[reqParam] )
    return S_OK( paramDict )

  def getTransformationWithStatus( self, status, connection = False ):
    ''' Gets a list of the transformations with the supplied status '''
    req = "SELECT TransformationID FROM Transformations WHERE Status = '%s';" % status
    res = self._query( req, conn = connection )
    if not res['OK']:
      return res
    transIDs = []
    for tupleIn in res['Value']:
      transIDs.append( tupleIn[0] )
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
    if not ( paramName in self.mutable ):
      return S_ERROR( "Can not update the '%s' transformation parameter" % paramName )
    req = "UPDATE Transformations SET %s='%s', LastUpdate=UTC_TIMESTAMP() WHERE TransformationID=%d" % ( paramName,
                                                                                                          paramValue,
                                                                                                          transID )
    return self._update( req, connection )

  def _getTransformationID( self, transName, connection = False ):
    ''' Method returns ID of transformation with the name=<name> '''
    try:
      transName = long( transName )
      cmd = "SELECT TransformationID from Transformations WHERE TransformationID=%d;" % transName
    except:
      if type( transName ) not in StringTypes:
        return S_ERROR( "Transformation should ID or name" )
      cmd = "SELECT TransformationID from Transformations WHERE TransformationName='%s';" % transName
    res = self._query( cmd, connection )
    if not res['OK']:
      gLogger.error( "Failed to obtain transformation ID for transformation", "%s:%s" % ( transName, res['Message'] ) )
      return res
    elif not res['Value']:
      gLogger.verbose( "Transformation %s does not exist" % ( transName ) )
      return S_ERROR( "Transformation does not exist" )
    return S_OK( res['Value'][0][0] )

  def __deleteTransformation( self, transID, connection = False ):
    req = "DELETE FROM Transformations WHERE TransformationID=%d;" % transID
    return self._update( req, connection )

  def __updateFilters( self, connection = False ):
    ''' Get filters for all defined input streams in all the transformations.
        If transID argument is given, get filters only for this transformation.
    '''
    resultList = []
    # Define the general filter first
    self.database_name = self.__class__.__name__
    value = Operations().getValue( 'InputDataFilter/%sFilter' % self.database_name, '' )
    if value:
      refilter = re.compile( value )
      resultList.append( ( 0, refilter ) )
    # Per transformation filters
    req = "SELECT TransformationID,FileMask FROM Transformations;"
    res = self._query( req, connection )
    if not res['OK']:
      return res
    for transID, mask in res['Value']:
      if mask:
        refilter = re.compile( mask )
        resultList.append( ( transID, refilter ) )
    self.filters = resultList
    return S_OK( resultList )

  def __filterFile( self, lfn, filters = None ):
    '''Pass the input file through a supplied filter or those currently active '''
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
    ''' Add a parameter for the supplied transformations '''
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    message = ''
    if paramName in self.TRANSPARAMS:
      res = self.__updateTransformationParameter( transID, paramName, paramValue, connection = connection )
      if res['OK'] and ( paramName != 'Body' ):
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
    ''' Delete a parameter from the additional parameters table '''
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
    if type( paramValue ) in [IntType, LongType]:
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
    for transID, parameterName, parameterValue, parameterType in res['Value']:
      parameterType = eval( parameterType )
      if parameterType in [IntType, LongType]:
        parameterValue = int( parameterValue )
      paramDict[parameterName] = parameterValue
    return S_OK( paramDict )

  def __deleteTransformationParameters( self, transID, parameters = [], connection = False ):
    ''' Remove the parameters associated to a transformation '''
    req = "DELETE FROM AdditionalParameters WHERE TransformationID=%d" % transID
    if parameters:
      req = "%s AND ParameterName IN (%s);" % ( req, stringListToString( parameters ) )
    return self._update( req, connection )

  ###########################################################################
  #
  # These methods manipulate the TransformationFiles table
  #

  def addFilesToTransformation( self, transName, lfns, connection = False ):
    ''' Add a list of LFNs to the transformation directly '''
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
      for fileID in fileIDs.keys():
        lfn = fileIDs[fileID]
        successful[lfn] = "Present"
        if fileID in res['Value']:
          successful[lfn] = "Added"
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def getTransformationFiles( self, condDict = {}, older = None, newer = None, timeStamp = 'LastUpdate',
                              orderAttribute = None, limit = None, offset = None, connection = False ):
    ''' Get files for the supplied transformations with support for the web standard structure '''
    connection = self.__getConnection( connection )
    req = "SELECT %s FROM TransformationFiles" % ( intListToString( self.TRANSFILEPARAMS ) )
    originalFileIDs = {}
    if condDict or older or newer:
      if condDict.has_key( 'LFN' ):
        lfns = condDict.pop( 'LFN' )
        if type( lfns ) in StringTypes:
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
        rList = [lfn]
        fDict = {}
        fDict['LFN'] = lfn
        count = 0
        for item in row:
          fDict[self.TRANSFILEPARAMS[count]] = item
          count += 1
          if type( item ) not in [IntType, LongType]:
            rList.append( str( item ) )
          else:
            rList.append( item )
        webList.append( rList )
        resultList.append( fDict )
    result = S_OK( resultList )
    # result['LFNs'] = originalFileIDs.values()
    result['Records'] = webList
    result['ParameterNames'] = ['LFN'] + self.TRANSFILEPARAMS
    return result

  def getFileSummary( self, lfns, connection = False ):
    ''' Get file status summary in all the transformations '''
    connection = self.__getConnection( connection )
    condDict = {'LFN':lfns}
    res = self.getTransformationFiles( condDict = condDict, connection = connection )
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      lfn = fileDict['LFN']
      transID = fileDict['TransformationID']
      if not resDict.has_key( lfn ):
        resDict[lfn] = {}
      if not resDict[lfn].has_key( transID ):
        resDict[lfn][transID] = {}
      resDict[lfn][transID] = fileDict
    failedDict = {}
    for lfn in lfns:
      if not resDict.has_key( lfn ):
        failedDict[lfn] = 'Did not exist in the Transformation database'
    return S_OK( {'Successful':resDict, 'Failed':failedDict} )

  def setFileStatusForTransformation( self, transID, fileStatusDict = {}, connection = False ):
    """ Set file status for the given transformation, based on
        fileStatusDict {fileID_A: 'statusA', fileID_B: 'statusB', ...}

        The ErrorCount is incremented automatically here
    """
    if not fileStatusDict:
      return S_OK()

    # Building the request with "ON DUPLICATE KEY UPDATE"
    req = "INSERT INTO TransformationFiles (TransformationID, FileID, Status, ErrorCount, LastUpdate) VALUES "

    updatesList = []
    for fileID, status in fileStatusDict.items():

      updatesList.append( "(%d, %d, '%s', VALUES(ErrorCount), UTC_TIMESTAMP())" % ( transID, fileID, status ) )

    req += ','.join( updatesList )
    req += " ON DUPLICATE KEY UPDATE Status=VALUES(Status),ErrorCount=ErrorCount+1,LastUpdate=VALUES(LastUpdate)"

    return self._update( req, connection )

  def getTransformationStats( self, transName, connection = False ):
    ''' Get number of files in Transformation Table for each status '''
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.getCounters( 'TransformationFiles', ['TransformationID', 'Status'], {'TransformationID':transID} )
    if not res['OK']:
      return res
    statusDict = {}
    total = 0
    for attrDict, count in res['Value']:
      status = attrDict['Status']
      if not re.search( '-', status ):
        statusDict[status] = count
        total += count
    statusDict['Total'] = total
    return S_OK( statusDict )

  def getTransformationFilesCount( self, transName, field, selection = {}, connection = False ):
    ''' Get the number of files in the TransformationFiles table grouped by the supplied field '''
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    selection['TransformationID'] = transID
    if field not in self.TRANSFILEPARAMS:
      return S_ERROR( "Supplied field not in TransformationFiles table" )
    res = self.getCounters( 'TransformationFiles', ['TransformationID', field], selection )
    if not res['OK']:
      return res
    countDict = {}
    total = 0
    for attrDict, count in res['Value']:
      countDict[attrDict[field]] = count
      total += count
    countDict['Total'] = total
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

  def __addExistingFiles( self, transID, connection = False ):
    ''' Add files that already exist in the DataFiles table to the transformation specified by the transID
    '''
    for tID, _filter in self.filters:
      if tID == transID:
        filters = [( tID, filter )]
        break
    if not filters:
      return S_ERROR( 'No filters defined for transformation %d' % transID )
    res = self.__getAllFileIDs( connection = connection )
    if not res['OK']:
      return res
    fileIDs, _lfnFilesIDs = res['Value']
    passFilter = []
    for fileID, lfn in fileIDs.items():
      if self.__filterFile( lfn, filters ):
        passFilter.append( fileID )
    return self.__addFilesToTransformation( transID, passFilter, connection = connection )

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
        if status not in ( 'Unused', 'Removed' ):
          candidates = True
          if not re.search( '-', status ):
            status = "%s-inherited" % status
            if taskID:
              taskID = str( int( originalID ) ).zfill( 8 ) + '_' + str( int( taskID ) ).zfill( 8 )
          req = "%s (%d,'%s','%s',%d,'%s','%s',UTC_TIMESTAMP())," % ( req, transID, status, taskID,
                                                                      fileID, targetSE, usedSE )
      if not candidates:
        continue

      req = req.rstrip( "," )
      res = self._update( req, connection )
      if not res['OK']:
        return res

    return S_OK()

  def __assignTransformationFile( self, transID, taskID, se, fileIDs, connection = False ):
    ''' Make necessary updates to the TransformationFiles table for the newly created task
    '''
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
    ''' Remove the files associated to a transformation '''
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

  def getTransformationTasks( self, condDict = {}, older = None, newer = None, timeStamp = 'CreationTime',
                              orderAttribute = None, limit = None, inputVector = False,
                              offset = None, connection = False ):
    connection = self.__getConnection( connection )
    req = "SELECT %s FROM TransformationTasks %s" % ( intListToString( self.TASKSPARAMS ),
                                                      self.buildCondition( condDict, older, newer, timeStamp,
                                                                           orderAttribute, limit, offset = offset ) )
    res = self._query( req, connection )
    if not res['OK']:
      return res
    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = []
      taskDict = {}
      count = 0
      for item in row:
        taskDict[self.TASKSPARAMS[count]] = item
        count += 1
        if type( item ) not in [IntType, LongType]:
          rList.append( str( item ) )
        else:
          rList.append( item )
      webList.append( rList )
      if inputVector:
        taskDict['InputVector'] = ''
        taskID = taskDict['TaskID']
        transID = taskDict['TransformationID']
        res = self.getTaskInputVector( transID, taskID )
        if res['OK']:
          if res['Value'].has_key( taskID ):
            taskDict['InputVector'] = res['Value'][taskID]
      resultList.append( taskDict )
    result = S_OK( resultList )
    result['Records'] = webList
    result['ParameterNames'] = self.TASKSPARAMS
    return result

  def getTasksForSubmission( self, transName, numTasks = 1, site = '', statusList = ['Created'],
                             older = None, newer = None, connection = False ):
    ''' Select tasks with the given status (and site) for submission '''
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
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
    ''' Delete tasks with taskID range in transformation '''
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
    ''' Reserve the taskID from transformation for submission '''
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
    ''' Set status and ExternalID for job with taskID in production with transformationID
    '''
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
    ''' Set status for job with taskID in production with transformationID '''
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if type( taskID ) != ListType:
      taskIDList = [taskID]
    else:
      taskIDList = list( taskID )
    for taskID in taskIDList:
      res = self.__setTaskParameterValue( transID, taskID, 'ExternalStatus', status, connection = connection )
      if not res['OK']:
        return res
    return S_OK()

  def getTransformationTaskStats( self, transName = '', connection = False ):
    ''' Returns dictionary with number of jobs per status for the given production.
    '''
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
    ''' Delete all the tasks from the TransformationTasks table for transformation with TransformationID
    '''
    req = "DELETE FROM TransformationTasks WHERE TransformationID=%d" % transID
    return self._update( req, connection )

  def __deleteTransformationTask( self, transID, taskID, connection = False ):
    ''' Delete the task from the TransformationTasks table for transformation with TransformationID
    '''
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
    for parameterName in sortList( queryDict.keys() ):
      parameterValue = queryDict[parameterName]
      if not parameterValue:
        continue
      parameterType = 'String'
      if type( parameterValue ) in [ListType, TupleType]:
        if type( parameterValue[0] ) in [IntType, LongType]:
          parameterType = 'Integer'
          parameterValue = [str( x ) for x in parameterValue]
        parameterValue = ';;;'.join( parameterValue )
      else:
        if type( parameterValue ) in [IntType, LongType]:
          parameterType = 'Integer'
          parameterValue = str( parameterValue )
        if type( parameterValue ) == DictType:
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
    ''' Get input vector for the given task '''
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if type( taskID ) != ListType:
      taskIDList = [taskID]
    else:
      taskIDList = list( taskID )
    taskString = ','.join( ["'" + str( x ) + "'" for x in taskIDList] )
    req = "SELECT TaskID,InputVector FROM TaskInputs WHERE TaskID in (%s) AND TransformationID='%d';" % ( taskString,
                                                                                                          transID )
    res = self._query( req )
    inputVectorDict = {}
    if res['OK'] and res['Value']:
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
    ''' Delete all the tasks inputs from the TaskInputs table for transformation with TransformationID
    '''
    req = "DELETE FROM TaskInputs WHERE TransformationID=%d" % transID
    if taskID:
      req = "%s AND TaskID=%d" % ( req, int( taskID ) )
    return self._update( req, connection )

  ###########################################################################
  #
  # These methods manipulate the TransformationLog table
  #

  def __updateTransformationLogging( self, transName, message, authorDN, connection = False ):
    ''' Update the Transformation log table with any modifications
    '''
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
    ''' Get logging info from the TransformationLog table
    '''
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
    ''' Remove the entries in the transformation log for a transformation
    '''
    req = "DELETE FROM TransformationLog WHERE TransformationID=%d;" % transID
    return self._update( req, connection )

  ###########################################################################
  #
  # These methods manipulate the DataFiles table
  #
  def __getAllFileIDs( self, connection = False ):
    ''' Get all the fileIDs for the supplied list of lfns
    '''
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
    ''' Get lfns for the given list of fileIDs
    '''
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
    ''' Add a file to the DataFiles table and retrieve the FileIDs
    '''
    res = self.__getFileIDsForLfns( lfns, connection = connection )
    if not res['OK']:
      return res
    _fileIDs, lfnFileIDs = res['Value']
    for lfn in lfns:
      if not lfn in lfnFileIDs.keys():
        req = "INSERT INTO DataFiles (LFN,Status) VALUES ('%s','New');" % lfn
        res = self._update( req, connection )
        if not res['OK']:
          return res
        lfnFileIDs[lfn] = res['lastRowId']
    return S_OK( lfnFileIDs )

  def __setDataFileStatus( self, fileIDs, status, connection = False ):
    ''' Set the status of the supplied files
    '''
    req = "UPDATE DataFiles SET Status = '%s' WHERE FileID IN (%s);" % ( status, intListToString( fileIDs ) )
    return self._update( req, connection )

  ###########################################################################
  #
  # Fill in / get the counters
  #

  def updateTransformationCounters( self, counterDict, connection = False ):
    ''' Insert in the table or update the transformation counters given a dict
    '''
    ##first, check that all the keys in this dict are among those expected
    for key in self.tasksStatuses + self.fileStatuses:
      if key not in counterDict.keys():
        return S_ERROR( "Key %s not in the table" % key )

    res = self.getFields( "TransformationCounters", ['TransformationID'],
                          condDict = {'TransformationID' : counterDict['TransformationID']}, conn = connection )
    if not res['OK']:
      return res
    if len( res['Value'] ): #if the Transformation is already in:
      res = self.updateFields( "TransformationCounters",
                               condDict = {'TransformationID' : counterDict['TransformationID']},
                               updateDict = counterDict,
                               conn = connection )
      if not res['OK']:
        return res
    else:
      res = self.insertFields( "TransformationCounters", inDict = counterDict, conn = connection )
      if not res['OK']:
        return res

    return S_OK()

  def getTransformationsCounters( self, TransIDs, connection = False ):
    ''' Get all the counters for the given transformationIDs
    '''
    fields = ['TransformationID'] + self.tasksStatuses + self.fileStatuses
    res = self.getFields( "TransformationCounters",
                          outFields = fields, condDict = {'TransformationID' : TransIDs},
                          conn = connection )
    if not res['OK']:
      return res
    resList = []
    for row in res['Value']:
      resList.append( dict( zip( fields, row ) ) )

    return S_OK( resList )

  ###########################################################################
  #
  # These methods manipulate multiple tables
  #

  def addTaskForTransformation( self, transID, lfns = [], se = 'Unknown', connection = False ):
    ''' Create a new task with the supplied files for a transformation.
    '''
    res = self._getConnectionTransID( connection, transID )
    if not res['OK']:
      return res
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
    ''' Extend SIMULATION type transformation by nTasks number of tasks
    '''
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
    ''' Clean the transformation specified by name or id '''
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
    ''' Remove the transformation specified by name or id '''
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

  def __checkUpdate( self, table, param, paramValue, selectDict = {}, connection = False ):
    ''' Check whether the update will perform an update '''
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
    ''' Check the presence of the lfn in the TransformationDB DataFiles table
    '''
    gLogger.info( "TransformationDB.exists: Attempting to determine existence of %s files." % len( lfns ) )
    res = self.__getFileIDsForLfns( lfns, connection = connection )
    if not res['OK']:
      return res
    fileIDs, _lfnFilesIDs = res['Value']
    failed = {}
    successful = {}
    fileIDsValues = set( fileIDs.values() )
    for lfn in lfns:
      if not lfn in fileIDsValues:
        successful[lfn] = False
      else:
        successful[lfn] = True
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def addFile( self, fileDicts, force = False, connection = False ):
    """  Add a new file to the TransformationDB together with its first replica.
         In the input dict, the only mandatory info are PFN and SE
    """
    gLogger.info( "TransformationDB.addFile: Attempting to add %s files." % len( fileDicts.keys() ) )
    successful = {}
    failed = {}
    # Determine which files pass the filters and are to be added to transformations
    transFiles = {}
    filesToAdd = []
    for lfn in fileDicts.keys():
      fileTrans = self.__filterFile( lfn )
      if not ( fileTrans or force ):
        successful[lfn] = True
      else:
        filesToAdd.append( lfn )
        for trans in fileTrans:
          if not transFiles.has_key( trans ):
            transFiles[trans] = []
          transFiles[trans].append( lfn )
    # Add the files to the DataFiles and Replicas tables
    if filesToAdd:
      connection = self.__getConnection( connection )
      res = self.__addDataFiles( filesToAdd, connection = connection )
      if not res['OK']:
        return res
      lfnFileIDs = res['Value']
      for lfn in filesToAdd:
        if lfnFileIDs.has_key( lfn ):
          successful[lfn] = True
        else:
          failed[lfn] = True
      # Add the files to the transformations
      # TODO: THIS SHOULD BE TESTED WITH A TRANSFORMATION WITH A FILTER
      for transID, lfns in transFiles.items():
        fileIDs = []
        for lfn in lfns:
          if lfnFileIDs.has_key( lfn ):
            fileIDs.append( lfnFileIDs[lfn] )
        if fileIDs:
          res = self.__addFilesToTransformation( transID, fileIDs, connection = connection )
          if not res['OK']:
            gLogger.error( "Failed to add files to transformation", "%s %s" % ( transID, res['Message'] ) )
            failed[lfn] = True
            successful[lfn] = False
          else:
            successful[lfn] = True
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def removeFile( self, lfns, connection = False ):
    ''' Remove file specified by lfn from the ProcessingDB
    '''
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
      if not lfnFilesIDs.has_key( lfn ):
        successful[lfn] = 'File did not exist'
    if fileIDs:
      res = self.__setTransformationFileStatus( fileIDs.keys(), 'Deleted', connection = connection )
      if not res['OK']:
        return res
      res = self.__setDataFileStatus( fileIDs.keys(), 'Deleted', connection = connection )
      if not res['OK']:
        return S_ERROR( "TransformationDB.removeFile: Failed to remove files." )
    for lfn in lfnFilesIDs.keys():
      if not failed.has_key( lfn ):
        successful[lfn] = True
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def addDirectory( self, path, force = False ):
    ''' Adds all the files stored in a given directory in file catalog '''
    gLogger.info( "TransformationDB.addDirectory: Attempting to populate %s." % path )
    res = pythonCall( 30, self.__addDirectory, path, force )
    if not res['OK']:
      gLogger.error( "Failed to invoke addDirectory with shifter proxy" )
      return res
    return res['Value']

  def __addDirectory( self, path, force ):
    res = setupShifterProxyInEnv( "ProductionManager" )
    if not res['OK']:
      return S_OK( "Failed to setup shifter proxy" )
    catalog = FileCatalog()
    start = time.time()
    res = catalog.listDirectory( path )
    if not res['OK']:
      gLogger.error( "TransformationDB.addDirectory: Failed to get files. %s" % res['Message'] )
      return res
    if not path in res['Value']['Successful']:
      gLogger.error( "TransformationDB.addDirectory: Failed to get files." )
      return res
    gLogger.info( "TransformationDB.addDirectory: Obtained %s files in %s seconds." % ( path, time.time() - start ) )
    successful = []
    failed = []
    for lfn in res['Value']['Successful'][path]["Files"].keys():
      res = self.addFile( {lfn:{}}, force = force )
      if not res['OK']:
        failed.append( lfn )
        continue
      if not lfn in res['Value']['Successful']:
        failed.append( lfn )
      else:
        successful.append( lfn )
    return {"OK":True, "Value": len( res['Value']['Successful'] ), "Successful":successful, "Failed": failed }
