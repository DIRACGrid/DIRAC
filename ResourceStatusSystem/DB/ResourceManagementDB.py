# $HeadURL:  $
''' ResourceManagementDB

  Module that provides basic methods to access the ResourceManagementDB.

'''

from datetime                             import datetime

from DIRAC                                import S_OK, S_ERROR 
from DIRAC.Core.Base.DB                   import DB
from DIRAC.ResourceStatusSystem.Utilities import MySQLWrapper

__RCSID__ = '$Id: $'

class ResourceManagementDB( object ):
  '''
    Class that defines the tables for the ResourceManagementDB on a python dictionary.
  '''

  # Written PrimaryKey as list on purpose !!
  _tablesDB = {}
  _tablesDB[ 'AccountingCache' ] = { 'Fields' : 
                     {
                       #'AccountingCacheID' : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'PlotType'      : 'VARCHAR(16) NOT NULL',
                       'PlotName'      : 'VARCHAR(64) NOT NULL',                     
                       'Result'        : 'TEXT NOT NULL',
                       'DateEffective' : 'DATETIME NOT NULL',
                       'LastCheckTime' : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'Name', 'PlotType', 'PlotName' ]                                            
                                }

  _tablesDB[ 'DowntimeCache' ] = { 'Fields' :
                      {
                       'DowntimeID'    : 'VARCHAR(64) NOT NULL',
                       'Element'       : 'VARCHAR(32) NOT NULL',
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'StartDate'     : 'DATETIME NOT NULL',
                       'EndDate'       : 'DATETIME NOT NULL',
                       'Severity'      : 'VARCHAR(32) NOT NULL',
                       'Description'   : 'VARCHAR(512) NOT NULL',
                       'Link'          : 'VARCHAR(255) NOT NULL',       
                       'DateEffective' : 'DATETIME NOT NULL',
                       'LastCheckTime' : 'DATETIME NOT NULL'     
                      },
                      'PrimaryKey' : [ 'DowntimeID' ]
                                }

  _tablesDB[ 'GGUSTicketsCache' ] = { 'Fields' :
                      {
                       'GocSite'       : 'VARCHAR(64) NOT NULL',
                       'Link'          : 'VARCHAR(1024) NOT NULL',    
                       'OpenTickets'   : 'INTEGER NOT NULL DEFAULT 0',
                       'Tickets'       : 'VARCHAR(1024) NOT NULL',
                       'LastCheckTime' : 'DATETIME NOT NULL'                       
                      },
                      'PrimaryKey' : [ 'GocSite' ]
                                }


  _tablesDB[ 'JobCache' ] = { 'Fields' :
                      {
                       'Site'          : 'VARCHAR(64) NOT NULL',
                       'Timespan'      : 'INTEGER NOT NULL',
                       'Checking'      : 'INTEGER NOT NULL DEFAULT 0',
                       'Completed'     : 'INTEGER NOT NULL DEFAULT 0',
                       'Done'          : 'INTEGER NOT NULL DEFAULT 0',
                       'Failed'        : 'INTEGER NOT NULL DEFAULT 0',
                       'Killed'        : 'INTEGER NOT NULL DEFAULT 0',
                       'Matched'       : 'INTEGER NOT NULL DEFAULT 0',
                       'Received'      : 'INTEGER NOT NULL DEFAULT 0',
                       'Running'       : 'INTEGER NOT NULL DEFAULT 0',                       
                       'Staging'       : 'INTEGER NOT NULL DEFAULT 0',
                       'Stalled'       : 'INTEGER NOT NULL DEFAULT 0',
                       'Waiting'       : 'INTEGER NOT NULL DEFAULT 0',
                       'LastCheckTime' : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'Site', 'Timespan' ]
                                }

  _tablesDB[ 'PilotCache' ] = { 'Fields' :
                      {
                       'CE'            : 'VARCHAR(64) NOT NULL',
                       'Timespan'      : 'INTEGER NOT NULL',
                       'Scheduled'     : 'INTEGER NOT NULL DEFAULT 0',
                       'Waiting'       : 'INTEGER NOT NULL DEFAULT 0',
                       'Submitted'     : 'INTEGER NOT NULL DEFAULT 0',
                       'Running'       : 'INTEGER NOT NULL DEFAULT 0',
                       'Done'          : 'INTEGER NOT NULL DEFAULT 0',
                       'Aborted'       : 'INTEGER NOT NULL DEFAULT 0',
                       'Cancelled'     : 'INTEGER NOT NULL DEFAULT 0',
                       'Deleted'       : 'INTEGER NOT NULL DEFAULT 0',
                       'Failed'        : 'INTEGER NOT NULL DEFAULT 0',
                       'Held'          : 'INTEGER NOT NULL DEFAULT 0',
                       'Killed'        : 'INTEGER NOT NULL DEFAULT 0',
                       'Stalled'       : 'INTEGER NOT NULL DEFAULT 0',
                       'LastCheckTime' : 'DATETIME NOT NULL'                                    
                      },
                      'PrimaryKey' : [ 'CE', 'Timespan' ]
                                }

  _tablesDB[ 'PolicyResult' ] = { 'Fields' : 
                      {
                       'Element'       : 'VARCHAR(32) NOT NULL',
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'PolicyName'    : 'VARCHAR(64) NOT NULL',
                       'StatusType'    : 'VARCHAR(16) NOT NULL DEFAULT ""',
                       'Status'        : 'VARCHAR(16) NOT NULL',
                       'Reason'        : 'VARCHAR(512) NOT NULL DEFAULT "Unspecified"',
                       'DateEffective' : 'DATETIME NOT NULL',
                       'LastCheckTime' : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'Element', 'Name', 'StatusType', 'PolicyName' ] 
                                }

  _tablesDB[ 'SpaceTokenOccupancyCache' ] = { 'Fields' :
                      {
                       'Endpoint'       : 'VARCHAR( 64 ) NOT NULL',
                       'Token'          : 'VARCHAR( 64 ) NOT NULL',
                       'Total'          : 'DOUBLE NOT NULL DEFAULT 0',                      
                       'Guaranteed'     : 'DOUBLE NOT NULL DEFAULT 0',
                       'Free'           : 'DOUBLE NOT NULL DEFAULT 0',                     
                       'LastCheckTime'  : 'DATETIME NOT NULL' 
                      },
                      'PrimaryKey' : [ 'Endpoint', 'Token' ]                                             
                                } 

  _tablesDB[ 'TransferCache' ] = { 'Fields' :
                      {
                       'SourceName'      : 'VARCHAR( 64 ) NOT NULL',
                       'DestinationName' : 'VARCHAR( 64 ) NOT NULL',
                       'Metric'          : 'VARCHAR( 16 ) NOT NULL',
                       'Value'           : 'DOUBLE NOT NULL DEFAULT 0',                     
                       'LastCheckTime'   : 'DATETIME NOT NULL' 
                      },
                      'PrimaryKey' : [ 'SourceName', 'DestinationName', 'Metric' ]                                             
                                } 
 
  _tablesDB[ 'UserRegistryCache' ] = { 'Fields' : 
                      {
                       'Login'         : 'VARCHAR(16)',
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'Email'         : 'VARCHAR(64) NOT NULL',
                       'LastCheckTime' : 'DATETIME NOT NULL'  
                      },
                      'PrimaryKey' : [ 'Login' ]           
                                }   

  _tablesDB[ 'VOBOXCache' ] = { 'Fields' :
                      {
                       'Site'          : 'VARCHAR( 64 ) NOT NULL',
                       'System'        : 'VARCHAR( 64 ) NOT NULL',
                       'ServiceUp'     : 'INTEGER NOT NULL DEFAULT 0',
                       'MachineUp'     : 'INTEGER NOT NULL DEFAULT 0',
                       'LastCheckTime' : 'DATETIME NOT NULL'                                            
                      },        
                      'PrimaryKey' : [ 'Site', 'System' ]        
                                }
  
  _tablesDB[ 'ErrorReportBuffer' ] = { 'Fields' : 
                      {
                       'ID'            : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'ElementType'   : 'VARCHAR(32) NOT NULL',
                       'Reporter'      : 'VARCHAR(64) NOT NULL',
                       'ErrorMessage'  : 'VARCHAR(512) NOT NULL',
                       'Operation'     : 'VARCHAR(64) NOT NULL',
                       'Arguments'     : 'VARCHAR(512) NOT NULL DEFAULT ""',
                       'DateEffective' : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'ID' ]
                                }  
  
  _tablesLike  = {}
  _tablesLike[ 'PolicyResultWithID' ] = { 'Fields' : 
                      {
                       'ID'            : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                       'Element'       : 'VARCHAR(32) NOT NULL',
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'PolicyName'    : 'VARCHAR(64) NOT NULL',
                       'StatusType'    : 'VARCHAR(16) NOT NULL DEFAULT ""',
                       'Status'        : 'VARCHAR(8) NOT NULL',
                       'Reason'        : 'VARCHAR(512) NOT NULL DEFAULT "Unspecified"',
                       'DateEffective' : 'DATETIME NOT NULL',                       
                       'LastCheckTime' : 'DATETIME NOT NULL'                                   
                      },
                      'PrimaryKey' : [ 'ID' ]
                                }
  _likeToTable = {
                   'PolicyResultLog'     : 'PolicyResultWithID',
                   'PolicyResultHistory' : 'PolicyResultWithID',
                  }
  
  def __init__( self, maxQueueSize = 10, mySQL = None ):
    '''
      Constructor, accepts any DB or mySQL connection, mostly used for testing
      purposes.
    '''
    self._tableDict = self.__generateTables()
    
    if mySQL is not None:
      self.database = mySQL
    else:
      self.database = DB( 'ResourceManagementDB', 
                          'ResourceStatus/ResourceManagementDB', maxQueueSize )

  ## SQL Methods ############################################################### 
      
  def insert( self, params, meta ):
    '''
    Inserts args in the DB making use of kwargs where parameters such as
    the 'table' are specified ( filled automatically by the Client). Typically you 
    will not pass kwargs to this function, unless you know what are you doing 
    and you have a very special use case.

    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).

      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''

    utcnow = datetime.utcnow().replace( microsecond = 0 )

    # We force lastCheckTime to utcnow if it is not present on the params
    #if not( 'lastCheckTime' in params and not( params[ 'lastCheckTime' ] is None ) ):
    if 'lastCheckTime' in params and params[ 'lastCheckTime' ] is None:  
      params[ 'lastCheckTime' ] = utcnow  

    if 'dateEffective' in params and params[ 'dateEffective' ] is None:
      params[ 'dateEffective' ] = utcnow
    
    return MySQLWrapper.insert( self, params, meta )

  def update( self, params, meta ):
    '''
    Updates row with values given on args. The row selection is done using the
    default of MySQLMonkey ( column.primary or column.keyColumn ). It can be
    modified using kwargs. The 'table' keyword argument is mandatory, and 
    filled automatically by the Client. Typically you will not pass kwargs to 
    this function, unless you know what are you doing and you have a very 
    special use case.

    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).

      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    
    # We force lastCheckTime to utcnow if it is not present on the params
    #if not( 'lastCheckTime' in params and not( params[ 'lastCheckTime' ] is None ) ):
    if 'lastCheckTime' in params and params[ 'lastCheckTime' ] is None:      
      params[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 ) 
    
    return MySQLWrapper.update( self, params, meta )

  def select( self, params, meta ):
    '''
    Uses arguments to build conditional SQL statement ( WHERE ... ). If the
    sql statement desired is more complex, you can use kwargs to interact with
    the MySQL buildCondition parser and generate a more sophisticated query.

    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).

      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return MySQLWrapper.select( self, params, meta )

  def delete( self, params, meta ):
    '''
    Uses arguments to build conditional SQL statement ( WHERE ... ). If the
    sql statement desired is more complex, you can use kwargs to interact with
    the MySQL buildCondition parser and generate a more sophisticated query. 
    There is only one forbidden query, with all parameters None ( this would 
    mean a query of the type `DELETE * from TableName` ). The usage of kwargs 
    is the same as in the get function.

    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).

      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return MySQLWrapper.delete( self, params, meta )

  ## Extended SQL methods ######################################################
  
  def addOrModify( self, params, meta ):
    '''
    Using the PrimaryKeys of the table, it looks for the record in the database.
    If it is there, it is updated, if not, it is inserted as a new entry. 
    
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).

      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''
        
    selectQuery = self.select( params, meta )
    if not selectQuery[ 'OK' ]:
      return selectQuery 
    
    isUpdate = False
              
    if selectQuery[ 'Value' ]:      
      
      # Pseudo - code
      # for all column not being PrimaryKey and not a time column: 
      #   if one or more column different than params if not None:
      #     we update dateTime as well
      
      columns = selectQuery[ 'Columns' ]
      values  = selectQuery[ 'Value' ]
      
      if len( values ) != 1:
        return S_ERROR( 'More than one value returned on addOrModify, please report !!' )

      selectDict = dict( zip( columns, values[ 0 ] ) )
      
      newDateEffective = None
      
      for key, value in params.items():
        if key in ( 'lastCheckTime', 'dateEffective' ):
          continue
        
        if value is None:
          continue
        
        if value != selectDict[ key[0].upper() + key[1:] ]:
          newDateEffective = datetime.utcnow().replace( microsecond = 0 )   
          break
      
      if 'dateEffective' in params:
        params[ 'dateEffective' ] = newDateEffective              
      
      userQuery = self.update( params, meta )
      isUpdate  = True
      
    else:      
      userQuery = self.insert( params, meta )

    # This part only applies to PolicyResult table
    logResult = self._logRecord( params, meta, isUpdate )
    if not logResult[ 'OK' ]:
      return logResult
    
    return userQuery      

  # FIXME: this method looks unused. Maybe can be removed from the code.
  def addIfNotThere( self, params, meta ):
    '''
    Using the PrimaryKeys of the table, it looks for the record in the database.
    If it is not there, it is inserted as a new entry. 
    
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).

      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''
        
    selectQuery = self.select( params, meta )
    if not selectQuery[ 'OK' ]:
      return selectQuery 
       
    if selectQuery[ 'Value' ]:      
      return selectQuery
    
    return self.insert( params, meta )   

  ## Auxiliar methods ##########################################################

  def getTable( self, tableName ):
    '''
      Returns a table dictionary description given its name 
    '''
    if tableName in self._tableDict:
      return S_OK( self._tableDict[ tableName ] )
    
    return S_ERROR( '%s is not on the schema' % tableName )
    
  def getTablesList( self ):
    '''
      Returns a list of the table names in the schema.
    '''
    return S_OK( self._tableDict.keys() )

  ## Protected methods #########################################################

  def _checkTable( self ):
    '''
      Method used by database tools to write the schema
    '''  
    return self.__createTables()

  def _logRecord( self, params, meta, isUpdate ):
    '''
      Method that records every change on a LogTable.
    '''
  
    if not ( 'table' in meta and meta[ 'table' ] == 'PolicyResult' ):
      return S_OK()
        
    if isUpdate:
      
      # This looks little bit like a non-sense. If we were updating, we may have
      # not passed a complete set of parameters, so we have to get all them from the
      # database :/. It costs us one more query.
      updateRes = self.select( params, meta )
      if not updateRes[ 'OK' ]:
        return updateRes
                    
      params = dict( zip( updateRes[ 'Columns' ], updateRes[ 'Value' ][ 0 ] )) 

    # Writes to PolicyResult"Log"                
    meta[ 'table' ] += 'Log'

    logRes = self.insert( params, meta )
    
    return logRes

  ## Private methods ###########################################################

  def __createTables( self, tableName = None ):
    '''
      Writes the schema in the database. If no tableName is given, all tables
      are written in the database. If a table is already in the schema, it is
      skipped to avoid problems trying to create a table that already exists.
    '''

    # Horrible SQL here !!
    tablesCreatedRes = self.database._query( "show tables" )
    if not tablesCreatedRes[ 'OK' ]:
      return tablesCreatedRes
    tablesCreated = [ tableCreated[0] for tableCreated in tablesCreatedRes[ 'Value' ] ]

    tables = {}
    if tableName is None:
      tables.update( self._tableDict )
    
    elif tableName in self._tableDict:
      tables = { tableName : self._tableDict[ tableName ] }
    
    else:
      return S_ERROR( '"%s" is not a known table' % tableName )    
      
    for tableName in tablesCreated:
      if tableName in tables:
        del tables[ tableName ]  
              
    res = self.database._createTables( tables )
    if not res[ 'OK' ]:
      return res
    
    # Human readable S_OK message
    if res[ 'Value' ] == 0:
      res[ 'Value' ] = 'No tables created'
    else:
      res[ 'Value' ] = 'Tables created: %s' % ( ','.join( tables.keys() ) )
    return res      
  
  def __generateTables( self ):
    '''
      Method used to transform the class variables into instance variables,
      for safety reasons.
    '''
  
    # Avoids copying object.
    tables = {}
    tables.update( self._tablesDB )
    
    for tableName, tableLike in self._likeToTable.items():
      
      tables[ tableName ] = self._tablesLike[ tableLike ]
       
    return tables

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF