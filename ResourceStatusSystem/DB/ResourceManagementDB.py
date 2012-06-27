# $HeadURL $
''' ResourceManagementDB

  Module that provides basic methods to access the ResourceManagementDB.

'''

from DIRAC                                import S_OK, S_ERROR 
from DIRAC.Core.Base.DB                   import DB
from DIRAC.ResourceStatusSystem.Utilities import MySQLWrapper

__RCSID__ = '$Id: $'

class ResourceManagementDB( object ):

  # Written PrimaryKey as list on purpose !!
  __tablesDB = {}
  __tablesDB[ 'AccountingCache' ] = { 'Fields' : 
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

  __tablesDB[ 'ClientCache' ] = { 'Fields' :
                      {
                       #'ClientCacheID' : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'CommandName'   : 'VARCHAR(64) NOT NULL',
                       'Opt_ID'        : 'VARCHAR(64)',
                       'Value'         : 'VARCHAR(16) NOT NULL',
                       'Result'        : 'VARCHAR(255) NOT NULL',        
                       'DateEffective' : 'DATETIME NOT NULL',
                       'LastCheckTime' : 'DATETIME NOT NULL'     
                      },
                      'PrimaryKey' : [ 'Name', 'CommandName', 'Value' ]
                                }

  __tablesDB[ 'PolicyResult' ] = { 'Fields' : 
                      {
                       #'PolicyResultID' : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                       'Granularity'   : 'VARCHAR(32) NOT NULL',
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'PolicyName'    : 'VARCHAR(64) NOT NULL',
                       'StatusType'    : 'VARCHAR(16) NOT NULL DEFAULT ""',
                       'Status'        : 'VARCHAR(16) NOT NULL',
                       'Reason'        : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                       'DateEffective' : 'DATETIME NOT NULL',
                       'LastCheckTime' : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'Name', 'StatusType', 'PolicyName' ] 
                                }
  
  __tablesDB[ 'PolicyResultLog' ] = { 'Fields' : 
                      {
                       'PolicyResultLogID' : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                       'Granularity'       : 'VARCHAR(32) NOT NULL',
                       'Name'              : 'VARCHAR(64) NOT NULL',
                       'PolicyName'        : 'VARCHAR(64) NOT NULL',
                       'StatusType'        : 'VARCHAR(16) NOT NULL DEFAULT ""',
                       'Status'            : 'VARCHAR(8) NOT NULL',
                       'Reason'            : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                       'LastCheckTime'     : 'DATETIME NOT NULL'                                   
                      },
                      'PrimaryKey' : [ 'PolicyResultLogID' ]
                                }

  __tablesDB[ 'SpaceTokenOccupancyCache' ] = { 'Fields' :
                      {
                       #'SpaceTokenOccupancyCacheID' : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                       'Site'          : 'VARCHAR( 64 ) NOT NULL',
                       'Token'         : 'VARCHAR( 64 ) NOT NULL',
                       'Total'         : 'INTEGER NOT NULL DEFAULT 0',                      
                       'Guaranteed'    : 'INTEGER NOT NULL DEFAULT 0',
                       'Free'          : 'INTEGER NOT NULL DEFAULT 0',                     
                       'LastCheckTime' : 'DATETIME NOT NULL' 
                      },
                      'PrimaryKey' : [ 'Site', 'Token' ]                                             
                                } 
 
  __tablesDB[ 'UserRegistryCache' ] = { 'Fields' : 
                      {
                       'Login' : 'VARCHAR(16)',
                       'Name'  : 'VARCHAR(64) NOT NULL',
                       'Email' : 'VARCHAR(64) NOT NULL' 
                      },
                      'PrimaryKey' : [ 'Login' ]           
                                }   

  __tablesDB[ 'VOBOXCache' ] = { 'Fields' :
                      {
                       #'VOBOXCacheID'  : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                       'Site'          : 'VARCHAR( 64 ) NOT NULL',
                       'System'        : 'VARCHAR( 64 ) NOT NULL',
                       'ServiceUp'     : 'INTEGER NOT NULL DEFAULT 0',
                       'MachineUp'     : 'INTEGER NOT NULL DEFAULT 0',
                       'LastCheckTime' : 'DATETIME NOT NULL'                                            
                      },        
                      'PrimaryKey' : [ 'Site', 'System' ]        
                                }
  
  __tablesLike  = {}
  __likeToTable = {}
  
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
    return MySQLWrapper.insert( self.database, params, meta )

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
    return MySQLWrapper.update( self.database, params, meta )

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
    return MySQLWrapper.select( self.database, params, meta )

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
    return MySQLWrapper.delete( self.database, params, meta )

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
    tables.update( self.__tablesDB )
    
    for tableName, tableLike in self.__likeToTable.items():
      
      tables[ tableName ] = self.__tablesLike[ tableLike ]
       
    return tables

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF