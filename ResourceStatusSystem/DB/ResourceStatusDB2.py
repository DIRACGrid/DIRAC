# $HeadURL $
''' ResourceStatusDB

  Module that provides basic methods to access the ResourceStatusDB.

'''

from DIRAC                                import S_OK, S_ERROR 
from DIRAC.Core.Base.DB                   import DB
from DIRAC.ResourceStatusSystem.Utilities import MySQLWrapper

__RCSID__ = '$Id: $'

class ResourceStatusDB2( object ):
  
  # Written PrimaryKey as list on purpose !!
  __tables = {}
  
  __tablesLike = {}
  __tablesLike[ 'ElementStatus' ]    = { 'Fields' : 
                    {
                     'Name'            : 'VARCHAR(64) NOT NULL',
                     'StatusType'      : 'VARCHAR(16) NOT NULL DEFAULT ""',
                     'Status'          : 'VARCHAR(8) NOT NULL',
                     'Reason'          : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                     'DateEffective'   : 'DATETIME NOT NULL',
                     'LastCheckTime'   : 'DATETIME NOT NULL',
                     'TokenOwner'      : 'VARCHAR(16) NOT NULL DEFAULT "RS_SVC"',
                     'TokenExpiration' : 'DATETIME NOT NULL'
                    },
                    'PrimaryKey' : [ 'Name', 'StatusType' ]              
                                    }
    
  __tablesLike[ 'ElementLog' ]       = { 'Fields' : 
                    {
                     'ID'              : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                     'Name'            : 'VARCHAR(64) NOT NULL',
                     'StatusType'      : 'VARCHAR(16) NOT NULL DEFAULT ""',
                     'Status'          : 'VARCHAR(8) NOT NULL',
                     'Reason'          : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                     'DateEffective'   : 'DATETIME NOT NULL',
                     'LastCheckTime'   : 'DATETIME NOT NULL',
                     'TokenOwner'      : 'VARCHAR(16) NOT NULL DEFAULT "RS_SVC"',
                     'TokenExpiration' : 'DATETIME NOT NULL'
                    },
                    'PrimaryKey' : [ 'ID' ]                
                                    }
  
  __tablesLike[ 'ElementScheduled' ] = { 'Fields' : 
                    {
                     'ID'              : 'INT UNSIGNED AUTO_INCREMENT NOT NULL',
                     'Name'            : 'VARCHAR(64) NOT NULL',
                     'StatusType'      : 'VARCHAR(16) NOT NULL DEFAULT ""',
                     'Status'          : 'VARCHAR(8) NOT NULL',
                     'Reason'          : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                     'DateEffective'   : 'DATETIME NOT NULL',
                     'LastCheckTime'   : 'DATETIME NOT NULL',
                     'TokenOwner'      : 'VARCHAR(16) NOT NULL DEFAULT "RS_SVC"',
                     'TokenExpiration' : 'DATETIME NOT NULL'
                    },
                    'PrimaryKey' : None                
                                    }

  __likeToTable = { 
                    'SiteStatus'        : 'ElementStatus',
                    'SiteLog'           : 'ElementLog',
                    'SiteHistory'       : 'ElementLog',
                    'SiteScheduled'     : 'ElementScheduled',
                    'ResourceStatus'    : 'ElementStatus',
                    'ResourceLog'       : 'ElementLog',
                    'ResourceHistory'   : 'ElementLog',
                    'ResourceScheduled' : 'ElementScheduled',
                    'NodeStatus'        : 'ElementStatus',
                    'NodeLog'           : 'ElementLog',
                    'NodeHistory'       : 'ElementLog',      
                    'NodeScheduled'     : 'ElementScheduled'             
                   }

# No idea whether they make sense or not
#  __tables[ 'ElementPresent' ]   = {} #????  
#  __tables[ 'Element' ]          = {} #????
  
  def __init__( self, maxQueueSize = 10, mySQL = None ):
    '''
      Constructor, accepts any DB or mySQL connection, mostly used for testing
      purposes.
    '''

    self.tables = self.__generateTables()
    
    if mySQL is not None:
      self.database = mySQL
    else:
      self.database = DB( 'ResourceStatusDB', 
                          'ResourceStatus/ResourceStatusDB', maxQueueSize )  

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

  def createTables( self, tableName = None ):
    '''
      Writes the schema in the database. If no tableName is given, all tables
      are written in the database.
    '''

    tables = []
    if tableName is None:
      tables = self.getTablesList()
    
    elif tableName in self.tables:
      tables = [ tableName ]
      
    return self.database._createTables( tables )  

  def getTable( self, tableName ):
    '''
      Returns a table dictionary description given its name 
    '''
    if tableName in self.tables:
      return S_OK( self.tables[ tableName ] )
    
    return S_ERROR( '%s is not on the schema' % tableName )
    
  def getTablesList( self ):
    '''
      Returns a list of the table names in the schema.
    '''
    return S_OK( self.tables.keys() )

  ## Private methods ###########################################################

  def __generateTables( self ):
    '''
      Method used to transform the class variables into instance variables,
      for safety reasons.
    '''
  
    tables = self.__tables
    
    for tableName, tableLike in self.__likeToTable.items():
      
      tables[ tableName ] = self.__tablesLike[ tableLike ]
       
    return tables
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF   