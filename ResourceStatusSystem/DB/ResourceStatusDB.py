# $HeadURL $
''' ResourceStatusDB

  Module that provides basic methods to access the ResourceStatusDB.

'''

from datetime                                              import datetime 

from DIRAC                                                 import S_OK, S_ERROR 
from DIRAC.Core.Base.DB                                    import DB
from DIRAC.ResourceStatusSystem.Utilities                  import MySQLWrapper

__RCSID__ = '$Id: $'

class ResourceStatusDB( object ):
  '''
    Class that defines the tables for the ResourceStatusDB on a python dictionary.
  '''
  
  # Written PrimaryKey as list on purpose !!
  _tablesDB = {}
  
  _tablesLike = {}
  _tablesLike[ 'ElementStatus' ]    = { 'Fields' : 
                    {
                     'Name'            : 'VARCHAR(64) NOT NULL',
                     'StatusType'      : 'VARCHAR(128) NOT NULL DEFAULT "all"',
                     'Status'          : 'VARCHAR(8) NOT NULL DEFAULT ""',
                     'ElementType'     : 'VARCHAR(32) NOT NULL DEFAULT ""',
                     'Reason'          : 'VARCHAR(512) NOT NULL DEFAULT "Unspecified"',
                     'DateEffective'   : 'DATETIME NOT NULL',
                     'LastCheckTime'   : 'DATETIME NOT NULL DEFAULT "1000-01-01 00:00:00"',
                     'TokenOwner'      : 'VARCHAR(16) NOT NULL DEFAULT "rs_svc"',
                     'TokenExpiration' : 'DATETIME NOT NULL DEFAULT "9999-12-31 23:59:59"'
                    },
                    #FIXME: elementType is needed to be part of the key ??
                    'PrimaryKey' : [ 'Name', 'StatusType' ]#, 'ElementType' ]              
                                    }
    
  _tablesLike[ 'ElementWithID' ]       = { 'Fields' : 
                    {
                     'ID'              : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                     'Name'            : 'VARCHAR(64) NOT NULL',
                     'StatusType'      : 'VARCHAR(128) NOT NULL DEFAULT "all"',
                     'Status'          : 'VARCHAR(8) NOT NULL DEFAULT ""',
                     'ElementType'     : 'VARCHAR(32) NOT NULL DEFAULT ""',
                     'Reason'          : 'VARCHAR(512) NOT NULL DEFAULT "Unspecified"',
                     'DateEffective'   : 'DATETIME NOT NULL',
                     'LastCheckTime'   : 'DATETIME NOT NULL DEFAULT "1000-01-01 00:00:00"',
                     'TokenOwner'      : 'VARCHAR(16) NOT NULL DEFAULT "rs_svc"',
                     'TokenExpiration' : 'DATETIME NOT NULL DEFAULT "9999-12-31 23:59:59"'
                    },
                    'PrimaryKey' : [ 'ID' ]                
                                    }

  _likeToTable = { 
                    'SiteStatus'        : 'ElementStatus',
                    'SiteLog'           : 'ElementWithID',
                    'SiteHistory'       : 'ElementWithID',
                    'ResourceStatus'    : 'ElementStatus',
                    'ResourceLog'       : 'ElementWithID',
                    'ResourceHistory'   : 'ElementWithID',
                    'NodeStatus'        : 'ElementStatus',
                    'NodeLog'           : 'ElementWithID',
                    'NodeHistory'       : 'ElementWithID',
                    'ComponentStatus'   : 'ElementStatus',
                    'ComponentLog'      : 'ElementWithID',
                    'ComponentHistory'  : 'ElementWithID',           
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
   
    
    utcnow = datetime.utcnow().replace( microsecond = 0 )
    # We force lastCheckTime to utcnow if it is not present on the params
    #if not( 'lastCheckTime' in params and not( params[ 'lastCheckTime' ] is None ) ):
    if 'lastCheckTime' in params and params[ 'lastCheckTime' ] is None:  
      params[ 'lastCheckTime' ] = utcnow
    
    # If it is a XStatus table, we force dateEffective to now.
    if 'table' in meta and meta[ 'table' ].endswith( 'Status' ):
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
    if not( 'lastCheckTime' in params and not( params[ 'lastCheckTime' ] is None ) ):
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
      
      userQuery  = self.update( params, meta )
      isUpdate   = True
    else:      
      userQuery = self.insert( params, meta )
    
    logResult = self._logRecord( params, meta, isUpdate )
    if not logResult[ 'OK' ]:
      return logResult
    
    return userQuery      

  def modify( self, params, meta ):
    '''
    Using the PrimaryKeys of the table, it looks for the record in the database.
    If it is there, it is updated, if not, it does nothing. 
    
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
             
    if not selectQuery[ 'Value' ]:
      return S_ERROR( 'Nothing to update for %s' % str( params ) )      
      
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
    
    logResult = self._logRecord( params, meta, True )
    if not logResult[ 'OK' ]:
      return logResult
    
    return userQuery

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
    
    insertQuery = self.insert( params, meta )  
  
    # Record logs     
    if 'table' in meta and meta[ 'table' ].endswith( 'Status' ):
                
      meta[ 'table' ] = meta[ 'table' ].replace( 'Status', 'Log' )

      logRes = self.insert( params, meta )
      if not logRes[ 'OK' ]:
        return logRes  
        
    return insertQuery        
      
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
      
    if not ( 'table' in meta and meta[ 'table' ].endswith( 'Status' ) ):
      return S_OK()
        
    if isUpdate:
      updateRes = self.select( params, meta )
      if not updateRes[ 'OK' ]:
        return updateRes
          
      # If we are updating more that one result at a time, this is most likely
      # going to be a mess. All queries must be one at a time, if need to do
      if len( updateRes[ 'Value' ] ) != 1:
        return S_ERROR( ' PLEASE REPORT to developers !!: %s, %s' % ( params, meta ) )
      if len( updateRes[ 'Value' ][ 0 ] ) != len( updateRes[ 'Columns' ] ):
        # Uyyy, something went seriously wrong !!
        return S_ERROR( ' PLEASE REPORT to developers !!: %s' % updateRes )
                    
      params = dict( zip( updateRes['Columns'], updateRes[ 'Value' ][0] )) 
                
    meta[ 'table' ] = meta[ 'table' ].replace( 'Status', 'Log' )

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