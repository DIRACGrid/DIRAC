# $HeadURL $
''' MySQLWrapper

  Module that provides functions needed for RSS databases and have not yet
  been incorporated to the DIRAC MySQL module.

'''

from DIRAC import S_ERROR

__RCSID__ = '$Id: $'
     
def insert( rssDB, params, meta ):
  '''
    Method that transforms the RSS DB insert into the MySQL insertFields 
    method.
  '''

  accepted_keys = [ 'table' ]
    
  # Protection to avoid misunderstandings between MySQLMonkey and new code.
  if set( meta.keys() ) - set( accepted_keys ):
    return S_ERROR( 'Insert statement only accepts %s, got %s' % ( accepted_keys, meta.keys() ) )
    
  tableName  = meta[ 'table' ]
  tablesList = rssDB.getTablesList()
  if not tablesList[ 'OK' ]:
    return tablesList
  
  if not tableName in tablesList[ 'Value' ]:
    return S_ERROR( '"%s" is not on the schema tables' )
    
  return rssDB.database.insertFields( tableName, inDict = params )    
  
def update( rssDB, params, meta ):
  '''
    Method that transforms the RSS DB update into the MySQL updateFields 
    method.
  '''

  accepted_keys = [ 'table', 'uniqueKeys' ]

  # Protection to avoid misunderstandings between MySQLMonkey and new code.
  if set( meta.keys() ) - set( accepted_keys ):
    return S_ERROR( 'Update statement only accepts %s, got %s' % ( accepted_keys, meta.keys() ) )
    
  tableName  = meta[ 'table' ]
  tablesList = rssDB.getTablesList()
  if not tablesList[ 'OK' ]:
    return tablesList
  
  if not tableName in tablesList[ 'Value' ]:
    return S_ERROR( '"%s" is not on the schema tables' )
   
  if 'uniqueKeys' in meta:
    uniqueKeys = meta[ 'uniqueKeys' ]
  else:
    res = rssDB.getTable( tableName )
    if not res[ 'OK' ]:
      return res
    uniqueKeys = res[ 'Value' ][ 'PrimaryKey' ]  
  
  # Little bit messy, but we split the fields that will be used to select and
  # which ones will be updated.
  paramsToUpdate = [ ( key, value ) for ( key, value ) in params.items() if key not in uniqueKeys ] 
  paramsToUpdate = dict( paramsToUpdate )
  
  paramsToSelect = [ ( key, value ) for ( key, value ) in params.items() if key in uniqueKeys ]
  paramsToSelect = dict( paramsToSelect )
  
  return rssDB.database.updateFields( tableName, condDict = paramsToSelect, 
                                      updateDict = paramsToUpdate )  
  
def select( rssDB, params, meta ):
  '''
    Method that transforms the RSS DB select into the MySQL getFields method.
  '''

  accepted_keys = [ 'table', 'columns', 'order', 'limit', 'onlyUniqueKeys' ]

  # Protection to avoid misunderstandings between MySQLMonkey and new code.
  if set( meta.keys() ) - set( accepted_keys ):
    return S_ERROR( 'Select statement only accepts %s, got %s' % ( accepted_keys, meta.keys() ) )

  tableName  = meta[ 'table' ]
  tablesList = rssDB.getTablesList()
  if not tablesList[ 'OK' ]:
    return tablesList
  
  if not tableName in tablesList[ 'Value' ]:
    return S_ERROR( '"%s" is not on the schema tables' )
  
  outFields, limit, order = None, None, None
  if 'columns' in meta:
    outFields = meta[ 'columns' ]
  if 'limit' in meta:
    limit     = meta[ 'limit' ]  
  if 'order' in meta:
    order     = meta[ 'order' ]
  if 'onlyUniqueKeys' in meta:
    
    tableDefinition = rssDB.getTable( tableName )
    if not tableDefinition[ 'OK' ]:
      return tableDefinition
    keys = tableDefinition[ 'Value' ][ 'PrimaryKey' ]
    
    newParams = {}
    for key in keys:
      if key in params:
        newParams[ key ] = params[ key ]
         
    params = newParams   
      
  return rssDB.database.getFields( tableName, condDict = params, 
                                   outFields = outFields, limit = limit,
                                   orderAttribute = order )
      
def delete( rssDB, params, meta ):
  '''
    Method that transforms the RSS DB delete into the MySQL 
  '''
    
  accepted_keys = [ 'table' ]

  # Protection to avoid misunderstandings between MySQLMonkey and new code.
  if set( meta.keys() ) - set( accepted_keys ):
    return S_ERROR( 'Delete statement only accepts %s, got %s' % ( accepted_keys, meta.keys() ) )

  tableName  = meta[ 'table' ]
  tablesList = rssDB.getTablesList()
  if not tablesList[ 'OK' ]:
    return tablesList
  
  if not tableName in tablesList[ 'Value' ]:
    return S_ERROR( '"%s" is not on the schema tables' )

  return rssDB.database.deleteEntries( tableName, condDict = params )
      
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  