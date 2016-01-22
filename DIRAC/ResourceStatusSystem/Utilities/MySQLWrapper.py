# $HeadURL $
''' MySQLWrapper

  Module that provides functions needed for RSS databases and have not yet
  been incorporated to the DIRAC MySQL module.

'''

from DIRAC import S_ERROR

__RCSID__ = '$Id: $'

def _capitalize( params ):
  '''
    Capitalize first letter of all keys in dictionary
  '''
  
  capitalizedParams = {}
  for key, value in params.items():
    capitalizedParams[ key[0].upper() + key[1:] ] = value
  
  return capitalizedParams  

def _discardNones( params ):
  '''
    Remove all keys with None as value in the key,value pair
  '''
     
  validParams = {}
  for key, value in params.items():
    if value is not None:
      validParams[ key ] = value
      
  return validParams       
     
def insert( rssDB, params, meta ):
  '''
    Method that transforms the RSS DB insert into the MySQL insertFields 
    method.
  '''
  
  # onlyUniqueKeys is not used, but if deleted here, addOrModify method crashes
  accepted_keys = ( 'table', 'onlyUniqueKeys' )

  params = _capitalize( params )
  params = _discardNones( params )
    
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

  accepted_keys = ( 'table', 'onlyUniqueKeys', 'uniqueKeys' )

  params = _capitalize( params )
  params = _discardNones( params )

  # Protection to avoid misunderstandings between MySQLMonkey and new code.
  if set( meta.keys() ) - set( accepted_keys ):
    return S_ERROR( 'Update statement only accepts %s, got %s' % ( accepted_keys, meta.keys() ) )
    
  tableName  = meta[ 'table' ]
  tablesList = rssDB.getTablesList()
  if not tablesList[ 'OK' ]:
    return tablesList
  
  if not tableName in tablesList[ 'Value' ]:
    return S_ERROR( '"%s" is not on the schema tables' % tableName )
   
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

  accepted_keys = ( 'table', 'columns', 'order', 'limit', 'onlyUniqueKeys', 'older', 'newer' )

  params = _capitalize( params )
  params = _discardNones( params )

  # Protection to avoid misunderstandings between MySQLMonkey and new code.
  if set( meta.keys() ) - set( accepted_keys ):
    return S_ERROR( 'Select statement only accepts %s, got %s' % ( accepted_keys, meta.keys() ) )

  tableName  = meta[ 'table' ]
  tablesList = rssDB.getTablesList()
  
  if not tableName in tablesList[ 'Value' ]:
    return S_ERROR( '"%s" is not on the schema tables' )
  
  outFields, limit, order = None, None, None
  if 'columns' in meta:
    outFields = meta[ 'columns' ]
  else:
    tableDefinition = rssDB.getTable( tableName )
    if not tableDefinition[ 'OK' ]:
      return tableDefinition
    outFields = tableDefinition[ 'Value' ][ 'Fields' ].keys()
      
  if 'limit' in meta:
    limit     = meta[ 'limit' ]  
  if 'order' in meta:
    order     = meta[ 'order' ]
    
  keys = []
    
  if 'uniqueKeys' in meta:
    keys = meta[ 'uniqueKeys' ]
  elif 'onlyUniqueKeys' in meta:   
    tableDefinition = rssDB.getTable( tableName )
    if not tableDefinition[ 'OK' ]:
      return tableDefinition
    keys = tableDefinition[ 'Value' ][ 'PrimaryKey' ]
    
  if keys:  
    newParams = {}
    for key in keys:
      if key in params:
        newParams[ key ] = params[ key ]
         
    params = newParams   
  
  field, older, newer = None, None, None
  if 'older' in meta:
    field, older = meta[ 'older' ]     
  elif 'newer' in meta:
    field, newer = meta[ 'newer' ]   
     
  selectResult = rssDB.database.getFields( tableName, condDict = params, 
                                           outFields = outFields, limit = limit,
                                           orderAttribute = order, older = older,
                                           newer = newer, timeStamp = field )
  selectResult[ 'Columns' ] = outFields
  return selectResult
        
def delete( rssDB, params, meta ):
  '''
    Method that transforms the RSS DB delete into the MySQL 
  '''
    
  accepted_keys = ( 'table', 'older', 'newer' )

  params = _capitalize( params )
  params = _discardNones( params )

  # Protection to avoid misunderstandings between MySQLMonkey and new code.
  if set( meta.keys() ) - set( accepted_keys ):
    return S_ERROR( 'Delete statement only accepts %s, got %s' % ( accepted_keys, meta.keys() ) )

  tableName  = meta[ 'table' ]
  tablesList = rssDB.getTablesList()
  if not tablesList[ 'OK' ]:
    return tablesList
  
  if not tableName in tablesList[ 'Value' ]:
    return S_ERROR( '"%s" is not on the schema tables' )
  
  field, older, newer = None, None, None  
  if 'older' in meta:
    field, older = meta[ 'older' ]
  elif 'newer' in meta:
    field, newer = meta[ 'newer' ]

  # Small secutiry measure, this prevents full table deletion.. by mistake I hope.
  if not params and not field and not ( older or newer ):
    return S_ERROR( 'Dude, you are going to delete the whole table %s' % tableName )
    
  return rssDB.database.deleteEntries( tableName, condDict = params, older = older,
                                       newer = newer, timeStamp = field )
      
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  