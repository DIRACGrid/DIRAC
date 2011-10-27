################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

from DIRAC import S_OK#, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSDBException

#def localsToDict( a ):
#  return {}

################################################################################
# MySQL Monkey 
################################################################################

class MySQLMonkey( object ):
  
  def __init__( self, dbWrapper ):
    
    self.mStatements = MySQLStatements( dbWrapper ) 
    self.mSchema     = MySQLSchema( self.mStatements )
    self.mStatements.setSchema( self.mSchema )
    
  def __getattr__( self, attrName ):
    return getattr( self.mStatements, attrName )

################################################################################
# MySQL Schema
################################################################################

class MySQLNode( object ):
  
  def __init__( self, name, parent = None ):
    self.name   = name
    self.parent = parent
    self.nodes  = []

  def __getattr__( self, attrName ):
    for n in self.nodes:
      if n.name == attrName:
        return n
    raise AttributeError( '%s %s has no attribute %s' % ( self.__class__.__name__, self.name, attrName ) )

  def __repr__( self ):
    type = self.__class__.__name__.replace( 'MySQL', '' )
    
    msg  = '%s %s:\n' % ( type, self.name ) 
    msg += ','.join( [ '<%s>' % n.name for n in self.nodes ] ) 
    return msg   

################################################################################

class MySQLColumn( MySQLNode ):  
  
  def __init__( self, columnName, parent ):
    
    super( MySQLColumn, self ).__init__( columnName, parent )
     
    self.primary    = False
    self.keyUsage   = False
    self.extra      = None
    self.position   = None
    self.dataType   = None
    self.charMaxLen = None 

  def __repr__( self ):
    return 'Column %s' % self.name

################################################################################

class MySQLTable( MySQLNode ):
  
  def __init__( self, tableName, parent, tableType ):
    
    super( MySQLTable, self ).__init__( tableName, parent )
    
    self.type  = tableType
    self.nodes = self.__inspectTableColumns()
    
    def columnSort( col ):
      return col.position 
    
    self.nodes.sort( key = columnSort )

  def __inspectTableColumns( self ):

    columns = {}
    
    columnsQuery = self.__inspectColumns()
    
    for col in columnsQuery[ 'Value' ]:
      
      column = MySQLColumn( col[ 0 ], self )
      column.extra      = col[ 1 ]
      column.position   = col[ 2 ]
      column.dataType   = col[ 3 ]
      column.charMaxLen = col[ 4 ] 
      
      columns[ col[0] ] = column
            
    keyColumnsUsageQuery = self.__inspectKeyColumnsUsage()        
           
    for kCol in keyColumnsUsageQuery[ 'Value' ]:
      
      constraint, columnName = kCol
      if columns.has_key( columnName ):
        if constraint == 'PRIMARY':
          columns[ columnName ].primary  = True
        else:
          columns[ columnName ].keyUsage = True  
            
    return columns.values()

  def __inspectColumns( self ):
    
    rDict  = { 
               'TABLE_NAME'   : self.name,
               'TABLE_SCHEMA' : self.parent.name 
             }
    kwargs = { 
               'columns'    : [ 'COLUMN_NAME', 'EXTRA', 'ORDINAL_POSITION', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH' ], 
               'table'      : 'information_schema.COLUMNS' 
             } 
        
    columnsQuery = self.parent.mm.get2( rDict, **kwargs )
    if not columnsQuery[ 'OK' ]:
      columnsQuery = { 'Value' : [] }

    return columnsQuery

  def __inspectKeyColumnsUsage( self ):
    
    rDict  = { 'TABLE_SCHEMA' : self.parent.name,
               'TABLE_NAME'   : self.name }
    
    kwargs = { 'columns' : [ 'CONSTRAINT_NAME', 'COLUMN_NAME' ],
               'table'   : 'information_schema.KEY_COLUMN_USAGE' }
    
    keyColumnsUsageQuery = self.parent.mm.get2( rDict, **kwargs )
    if not keyColumnsUsageQuery[ 'OK' ]:
      keyColumnsUsageQuery = { 'Value' : [] }
      
    return keyColumnsUsageQuery        

################################################################################

class MySQLSchema( MySQLNode ):
  
  def __init__( self, mStatements ):
    
    super( MySQLSchema, self ).__init__( mStatements.dbWrapper.db._MySQL__dbName )
    
    self.mm    = mStatements
    self.nodes = self.__inspectTables()   
    
  def __inspectTables( self ):
  
    rDict  = { 'TABLE_SCHEMA' : self.name }
    kwargs = { 'columns'    : [ 'TABLE_NAME', 'TABLE_TYPE' ], 
               'table'      : 'information_schema.TABLES' } 
        
    tablesQuery = self.mm.get2( rDict, **kwargs )
    if not tablesQuery[ 'OK' ]:
      tablesQuery[ 'Value' ] = []

    tables = []
      
    for pair in tablesQuery[ 'Value' ]:
      
      tableName,tableType = pair
      table               = MySQLTable( tableName, self, tableType )

      tables.append( table )
        
    return tables
  
################################################################################
# MySQL Statements
################################################################################

class MySQLStatements( object ):
  
  ACCEPTED_KWARGS = [ 'table',  
                      'sort', 'order', 'limit', 'columns', 'group', 'count',  
                      'minor', 'or', 'dict', 'not',
                      'uniqueKeys', 'onlyUniqueKeys' ]
  
  def __init__( self, dbWrapper ):
    self.dbWrapper = dbWrapper
    self.SCHEMA    = {}

  def setSchema( self, schema ):
    
    for table in schema.nodes:
      self.SCHEMA[ table.name ] = { 'columns' : [], 'keyColumns' : [] }
      
      for column in table.nodes:
        if not column.extra:
          self.SCHEMA[ table.name ][ 'columns' ].append( column.name )
        
          if column.primary == True:  
            self.SCHEMA[ table.name ][ 'keyColumns' ].append( column.name )
          elif column.keyUsage == True:
            self.SCHEMA[ table.name ][ 'keyColumns' ].append( column.name )  

################################################################################
# PUBLIC FUNCTIONS II
################################################################################

  def insert2( self, *args, **kwargs ):
    
    #try:
      # PARSING #
      pArgs,pKwargs  = self.parseInput( *args, **kwargs )
      pKwargs[ 'onlyUniqueKeys' ] = True
      # END PARSING #
    
      return self.__insert( pArgs, **pKwargs )
    #except:
    #  return S_ERROR( 'Message' )

  def update2( self, *args, **kwargs ):
    
    #try:
      # PARSING #
      pArgs,pKwargs  = self.parseInput( *args, **kwargs )
      pKwargs[ 'onlyUniqueKeys' ] = True
      # END PARSING #
    
      return self.__update( pArgs, **pKwargs )
    #except:
    #  return S_ERROR( 'Message' )

  def get2( self, *args, **kwargs ):
    
    #try:
      # PARSING #
      pArgs,pKwargs  = self.parseInput( *args, **kwargs )
      # END PARSING #
    
      return self.__select( pArgs, **pKwargs )
    #except:
    #  return S_ERROR( 'Message' )

  def delete2( self, *args, **kwargs ):
    
    #try:
      # PARSING #
      pArgs,pKwargs  = self.parseInput( *args, **kwargs )
      # END PARSING #
    
      return self.__delete( pArgs, **pKwargs )
    #except:
    #  return S_ERROR( 'Message')

################################################################################
# PUBLIC FUNCTIONS
################################################################################

#  def insert( self, rDict, **kwargs ):  
#    
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    kwargs[ 'onlyUniqueKeys' ] = True
#    # END PARSING #
#    
#    return self.__insert( rDict, **kwargs )
#
#  def insertQuery( self, rDict, **kwargs ):
#    
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    kwargs[ 'onlyUniqueKeys' ] = True
#    # END PARSING #
#
#    return self.__insertSQLStatement( rDict, **kwargs )

################################################################################

#  def select( self, rDict, **kwargs ):  
#
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    kwargs[ 'onlyUniqueKeys' ] = True
#    # END PARSING #
#    
#    return self.__select( rDict, **kwargs )
#
#  def selectQuery( self, rDict, **kwargs ):
#    
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    kwargs[ 'onlyUniqueKeys' ] = True
#    # END PARSING #
#
#    return self.__selectSQLStatement( rDict, **kwargs )

################################################################################

#  def get( self, rDict, **kwargs ):
#
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    #kwargs[ 'onlyUniqueKeys' ] = None 
#    # END PARSING #
#
#    return self.__select( rDict, **kwargs )
#
#  def getQuery( self, rDict, **kwargs ):
#    
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    #kwargs[ 'onlyUniqueKeys' ] = None
#    # END PARSING #
#
#    return self.__selectSQLStatement( rDict, **kwargs )

################################################################################

#  def update( self, rDict, **kwargs ):
#
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    kwargs[ 'onlyUniqueKeys' ] = True
#    # END PARSING #
#    
#    return self.__update( rDict, **kwargs )    
#
#  def updateQuery( self, rDict, **kwargs ):
#    
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    kwargs[ 'onlyUniqueKeys' ] = True
#    # END PARSING #
#
#    return self.__updateSQLStatement( rDict, **kwargs )

################################################################################    
    
#  def delete( self, rDict, **kwargs ):
#
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    #kwargs[ 'onlyUniqueKeys' ] = None
#    # END PARSING #
#
#    return self.__delete( rDict, **kwargs )
#
#  def deleteQuery( self, rDict, **kwargs ):
#    
#    # PARSING #
#    rDict  = self.parseDict( rDict )
#    kwargs = self.parseKwargs( kwargs )
#    #kwargs[ 'onlyUniqueKeys' ] = None
#    # END PARSING #
#
#    return self.__deleteSQLStatement( rDict, **kwargs )

################################################################################
# PARSERS
################################################################################

  def parseInput( self, *args, **kwargs ):
    
    parsedKwargs = self.__parseKwargs( **kwargs )
    parsedArgs   = self.__parseArgs( *args, **kwargs )

    return parsedArgs,parsedKwargs

  def __parseArgs( self, *args, **kwargs ):
    
    # CHECK FOR EVIL !!
    
    if 'information_schema' in kwargs[ 'table' ]:
      return args[0]
    
    if self.SCHEMA.has_key( kwargs[ 'table' ] ):
      _columns = self.SCHEMA[ kwargs[ 'table' ] ][ 'columns' ]
    
      if len( args ) != len( _columns ):
        msg = 'Arguments length is %d, got %d - %s' 
        msg = msg % ( len( _columns ), len(args), str(args) )
        raise RSSDBException( msg )
    
    argsDict = {}
    _junk    = map(lambda k, v: argsDict.update({k: v}), _columns, args )
     
    return argsDict

#  def parseKwargs( self, kwargs ):
#    pKwargs = {}
#    for ak in self.ACCEPTED_KWARGS:
#      pKwargs[ ak ] = kwargs.pop( ak, None )
#  
#    if not pKwargs.has_key( 'table' ):
#      raise RSSDBException( 'Table name not given' )
#    
##    if pKwargs[ 'onlyUniqueKeys' ] is None:
##      pKwargs[ 'onlyUniqueKeys' ] = True
#      
#    if pKwargs[ 'uniqueKeys' ] is None:
#      if self.dbWrapper.__SCHEMA__.has_key( pKwargs[ 'table'] ):
#        if self.dbWrapper.__SCHEMA__[ pKwargs[ 'table'] ].has_key( 'uniqueKeys' ):
#          pKwargs[ 'uniqueKeys' ] = self.dbWrapper.__SCHEMA__[ pKwargs[ 'table'] ][ 'uniqueKeys' ]  
#    
#    return pKwargs  

  def __parseKwargs( self, **kwargs ):
    
    pKwargs = {}
    for ak in self.ACCEPTED_KWARGS:
      pKwargs[ ak ] = kwargs.pop( ak, None )
  
    if not pKwargs.has_key( 'table' ):
      raise RSSDBException( 'Table name not given' )
    
    if 'information_schema' in pKwargs[ 'table' ]:
      return pKwargs
    
#    if pKwargs[ 'onlyUniqueKeys' ] is None:
#      pKwargs[ 'onlyUniqueKeys' ] = True
      
    if pKwargs[ 'uniqueKeys' ] is None:
      pKwargs[ 'uniqueKeys' ] = self.SCHEMA[ pKwargs[ 'table' ]][ 'keyColumns' ]
#      if self.dbWrapper.__SCHEMA__.has_key( pKwargs[ 'table'] ):
#        if self.dbWrapper.__SCHEMA__[ pKwargs[ 'table'] ].has_key( 'uniqueKeys' ):
#          pKwargs[ 'uniqueKeys' ] = self.dbWrapper.__SCHEMA__[ pKwargs[ 'table'] ][ 'uniqueKeys' ]  
    
    return pKwargs  
  
################################################################################
# RAW SQL FUNCTIONS
################################################################################

  def __insert( self, rDict, **kwargs ):
    
    sqlStatement = self.__insertSQLStatement( rDict, **kwargs )
    return self.dbWrapper.db._update( sqlStatement )
    
  def __select( self, rDict, **kwargs ):

    sqlStatement = self.__selectSQLStatement( rDict, **kwargs )
    sqlQuery     = self.dbWrapper.db._query( sqlStatement )
    
    return S_OK( [ list(rQ) for rQ in sqlQuery[ 'Value' ]] )     
 
  def __update( self, rDict, **kwargs ):
     
    sqlStatement = self.__updateSQLStatement( rDict, **kwargs )
    return self.dbWrapper.db._update( sqlStatement )
       
  def __delete( self, rDict, **kwargs ):
    
    sqlStatement = self.__deleteSQLStatement( rDict, **kwargs )
    return self.dbWrapper.db._update( sqlStatement )     
       
################################################################################
# SQL STATEMENTS FUNCTIONS
################################################################################       
       
  def __insertSQLStatement( self, rDict, **kwargs ):  
    
    table = kwargs[ 'table' ]
    
    req = 'INSERT INTO %s (' % table  
    req += ','.join( '%s' % key for key in rDict.keys())
    req += ') VALUES ('
    req += ','.join( '"%s"' % value for value in rDict.values())
    req += ')'   
    
    return req
  
  def __selectSQLStatement( self, rDict, **kwargs ):

    table = kwargs[ 'table' ]
    
    columns  = kwargs[ 'columns' ]
    sort     = kwargs[ 'sort' ]
    limit    = kwargs[ 'limit' ]      
    order    = kwargs[ 'order' ]
    group    = kwargs[ 'group' ]
  
    whereElements = self.__getWhereElements( rDict, **kwargs )       
    columns       = self.__getColumns( columns, **kwargs )

    if sort is not None: 
      sort        = self.__listToString( sort )  
    if order is not None:
      order       = self.__listToString( order ) 
    if group is not None:
      order       = self.__listToString( group )
                
    req = 'SELECT %s from %s' % ( columns, table )
    if whereElements:
      req += ' WHERE %s' % whereElements
    if sort:
      req += ' ORDER BY %s' % sort
      if order:
        req += ' %s' % order 
    if group:
      req += ' GROUP BY %s' % group    
    if limit:
      req += ' LIMIT %d' % limit   
  
    return req
  
  def __updateSQLStatement( self, rDict, **kwargs ):
    
    table = kwargs[ 'table' ]
    
    whereElements = self.__getWhereElements( rDict, **kwargs )
        
    req = 'UPDATE %s SET ' % table
    req += ','.join( '%s="%s"' % (key,value) for (key,value) in rDict.items() if ( key not in kwargs['uniqueKeys'] and value is not None ) )
    # This was a bug, but is quite handy.
    # Prevents users from updating the whole table in one go if on the client
    # they execute updateX() without arguments for the uniqueKeys values 
    if whereElements is not None:
      req += ' WHERE %s' % whereElements

    return req

  def __deleteSQLStatement( self, rDict, **kwargs ):  
    
    table = kwargs[ 'table' ]
    
    whereElements = self.__getWhereElements( rDict, **kwargs )
    
    req = 'DELETE from %s' % table
    # This was a bug, but is quite handy.
    # Prevents users from deleting the whole table in one go if on the client
    # they execute deleteX() without arguments. 
    if whereElements is not None:
      req += ' WHERE %s' % whereElements
    
    return req  
       
################################################################################       
################################################################################   
       
################################################################################
# AUXILIAR FUNCTIONS   
    
  def __getColumns( self, columnsList, **kwargs ):
    
    #columns = ""
    
    #KWARGS
    count   = kwargs[ 'count' ]
    
    if columnsList is None:
      columnsList = [ "*" ]  
        
    # Either True, or a string value  
    if count == True:  
      columnsList.append( 'COUNT(*)' )
    elif isinstance( count,str ):
      columnsList.append( 'COUNT(%s)' % count )    
       
    return self.__listToString( columnsList )
    
    #return columns
    
  def __listToString( self, itemsList ):  
    
    if not isinstance( itemsList, list):
      itemsList = [ itemsList ]
    
    return ','.join( _i for _i in itemsList )  

  def __getWhereElements( self, rDict, **kwargs ):
   
    items = []

    for k,v in rDict.items():    

      if kwargs.has_key('onlyUniqueKeys') and kwargs[ 'onlyUniqueKeys' ] and k not in kwargs[ 'uniqueKeys' ]:#self.SCHEMA[ kwargs[ 'table' ]][ 'keyColumns' ]:#
        continue
      
      if v is None:
        continue
      
      elif isinstance( v, list ):
        if len(v) > 1:
          items.append( '%s IN %s' % ( k, tuple( [ str(vv) for vv in v if vv is not None ] ) ) )
        elif len(v):
          if v[ 0 ] is not None:
            items.append( '%s="%s"' % ( k, v[0] ) )
        else:
          items.append( '%s=""' % k )
          #raise NameError( rDict )      
      else:
        items.append( '%s="%s"' % ( k, v ) )      
                
    if kwargs.has_key( 'minor' ) and kwargs[ 'minor' ] is not None:
      
      for k,v in kwargs[ 'minor' ].items():
        if v is not None:  
          items.append( '%s < "%s"' % ( k, v ) ) 
    
    if kwargs.has_key( 'not' ) and kwargs[ 'not' ] is not None:
      
      for k,v in kwargs[ 'not' ].items():
        if v is not None:  
          items.append( '%s != "%s"' % ( k, v ) )      
    
    if kwargs.has_key( 'or' ) and kwargs[ 'or' ] is not None:
          
      orItems = []
      for orDict in kwargs[ 'or' ]:      
        ordict   = orDict.pop( 'dict', {} )
        orkwargs = orDict.pop( 'kwargs', {} )
        orItems.append( '(%s)' % self.__getWhereElements( ordict, **orkwargs ) )                     
                
      items.append( ' OR '. join( orItem for orItem in orItems ) )                      
                
    return ' AND '.join( item for item in items )

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  