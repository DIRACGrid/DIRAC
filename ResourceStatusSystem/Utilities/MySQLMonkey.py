################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

from DIRAC import S_OK#, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSDBException

from datetime import datetime

import re

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
    type_ = self.__class__.__name__.replace( 'MySQL', '' )

    msg  = '%s %s:\n' % ( type_, self.name )
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

    columnsQuery = self.parent.mm.get( rDict, kwargs )
    if not columnsQuery[ 'OK' ]:
      columnsQuery = { 'Value' : [] }

    return columnsQuery

  def __inspectKeyColumnsUsage( self ):

    rDict  = { 'TABLE_SCHEMA' : self.parent.name,
               'TABLE_NAME'   : self.name }

    kwargs = { 'columns' : [ 'CONSTRAINT_NAME', 'COLUMN_NAME' ],
               'table'   : 'information_schema.KEY_COLUMN_USAGE' }

    keyColumnsUsageQuery = self.parent.mm.get( rDict, kwargs )
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

    tablesQuery = self.mm.get( rDict, kwargs )
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
                      'minor', 'or', 'not',#'dict', 'not',
                      'uniqueKeys', 'onlyUniqueKeys' ]

  def __init__( self, dbWrapper ):
    self.dbWrapper = dbWrapper
    self.SCHEMA    = {}
    self.mSchema   = None

  def setSchema( self, schema ):
    '''
    Makes a dirty quick solution, eventually it will use NodeTree..
    '''

    self.mSchema = schema

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

  def insert( self, params, meta ):
    params, meta  = self.__parseInput( params, meta )
    meta[ 'onlyUniqueKeys' ] = True
    return self.__insert( params, **meta )

  def update( self, params, meta ):
    params, meta  = self.__parseInput( params, meta )
    meta[ 'onlyUniqueKeys' ] = True
    return self.__update( params, **meta )

  def get( self, params, meta ):
    params, meta  = self.__parseInput( params, meta )
    return self.__select( params, **meta )

  def delete( self, params, meta ):
    params, meta  = self.__parseInput( params, meta )
    return self.__delete( params, **meta )

################################################################################
# PUBLIC FUNCTIONS
################################################################################

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

  def __parseInput( self, params, meta ):

    parsedMeta   = self.__parseKwargs( meta )
    parsedParams = self.__parseArgs( params, parsedMeta )

    return self.__parseEvil( parsedParams, parsedMeta )

    #return parsedParams,parsedMeta

  def __parseArgs( self, params, meta ):

    upperParams = {}

    for p in params.keys():
      pCap = p[0].upper() + p[1:]
      upperParams[ pCap ] = params[ p ]

    # CHECK FOR EVIL !!

    if 'information_schema' in meta[ 'table' ]:
      return upperParams

    if self.SCHEMA.has_key( meta[ 'table' ] ):
      _columns = self.SCHEMA[ meta[ 'table' ] ][ 'columns' ]

      if len( upperParams ) != len( _columns ):
        msg = 'Arguments length is %d, got %d - %s'
        msg = msg % ( len( _columns ), len(upperParams), str(upperParams) )
        raise RSSDBException( msg )

      for _col in _columns:
        if not upperParams.has_key( _col ):
          raise RSSDBException( 'Wrong parameter name: %s' % _col )

    return upperParams

  def __parseKwargs( self, meta ):

    pKwargs = {}
    for ak in self.ACCEPTED_KWARGS:
      pKwargs[ ak ] = meta.pop( ak, None )

    if not pKwargs.has_key( 'table' ) or pKwargs[ 'table' ] is None:
      raise RSSDBException( 'Table name not given' )

    if 'information_schema' in pKwargs[ 'table' ]:
      return pKwargs

    if not self.SCHEMA.has_key( pKwargs[ 'table' ]):
      raise RSSDBException( 'Table "%s" not found' % pKwargs[ 'table' ] )

    if pKwargs[ 'uniqueKeys' ] is None:
      pKwargs[ 'uniqueKeys' ] = self.SCHEMA[ pKwargs[ 'table' ]][ 'keyColumns' ]

    return pKwargs

  def __parseEvil( self, parsedArgs, parsedKwargs ):

    #First parameters, then meta

    if self.mSchema is None:
      return parsedArgs, parsedKwargs

    #parsedMeta
    def parseMultiple( l ):

      if parsedKwargs[ l ] is None:
        return None
      if not isinstance( parsedKwargs[ l ], list ):
        parsedKwargs[ l ] = [ parsedKwargs[ l ] ]
      res = self.dbWrapper.db._escapeValues( parsedKwargs[ l ] )
      #res = self._checkMultipleALPHA( parsedKwargs[ l ] )
      if not res[ 'OK' ]:
        raise RSSDBException( res[ 'Message' ] )
      return res[ 'Value' ]

    def parseDict( d, dataType ):

      if d is None:
        return d

      #k = self.dbWrapper.db._escapeValues( d.keys() )[ 'Value' ]
      k = self._checkMultipleALPHA( d.keys() )
      if dataType == 'DATETIME':
        v = self._checkDATETIME( d.values() )
      if dataType == 'STRING':
        #v = self.dbWrapper.db._escapeValues( d.values() )[ 'Value' ]
        v = self._checkMultipleALPHA( d.values() )

      return dict(zip(k,v))

    def parseDict2( mm, table, d ):

      if d is None:
        return d

      res = []

      for k,v in d.items():
      #k = self.dbWrapper.db._escapeValues( d.keys() )[ 'Value' ]
        k = self._checkALPHA( k )
      
      
        dataType = getattr( table, k ).dataType.upper()
        if not isinstance( v, list ):
          v = [ v ]

        # always returns a list !!
        v = getattr( self, '_check%s' % dataType )( v )
        
#      if dataType == 'DATETIME':
#        v = self._checkDATETIME( d.values() )
#      if dataType == 'STRING':
#        #v = self.dbWrapper.db._escapeValues( d.values() )[ 'Value' ]
#        v = self._checkMultipleALPHA( d.values() )

      return dict(zip(k,v))

    # table alreadyChecked
    table = getattr( self.mSchema, parsedKwargs[ 'table' ] )
    
    # sort
    if parsedKwargs.has_key( 'sort' ) and parsedKwargs['sort'] is not None:
      parsedKwargs[ 'sort'  ] = self._checkMultipleALPHA( parsedKwargs[ 'sort'  ] )#parseMultiple( 'sort' )
    # order
    if parsedKwargs.has_key( 'order') and parsedKwargs[ 'order' ] is not None:
      parsedKwargs[ 'order' ] = self._checkMultipleALPHA( parsedKwargs[ 'order'  ] )#parseMultiple( 'order' )
    # limit
    if parsedKwargs.has_key( 'limit') and parsedKwargs[ 'limit' ] is not None:
      if not isinstance( parsedKwargs[ 'limit' ], int ):
        raise RSSDBException( 'Non integer limit value' )
    # columns
    if parsedKwargs.has_key( 'columns') and parsedKwargs[ 'columns' ] is not None:
      parsedKwargs[ 'columns' ] = self._checkMultipleALPHA( parsedKwargs[ 'columns'  ] )#parseMultiple( 'columns' )
    # group
    if parsedKwargs.has_key( 'group') and parsedKwargs[ 'group' ] is not None:
      parsedKwargs[ 'group' ] = self._checkMultipleALPHA( parsedKwargs[ 'group'  ] )[0]#parseMultiple( 'group' )
    # uniqueKeys
    if parsedKwargs.has_key( 'uniqueKeys') and parsedKwargs[ 'uniqueKeys' ] is not None:
      parsedKwargs[ 'uniqueKeys' ] = self._checkMultipleALPHA( parsedKwargs[ 'uniqueKeys'  ] )#parseMultiple( 'uniqueKeys' )
    # count
    if parsedKwargs.has_key( 'count') and parsedKwargs[ 'count'] is not None and parsedKwargs[ 'count' ] != True:
      #count = self.dbWrapper.db._escapeString( parsedKwargs[ 'count' ])
      count = self._checkALPHA( parsedKwargs[ 'count' ])
      if not count[ 'OK' ]:
        raise RSSDBException( 'Cannot escape count value')
      parsedKwargs[ 'count'] = count[ 'Value' ]
    # or
    if parsedKwargs.has_key( 'or' ) and parsedKwargs['or'] is not None:
      orD = parsedKwargs[ 'or' ]
      if orD is not None:
        parsedOr = []
        for d in orD:
          #small hack to prevent a crash parsing the arguments of the dict
          d['kwargs']['table'] = parsedKwargs[ 'table' ]
          pDict,pKw = self.__parseEvil( d['dict'], d['kwargs'] )
          parsedOr.append( { 'dict' : pDict, 'kwargs' :pKw } )
        #orD[ 'dict' ],orD[ 'kwargs' ] = self.__parseEvil( orD[ 'dict' ],orD[ 'kwargs' ] )
        parsedKwargs[ 'or' ] = parsedOr

    # minor ( datetime )
    if parsedKwargs.has_key( 'minor' ) and parsedKwargs[ 'minor' ] is not None:
      parsedKwargs[ 'minor' ] = parseDict( parsedKwargs[ 'minor' ], 'DATETIME' )

    # not ( string )
    if parsedKwargs.has_key( 'not' ) and parsedKwargs[ 'not' ] is not None:
      parsedKwargs[ 'not' ] = parseDict( parsedKwargs[ 'not' ], 'STRING' )

#    if not isinstance( parsedKwargs[ 'sort' ], list ):
#      parsedKwargs[ 'sort' ] = [ parsedKwargs[ 'sort' ] ]
#    res = self.dbWrapper.db._escapeValues( parsedKwargs[ 'sort' ] )
#    if not res[ 'OK' ]:
#      raise RSSDBException( res[ 'Message' ] )
#    parsedKwargs[ 'sort' ] = res[ 'Value' ]

    # parsedArgs
    for k,v in parsedArgs.items():

      self._checkALPHA( k )
      if v is None:
        continue

      dataType = getattr( table, k ).dataType.upper()
      if not isinstance( v, list ):
        v = [ v ]

      v = getattr( self, '_check%s' % dataType )( v )
      parsedArgs[ k ] = v

    return parsedArgs, parsedKwargs

  def _checkMultipleALPHA( self,suspicious ):

    if not isinstance( suspicious, list ):
      suspicious = [ suspicious ]

    for s in suspicious:
      self._checkALPHA( s )
    return suspicious

  def _checkALPHA( self,suspicious ):
    if not re.match( "[A-Za-z_-]+$", suspicious ):
      raise RSSDBException( 'Non alpha value "%s"' % suspicious )
    return suspicious

  def _checkVARCHAR( self, suspicious ):

    res = self.dbWrapper.db._escapeValues( suspicious )
    if not res[ 'OK' ]:
      raise RSSDBException( res[ 'Message' ] )
    return res[ 'Value' ]

  def _checkDATETIME( self, suspicious ):
    for s in suspicious:
      if not isinstance( s, datetime ):
        raise RSSDBException( 'Non datetime value "%s"' % s )
    return suspicious

  def _checkFLOAT(self, suspicious):
    for i in list(suspicious):
      if type(i) not in (int, float):
        raise RSSDBException( 'Non numeric value "%s"' % suspicious )
    return suspicious

  _checkNUMERIC  = _checkFLOAT
  _checkDECIMAL  = _checkFLOAT

  _checkINTEGER  = _checkFLOAT
  _checkTINYINT  = _checkFLOAT
  _checkSMALLINT = _checkFLOAT
  _checkINT      = _checkFLOAT
  _checkBIGINT   = _checkFLOAT

  _checkREAL     = _checkFLOAT
  _checkDOUBLE   = _checkFLOAT

  _checkBLOB     = _checkVARCHAR
  _checkTEXT     = _checkVARCHAR

################################################################################
# RAW SQL FUNCTIONS
################################################################################

  def __insert( self, rDict, **kwargs ):

    sqlStatement = self.__insertSQLStatement( rDict, **kwargs )
    print sqlStatement
    return self.dbWrapper.db._update( sqlStatement )

  def __select( self, rDict, **kwargs ):

    sqlStatement = self.__selectSQLStatement( rDict, **kwargs )
    print sqlStatement
    sqlQuery     = self.dbWrapper.db._query( sqlStatement )
    return S_OK( [ list(rQ) for rQ in sqlQuery[ 'Value' ]] )

  def __update( self, rDict, **kwargs ):

    sqlStatement = self.__updateSQLStatement( rDict, **kwargs )
    print sqlStatement
    return self.dbWrapper.db._update( sqlStatement )

  def __delete( self, rDict, **kwargs ):

    sqlStatement = self.__deleteSQLStatement( rDict, **kwargs )
    print sqlStatement
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

    #print req
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
      columnsList = [ '*' ]

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
            items.append( '%s=%s' % ( k, v[0] ) )
            #items.append( '%s="%s"' % ( k, v[0] ) )
        else:
          items.append( '%s=""' % k )
          #raise NameError( rDict )
      else:
        #items.append( '%s="%s"' % ( k, v ) )
        items.append( '%s=%s' % ( k, v ) )

    if kwargs.has_key( 'minor' ) and kwargs[ 'minor' ] is not None:

      for k,v in kwargs[ 'minor' ].items():
        if v is not None:
          #items.append( '%s < "%s"' % ( k, v ) )
          items.append( '%s < %s' % ( k, v ) )

    if kwargs.has_key( 'not' ) and kwargs[ 'not' ] is not None:

      for k,v in kwargs[ 'not' ].items():
        if v is not None:
          items.append( '%s != "%s"' % ( k, v ) )
          items.append( '%s != %s' % ( k, v ) )

    if kwargs.has_key( 'or' ) and kwargs[ 'or' ] is not None:

      orItems = []
      for orDict in kwargs[ 'or' ]:
        ordict   = orDict.pop( 'dict', {} )
        orkwargs = orDict.pop( 'kwargs', {} )
        orItems.append( '(%s)' % self.__getWhereElements( ordict, **orkwargs ) )

      items.append( ' OR '. join( orItem for orItem in orItems ) )

    return ' AND '.join( item for item in items )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
