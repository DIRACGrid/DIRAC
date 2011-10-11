from DIRAC import S_OK

class MySQLMonkey( object ):
  
  ACCEPTED_KWARGS = [ 'table', 'uniqueKeys', 'sort', 'order', 'limit', 'columns', 
                      'onlyUniqueKeys' ]
  
  def __init__( self, dbWrapper ):
    self.dbWrapper = dbWrapper

################################################################################
# PUBLIC FUNCTIONS
################################################################################

  def insert( self, rDict, **kwargs ):  
    
    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    # END PARSING #
    
    return self.__insert( rDict, **kwargs )

  def insertQuery( self, rDict, **kwargs ):
    
    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    # END PARSING #

    return self.__insertSQLStatement( rDict, **kwargs )

################################################################################

  def select( self, rDict, **kwargs ):  

    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    # END PARSING #
    
    return self.__select( rDict, **kwargs )

  def selectQuery( self, rDict, **kwargs ):
    
    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    # END PARSING #

    return self.__selectSQLStatement( rDict, **kwargs )

################################################################################

  def get( self, rDict, **kwargs ):

    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    kwargs[ 'onlyUniqueKeys' ] = None 
    # END PARSING #

    return self.__select( rDict, **kwargs )

  def getQuery( self, rDict, **kwargs ):
    
    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    kwargs[ 'onlyUniqueKeys' ] = None
    # END PARSING #

    return self.__selectSQLStatement( rDict, **kwargs )

################################################################################

  def update( self, rDict, **kwargs ):

    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    # END PARSING #
    
    return self.__update( rDict, **kwargs )    

  def updateQuery( self, rDict, **kwargs ):
    
    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    # END PARSING #

    return self.__updateSQLStatement( rDict, **kwargs )


################################################################################    
    
  def delete( self, rDict, **kwargs ):

    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    kwargs[ 'onlyUniqueKeys' ] = None
    # END PARSING #

    return self.__delete( rDict, **kwargs )

  def deleteQuery( self, rDict, **kwargs ):
    
    # PARSING #
    rDict  = self.parseDict( rDict )
    kwargs = self.parseKwargs( kwargs )
    kwargs[ 'onlyUniqueKeys' ] = None
    # END PARSING #

    return self.__deleteSQLStatement( rDict, **kwargs )

      
################################################################################
    
  def localsToDict( self, locls ):

    rDict = {}
    for k,v in locls.items():
      if k not in ['self', 'k', 'v', 'rDict', 'kwargs' ]:
        if v is not None:
          rDict[ k[0].upper() + k[1:] ] = v   
    
    return rDict

################################################################################
# PARSERS
################################################################################

  def parseDict( self, rDict ):
    # checks that the fields of the table are correct !!
    return rDict

  def parseKwargs( self, kwargs ):
    
    pKwargs = {}
    for ak in self.ACCEPTED_KWARGS:
      pKwargs[ ak ] = kwargs.pop( ak, None )
  
    if not pKwargs.has_key( 'table' ):
      raise NameError( 'Table name not given' )
    
    if pKwargs[ 'onlyUniqueKeys' ] is None:
      pKwargs[ 'onlyUniqueKeys' ] = True
      
    if pKwargs[ 'uniqueKeys' ] is None:
      pKwargs[ 'uniqueKeys' ] = self.dbWrapper.__TABLES__[ pKwargs[ 'table'] ][ 'uniqueKeys' ]  
    
    return pKwargs  
  
################################################################################
# RAW SQL FUNCTIONS
################################################################################

  def __insert( self, rDict, **kwargs ):
    
    sqlStatement = self.__insertSQLStatement( rDict, **kwargs )
    return self.dbWrapper.db._update( sqlStatement )
    
  def __select( self, rDict, **kwargs ):

    sqlStatement = self.__selectSQLStatement( rDict, **kwargs)  
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
    
    req = "INSERT INTO %s (" % table  
    req += ','.join( "%s" % key for key in rDict.keys())
    req += ") VALUES ("
    req += ','.join( "'%s'" % value for value in rDict.values())
    req += ")"   
    
    return req
  
  def __selectSQLStatement( self, rDict, **kwargs ):

    table = kwargs[ 'table' ]
    
    columns  = kwargs[ 'columns' ]
    sort     = kwargs[ 'sort' ]
    limit    = kwargs[ 'limit' ]      
    order    = kwargs[ 'order' ]
  
    whereElements = self.__getWhereElements( rDict, **kwargs )       
    columns       = self.__getColumns( columns )

    if sort is not None: 
      sort        = self.__listToString( sort )  
    if order is not None:
      order       = self.__listToString( order ) 
                
    req = "SELECT %s from %s" % ( columns, table )
    if whereElements:
      req += " WHERE %s" % whereElements
    if sort:
      req += " ORDER BY %s" % sort
      if order:
        req += " %s" % order 
    if limit:
      req += " LIMIT %d" % limit   
  
    return req
  
  def __updateSQLStatement( self, rDict, **kwargs ):
    
    table = kwargs[ 'table' ]
    
    whereElements = self.__getWhereElements( rDict, **kwargs )
        
    req = "UPDATE %s SET " % table
    req += ','.join( "%s='%s'" % (key,value) for (key,value) in rDict.items() if ( key not in kwargs['uniqueKeys'] ) )
    req += " WHERE %s" % whereElements

    return req

  def __deleteSQLStatement( self, rDict, **kwargs ):  
    
    table = kwargs[ 'table' ]
    
    whereElements = self.__getWhereElements( rDict, **kwargs )
    
    req = "DELETE from %s" % table
    # This was a bug, but is quite handy.
    # Prevents users from deleting the whole table in one go if on the client
    # they execute deleteX() without arguments. 
    if whereElements is not None:
      req += " WHERE %s" % whereElements
    
    return req  
       
################################################################################       
################################################################################   
       
################################################################################
# AUXILIAR FUNCTIONS   
    
  def __getColumns( self, columnsList ):
    
    columns = ""
    
    if columnsList is None:
      columns = "*"  
    else:
      columns = self.__listToString( columnsList )
    
    return columns
    
  def __listToString( self, itemsList ):  
    
    if not isinstance( itemsList, list):
      itemsList = [ itemsList ]
    
    return ','.join( _i for _i in itemsList )  
      
################################################################################
# OTHER FUNCTIONS   
   
   
  def getColumns( self, columnsList ):
    
    cols = ""
    
    if columnsList is None:
      cols = "*"
    else:
      if not isinstance( columnsList, list):
        columnsList = [ columnsList ]
      cols = ','.join( col for col in columnsList )  
      
    return cols

  def __getWhereElements( self, rDict, **kwargs ):
   
    items = []

    for k,v in rDict.items():    
#      if kwargs[ 'excludeUniqueKeys' ] and k in kwargs[ 'uniqueKeys' ]:
#        continue
      
      if kwargs[ 'onlyUniqueKeys' ] and k not in kwargs[ 'uniqueKeys' ]:
        continue
      
      if v is None:
        continue
      
      elif isinstance( v, list ):
        if len(v) > 1:
          items.append( '%s IN %s' % ( k, tuple( [ str(vv) for vv in v if vv is not None ] ) ) )
        elif len(v):
          if v[ 0 ] is not None:
            items.append( "%s='%s'" % ( k, v[0] ) )
        else:
          items.append( "%s=''" % k )
          #raise NameError( rDict )      
      else:
        items.append( "%s='%s'" % ( k, v ) )
                
    if kwargs.has_key( 'minor' ):
      for k,v in kwargs[ 'minor' ].items():
        if v is not None:  
          items.append( "%s < '%s'" % ( k, v ) ) 
    
    if kwargs.has_key( 'or' ):
      
      orItems = []
      for orDict in kwargs[ 'or' ]:      
        
        ordict = orDict.pop( 'dict', {} )
        self.getWhereElements( ordict, **orDict )                     
                
      items.append( ' OR '. join( orItem for orItem in orItems ) )          
                
    whereElements = ' AND '.join( item for item in items )
    return whereElements   
  
  def getWhereElements( self, dict, **kwargs ):
   
    items = []

    for k,v in dict.items():
      
      if v is None:
        pass
      elif isinstance( v, list ):
        if len(v) > 1:
          items.append( '%s IN %s' % ( k, tuple( [ str(vv) for vv in v ] ) ) )
        elif len(v):
          items.append( "%s='%s'" % ( k, v[0] ) )
        else:
          raise NameError( dict )      
      else:
        items.append( "%s='%s'" % ( k, v ) )
                
    if kwargs.has_key( 'minor' ):
      for k,v in kwargs[ 'minor' ].items():
        if v is not None:  
          items.append( "%s < '%s'" % ( k, v ) ) 
    
    if kwargs.has_key( 'or' ):
      
      orItems = []
      for orDict in kwargs[ 'or' ]:      
        
        ordict = orDict.pop( 'dict', {} )
        self.getWhereElements( ordict, **orDict )                     
                
      items.append( ' OR '. join( orItem for orItem in orItems ) )          
                
    whereElements = ' AND '.join( item for item in items )
    return whereElements         
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  