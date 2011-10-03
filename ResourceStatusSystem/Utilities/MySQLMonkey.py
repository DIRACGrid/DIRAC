
class MySQLMonkey( object ):
  
  def __init__( self ):
    pass

  def getColumns( self, columnsList ):
    
    cols = ""
    
    if columnsList is None:
      cols = "*"
    else:
      if not isinstance( columnsList, list):
        columnsList = [ columnsList ]
      cols = ','.join( col for col in columnsList )  
      
    return cols
  
  def getWhereElements( self, dict, **kwargs ):
   
    items = []

    for k,v in dict.items():
      if v is None:
        pass
      elif isinstance( v, list ):
        if len(v) > 1:
          items.append( '%s IN %s' % ( k, tuple(v) ) )
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