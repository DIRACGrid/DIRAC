class Node( object ):
  
  def __init__( self, name, level, cLevel, pLevel ):
    
    self.realName      = name
    self.name          = self._fixName( name )
    self.level         = level
    self.attr          = {}
    self.childrenLevel = cLevel
    self.parentLevel   = pLevel
    
    self._parents = { pLevel : {} }  
    self._levels  = {} 
  
  def _fixName( self, name ):
    
    name = name.replace( '.', '_' )
    name = name.replace( '@', '_' )
    name = name.replace( '-', '_' )
    
    return name  
    
  def __getattr__( self, attrName ):
    
    if self._levels[ self.childrenLevel ].has_key( attrName ):
      return self._levels[ self.childrenLevel ][ attrName ]
    raise AttributeError( '%s %s has no attribute %s' % ( self.level, self.name, attrName ) )

  def setAttr( self, name, value ):
    self.attr[ name ] = value 

  def setChildren( self, child, level = None ):

    if not level:
      level = self.childrenLevel 
    
    if not self._levels.has_key( level ):
      self._levels[ level ] = {}
    
    if not self._levels[ level ].has_key( child.name ):
      if not child._parents.has_key( self.level ):
        child._parents[ self.level ] = {}
      child._parents[ self.level ][ self.name ] = self
      self._levels[ level ][ child.name ] = child 
    else:
      pass
      #raise AttributeError(  '%s is already a child' % child.name )  
    
    for parent in self._parents[ self.parentLevel ].values():
      parent.setChildren( child, level )      

#  def __repr__( self ):
#    
#    msg  = '%s %s:\n' % ( self.level, self.name ) 
#    msg += ','.join( [ '<%s>' % k for k in self._levels[ self.childrenLevel ].keys() ] ) 
#    return msg   
  
  
   
        
  
  