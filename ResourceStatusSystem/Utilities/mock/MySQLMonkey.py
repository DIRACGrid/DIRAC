################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

class MySQLStatements( object ):
  
  def __init__( self, dbWrapper ):
    self.dbWrapper = dbWrapper
    self.SCHEMA    = {}
  
  def insert2( self, *args, **kwargs  ):
    return { 'OK' : True, 'Value' : '' }
  
  def update2( self, *args, **kwargs  ):
    return { 'OK' : True, 'Value' : '' }  
  
  def get2( self, *args, **kwargs  ):
    return { 'OK' : True, 'Value' : '' }
 
  def delete2( self, *args, **kwargs  ):
    return { 'OK' : True, 'Value' : '' }

################################################################################

class MySQLSchema( object ):

  def __init__( self, mStatements ):
    self.mm    = mStatements
    self.nodes = []

################################################################################

class MySQLMonkey( object ):

  def __init__( self, dbWrapper ):
    self.mStatements = MySQLStatements( dbWrapper ) 
    self.mSchema     = MySQLSchema( self.mStatements )
    
  def __getattr__( self, attrName ):
    return getattr( self.mStatements, attrName )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
