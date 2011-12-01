################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.DB.mock.DB                 import DB
from DIRAC.ResourceStatusSystem.Utilities.mock.MySQLMonkey import MySQLMonkey

from DIRAC.ResourceStatusSystem.Utilities.Decorators import CheckDBExecution, ValidateDBTypes

class ResourceManagementDB( object ):
  
  def __init__( self ):
    
    self.db = DB()
    self.mm = MySQLMonkey( self )
    
  @CheckDBExecution
  @ValidateDBTypes  
  def insert( self, args, kwargs ):
    #return { 'OK' : True, 'Value' : '' }
    return self.mm.insert( *args, **kwargs )
  
  @CheckDBExecution
  @ValidateDBTypes  
  def update( self, args, kwargs ):
    #return { 'OK' : True, 'Value' : '' }
    return self.mm.update( *args, **kwargs )
  
  @CheckDBExecution
  @ValidateDBTypes
  def get( self, args, kwargs ):
    #return { 'OK' : True, 'Value' : '' }
    return self.mm.get( *args, **kwargs )
  
  @CheckDBExecution
  @ValidateDBTypes
  def delete( self, args, kwargs ):
    #return { 'OK' : True, 'Value' : '' }
    return self.mm.delete( *args, **kwargs )
  
  @CheckDBExecution
  def getSchema( self ):
    return { 'OK': True, 'Value' : self.mm.SCHEMA }
    #return { 'OK' : True, 'Value' : {} }
  
  @CheckDBExecution
  def inspectSchema( self ):
    #return { 'OK' : True, 'Value' : self.mm.mSchema }
    return { 'OK': True, 'Value' : self.mm.mSchema }
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 