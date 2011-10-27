################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB 
       
class ResourceStatusClient:
  """
  The :class:`ResourceStatusClient` class exposes the :mod:`DIRAC.ResourceStatus` 
  API. All functions you need are on this client.
  
  It has the 'direct-db-access' functions, the ones of the type:
   - insert
   - update
   - get
   - delete 
  
  plus a set of functions of the type:
   - getValid 
    
  that return parts of the RSSConfiguration stored on the CS, and used everywhere
  on the RSS module. Finally, and probably more interesting, it exposes a set
  of functions, badly called 'boosters'. They are 'home made' functions using the
  basic database functions that are interesting enough to be exposed.  
  
  The client will ALWAYS try to connect to the DB, and in case of failure, to the
  XML-RPC server ( namely :class:`ResourceStatusDB` and :class:`ResourceStatusHancler` ).

  You can use this client on this way

   >>> from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
   >>> rsClient = ResourceStatusClient()
   
  All functions calling methods exposed on the database or on the booster are 
  making use of some syntactic sugar, in this case a decorator that simplifies
  the client considerably.    
  """

  def __init__( self , serviceIn = None ):
    '''
      The client tries to connect to :class:ResourceStatusDB by default. If it 
      fails, then tries to connect to the Service :class:ResourceStatusHandler.
    '''
 
    if serviceIn == None:
      try:
        self.gate = ResourceStatusDB()
      except Exception:
        self.gate = RPCClient( "ResourceStatus/ResourceStatus" )
    else:
      self.gate = serviceIn
      
  def insert( self, *args, **kwargs ):
    return self.gate.insert( args, kwargs )

  def update( self, *args, **kwargs ):
    return self.gate.update( args, kwargs )

  def get( self, *args, **kwargs ):
    return self.gate.get( args, kwargs )

  def delete( self, *args, **kwargs ):
    return self.gate.delete( args, kwargs )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    