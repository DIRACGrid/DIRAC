# $HeadURL $
''' ResourceManagementHandler

  Module that allows users to access the ResourceManagementDB remotely.

'''

from DIRAC                                              import gConfig, S_OK, gLogger
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Utilities               import Synchronizer

__RCSID__ = '$Id: $'
db        = False

def initializeResourceManagementHandler( _serviceInfo ):
  '''
    Handler initialization, where we set the ResourceManagementDB as global db.
  '''
  
  global db
  db = ResourceManagementDB()
  # Regenerates DB tables if needed  
  db._checkTable()

  syncObject = Synchronizer.Synchronizer()
  gConfig.addListenerToNewVersionEvent( syncObject.sync )

  return S_OK()

################################################################################

class ResourceManagementHandler( RequestHandler ):
  '''
  The ResourceManagementHandler exposes the DB front-end functions through a
  XML-RPC server, functionalities inherited from :class:`DIRAC.Core.DISET.Reques\
  tHandler.RequestHandler`
  
  According to the ResourceManagementDB philosophy, only functions of the type:
  - insert
  - update
  - select
  - delete 
  
  are exposed. If you need anything more complicated, either look for it on the 
  :class:`ResourceManagementClient`, or code it yourself. This way the DB and the 
  Service are kept clean and tidied.

  To can use this service on this way, but you MUST NOT DO IT. Use it through the
  :class:`ResourceManagementClient`. If offers in the worst case as good perfor\
  mance as the :class:`ResourceManagementHandler`, if not better.

   >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
   >>> server = RPCCLient("ResourceStatus/ResourceManagement")
  '''
  
  def __init__( self, *args, **kwargs ):
    super( ResourceManagementHandler, self ).__init__( *args, **kwargs )
  
  @staticmethod
  def __logResult( methodName, result ):
    '''
      Method that writes to log error messages 
    '''
    
    if not result[ 'OK' ]:
      gLogger.error( '%s%s' % ( methodName, result[ 'Message' ] ) )

  @staticmethod
  def setDatabase( database ):
    '''
    This method let us inherit from this class and overwrite the database object
    without having problems with the global variables.

    :Parameters:
      **database** - `MySQL`
        database used by this handler

    :return: None
    '''
    global db
    db = database

  types_insert = [ dict, dict ]
  def export_insert( self, params, meta ):
    '''   
    This method is a bridge to access :class:`ResourceManagementDB` remotely. It 
    does not add neither processing nor validation. If you need to know more 
    about this method, you must keep reading on the database documentation.     
      
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    
      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    
    gLogger.info( 'insert: %s %s' % ( params, meta ) )
    
    res = db.insert( params, meta )
    self.__logResult( 'insert', res )
    
    return res   

  types_update = [ dict, dict ]
  def export_update( self, params, meta ):
    '''   
    This method is a bridge to access :class:`ResourceManagementDB` remotely. It 
    does not add neither processing nor validation. If you need to know more 
    about this method, you must keep reading on the database documentation.     
      
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    
      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''      

    gLogger.info( 'update: %s %s' % ( params, meta ) )
    
    res = db.update( params, meta )
    self.__logResult( 'update', res )
    
    return res  

  types_select = [ dict, dict ]
  def export_select( self, params, meta ):
    '''
    This method is a bridge to access :class:`ResourceManagementDB` remotely. 
    It does not add neither processing nor validation. If you need to know more\ 
    about this method, you must keep reading on the database documentation.     
      
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    
      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''      
    
    gLogger.info( 'select: %s %s' % ( params, meta ) )
    
    
    res = db.select( params, meta )
    self.__logResult( 'select', res )
    
    return res  

  types_delete = [ dict, dict ]
  def export_delete( self, params, meta ):
    '''   
    This method is a bridge to access :class:`ResourceManagementDB` remotely.\
    It does not add neither processing nor validation. If you need to know more \
    about this method, you must keep reading on the database documentation.     
      
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    
      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''         

    gLogger.info( 'delete: %s %s' % ( params, meta ) )
    
    res = db.delete( params, meta )
    self.__logResult( 'delete', res )
    
    return res  

  types_addOrModify = [ dict, dict ]
  def export_addOrModify( self, params, meta ):
    '''
    This method is a bridge to access :class:`ResourceManagementDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).

      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''

    gLogger.info( 'addOrModify: %s %s' % ( params, meta ) )
    
    res = db.addOrModify( params, meta )
    self.__logResult( 'addOrModify', res )
    
    return res  
  
  types_addIfNotThere = [ dict, dict ]
  def export_addIfNotThere( self, params, meta ):
    '''
    This method is a bridge to access :class:`ResourceManagementDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).

      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''

    gLogger.info( 'addIfNotThere: %s %s' % ( params, meta ) )
    
    res = db.addIfNotThere( params, meta )
    self.__logResult( 'addIfNotThere', res )
    
    return res  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF