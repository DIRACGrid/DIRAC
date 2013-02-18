# $HeadURL:  $
''' Command

  Base class for all commands.

'''

from DIRAC import gLogger, S_OK

__RCSID__ = '$Id:  $'

class Command( object ):
  ''' 
    The Command class is a simple base class for all the commands
    for interacting with the clients
  '''

  def __init__( self, args = None, clients = None ):
          
    self.apis       = ( 1 and clients ) or {}
    self.masterMode = False
    self.onlyCache  = False
    self.metrics    = { 'failed' : [] }
    
    self.args       = { 'onlyCache' : False }
    _args           = ( 1 and args ) or {}
    self.args.update( _args )

  def doNew( self, masterParams = None ):
    ''' To be extended by real commands
    '''   
    return S_OK( ( self.args, masterParams ) )
  
  def doCache( self ):
    ''' To be extended by real commands
    '''
    return S_OK( self.args )

  def doMaster( self ):
    ''' To be extended by real commands
    '''
    return S_OK( self.metrics )   
      
  def doCommand( self ):
    ''' To be extended by real commands
    '''
    
    if self.masterMode:
      gLogger.verbose( 'doMaster')
      return self.returnSObj( self.doMaster() )
    
    gLogger.verbose( 'doCache' )      
    result = self.doCache()
    if not result[ 'OK' ]:
      return self.returnERROR( result )
    # We may be interested on running the commands only from the cache,
    # without requesting new values. 
    if result[ 'Value' ] or self.args[ 'onlyCache' ]:
      return result
    
    gLogger.verbose( 'doNew' )
    return self.returnSObj( self.doNew() )
      
  def returnERROR( self, s_obj ):
    '''
      Overwrites S_ERROR message with command name, much easier to debug
    '''
    
    s_obj[ 'Message' ] = '%s %s' % ( self.__class__.__name__, s_obj[ 'Message' ] )
    return s_obj
  
  def returnSObj( self, s_obj ):
    '''
      Overwrites S_ERROR message with command name, much easier to debug
    '''
    
    if s_obj[ 'OK' ]:
      return s_obj
    
    return self.returnERROR( s_obj )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF