# $HeadURL:  $
''' Command

  Base class for all commands.

'''

from DIRAC import S_OK

__RCSID__ = '$Id:  $'

class Command( object ):
  ''' 
    The Command class is a simple base class for all the commands
    for interacting with the clients
  '''

  def __init__( self, args = None, clients = None ):
    
    self.args       = ( 1 and args ) or {}      
    self.apis       = ( 1 and clients ) or {}
    self.masterMode = False
    self.metrics    = { 'failed' : [] }

  def doNew( self, masterParams = None ):
    ''' To be extended by real commands
    '''   
    return S_OK( { 'Result' : None } )
  
  def doCache( self ):
    ''' To be extended by real commands
    '''
    return S_OK( { 'Result' : None } )

  def doMaster( self ):
    ''' To be extended by real commands
    '''
    return S_OK( self.metrics )   
      
  def doCommand( self ):
    ''' To be extended by real commands
    '''
    
    if self.masterMode:
      return self.returnSObj( self.doMaster() )
          
    result = self.doCache()
    if not result[ 'OK' ]:
      return self.returnERROR( result )
    if result[ 'Value' ]:
      return result
    
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