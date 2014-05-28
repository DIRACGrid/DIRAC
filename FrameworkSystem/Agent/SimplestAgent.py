""" :mod: SimplestAgent

Simplest Agent send a simple log message
"""
 
# # imports
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient


__RCSID__ = "Id: $"

class SimplestAgent( AgentModule ):
  """
  .. class:: SimplestAgent

  Simplest agent
  print a message on log
  """

  def initialize( self ):
    """ agent's initalisation
  
    :param self: self reference
    """
    self.message = self.am_getOption( 'Message', "SimplestAgent is working..." )
    self.log.info( "message = %s" % self.message )
    return S_OK()
  
  def execute( self ):
    """ execution in one agent's cycle
  
    :param self: self reference
    """
    self.log.info( "message is: %s" % self.message )
    simpleMessageService = RPCClient( 'Framework/Hello' )
    result = simpleMessageService.sayHello( self.message )
    if not result['OK']:
      self.log.error( "Error while calling the service: %s" % result['Message'] )
      return result
    self.log.info( "Result of the request is %s" % result[ 'Value' ])
    return S_OK()   
