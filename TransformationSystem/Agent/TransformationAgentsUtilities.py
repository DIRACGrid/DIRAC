from DIRAC import gLogger

AGENT_NAME = ''

class TransformationAgentsUtilities( object ):

  def __logVerbose( self, message, param = '', method = "execute", transID = 'None' ):
    """ verbose """
    gLogger.verbose( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logDebug( self, message, param = '', method = "execute", transID = 'None' ):
    """ debug """
    gLogger.debug( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logInfo( self, message, param = '', method = "execute", transID = 'None' ):
    """ info """
    gLogger.info( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logWarn( self, message, param = '', method = "execute", transID = 'None' ):
    """ warn """
    gLogger.warn( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logError( self, message, param = '', method = "execute", transID = 'None' ):
    """ error """
    gLogger.error( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logException( self, message, param = '', method = "execute", transID = 'None' ):
    """ exception """
    gLogger.exception( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

