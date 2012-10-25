from DIRAC import gLogger

class TransformationAgentsUtilities( object ):

  def __init__( self ):
    self.agentName = ''

  def _logVerbose( self, message, param = '', method = "execute", transID = 'None' ):
    """ verbose """
    gLogger.verbose( self.agentName + "." + method + ": [%s] " % str( transID ) + message, param )

  def _logDebug( self, message, param = '', method = "execute", transID = 'None' ):
    """ debug """
    gLogger.debug( self.agentName + "." + method + ": [%s] " % str( transID ) + message, param )

  def _logInfo( self, message, param = '', method = "execute", transID = 'None' ):
    """ info """
    gLogger.info( self.agentName + "." + method + ": [%s] " % str( transID ) + message, param )

  def _logWarn( self, message, param = '', method = "execute", transID = 'None' ):
    """ warn """
    gLogger.warn( self.agentName + "." + method + ": [%s] " % str( transID ) + message, param )

  def _logError( self, message, param = '', method = "execute", transID = 'None' ):
    """ error """
    gLogger.error( self.agentName + "." + method + ": [%s] " % str( transID ) + message, param )

  def _logException( self, message, param = '', method = "execute", transID = 'None' ):
    """ exception """
    gLogger.exception( self.agentName + "." + method + ": [%s] " % str( transID ) + message, param )

