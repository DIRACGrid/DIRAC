''' Utility Class for threaded agents (e.g. TransformationAgent)
    Mostly for logging
'''

from DIRAC import gLogger

__RCSID__ = "$Id$"

AGENT_NAME = ''

class TransformationAgentsUtilities( object ):
  ''' logging utilities for threaded TS agents
  '''

  def __init__( self ):
    ''' c'tor
    '''
    self.transInThread = {}
    self.debug = False

  def __threadForTrans( self, transID ):
    try:
      return self.transInThread.get( transID, ' [None] [None] ' ) + AGENT_NAME + '.'
    except NameError:
      return ''

  def _logVerbose( self, message, param = '', method = "execute", transID = 'None' ):
    ''' verbose '''
    if self.debug:
      gLogger.info( '(V) ' + self.__threadForTrans( transID ) + method + ' ' + message, param )
    else:
      gLogger.verbose( self.__threadForTrans( transID ) + method + ' ' + message, param )

  def _logDebug( self, message, param = '', method = "execute", transID = 'None' ):
    ''' debug '''
    gLogger.debug( self.__threadForTrans( transID ) + method + ' ' + message, param )

  def _logInfo( self, message, param = '', method = "execute", transID = 'None' ):
    ''' info '''
    gLogger.info( self.__threadForTrans( transID ) + method + ' ' + message, param )

  def _logWarn( self, message, param = '', method = "execute", transID = 'None' ):
    ''' warn '''
    gLogger.warn( self.__threadForTrans( transID ) + method + ' ' + message, param )

  def _logError( self, message, param = '', method = "execute", transID = 'None' ):
    ''' error '''
    gLogger.error( self.__threadForTrans( transID ) + method + ' ' + message, param )

  def _logException( self, message, param = '', lException = False, method = "execute", transID = 'None' ):
    ''' exception '''
    gLogger.exception( self.__threadForTrans( transID ) + method + ' ' + message, param, lException )
