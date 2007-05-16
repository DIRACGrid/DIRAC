# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/Attic/ContextManager.py,v 1.1 2007/05/16 10:06:58 acasajus Exp $
__RCSID__ = "$Id: ContextManager.py,v 1.1 2007/05/16 10:06:58 acasajus Exp $"

import OpenSSL

class ContextManager:

  def __init__( self ):
    self.sessionsDict = {}
    self.contextsDict = {}

  def __generateSession( self ):
    return SSL.Session()

  def getSession( self, sessionId ):
    if sessionId not in self.sessionsDict:
      self.sessionsDict[ sessionId ] = self.__generateSession()
    return self.sessionsDict[ sessionId ]

  def isValidSession( self, sessionId ):
    return sessionId in self.sessionsDict and self.sessionsDict[ sessionId ].valid()

  def freeSession( self, sessionId ):
    self.sessionsDict[ sessionId ].free()

  def setSession( self, sessionId, sessionObject ):
    self.sessionsDict[ sessionId ] = sessionObject

  def existsContext( self, contextId ):
    return contextId in self.contextsDict

  def setContext( self, contextId, context ):
    self.contextsDict[ contextId ] = context

  def getContext( self, contextId ):
    return self.contextsDict[ contextId ]

gContextManager = ContextManager()