# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/SessionManager.py,v 1.4 2007/09/20 18:34:16 acasajus Exp $
__RCSID__ = "$Id: SessionManager.py,v 1.4 2007/09/20 18:34:16 acasajus Exp $"

import OpenSSL

class SessionManager:

  def __init__( self ):
    self.sessionsDict = {}
    self.contextsDict = {}

  def __generateSession( self ):
    return OpenSSL.SSL.Session()

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

gSessionManager = SessionManager()