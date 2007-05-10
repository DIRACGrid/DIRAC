# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/SessionManager.py,v 1.2 2007/05/10 18:44:58 acasajus Exp $
__RCSID__ = "$Id: SessionManager.py,v 1.2 2007/05/10 18:44:58 acasajus Exp $"

import OpenSSL

class SessionManager:

  def __init__( self ):
    self.sessionsDict = {}

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

