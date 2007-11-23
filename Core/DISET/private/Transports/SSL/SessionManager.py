# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/SessionManager.py,v 1.5 2007/11/23 10:35:46 acasajus Exp $
__RCSID__ = "$Id: SessionManager.py,v 1.5 2007/11/23 10:35:46 acasajus Exp $"

import OpenSSL

class SessionManager:

  def __init__( self ):
    self.sessionsDict = {}

  def __generateSession( self ):
    return OpenSSL.SSL.Session()

  def get( self, sessionId ):
    if sessionId not in self.sessionsDict:
      self.sessionsDict[ sessionId ] = self.__generateSession()
    return self.sessionsDict[ sessionId ]

  def isValid( self, sessionId ):
    return sessionId in self.sessionsDict and self.sessionsDict[ sessionId ].valid()

  def free( self, sessionId ):
    self.sessionsDict[ sessionId ].free()

  def set( self, sessionId, sessionObject ):
    self.sessionsDict[ sessionId ] = sessionObject

gSessionManager = SessionManager()