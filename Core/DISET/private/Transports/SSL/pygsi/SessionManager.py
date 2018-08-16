# $HeadURL$
__RCSID__ = "$Id$"

import GSI

class SessionManager:

  def __init__( self ):
    self.sessionsDict = {}

  def __generateSession( self ):
    return GSI.SSL.Session()

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