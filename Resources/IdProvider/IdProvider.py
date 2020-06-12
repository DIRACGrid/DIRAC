""" IdProvider base class for various identity providers
"""

from DIRAC import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"


class IdProvider(object):

  def __init__(self, parameters=None, sessionManager=None):
    self.log = gLogger.getSubLogger(self.__class__.__name__)
    self.parameters = parameters
    self.sessionManager = sessionManager

  def setParameters(self, parameters):
    self.parameters = parameters
  
  def setManager(self, sessionManager):
    self.sessionManager = sessionManager

  def isSessionManagerAble(self):
    if not self.sessionManager:
      try:
        from OAuthDIRAC.FrameworkSystem.Client.OAuthManagerClient import gSessionManager
        self.sessionManager = gSessionManager
      except Exception as e:
        return S_ERROR('Session manager not able: %s' % e)
    return S_OK()
    