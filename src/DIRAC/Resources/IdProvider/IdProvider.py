""" IdProvider base class for various identity providers
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.AuthManagerClient import gSessionManager

__RCSID__ = "$Id$"


class IdProvider(object):

  def __init__(self, *args, **kwargs):  # parameters=None, sessionManager=None):
    """ C'or

        :param dict parameters: parameters of the identity Provider
        :param object sessionManager: session manager
    """
    self.log = gLogger.getSubLogger(self.__class__.__name__)
    self.parameters = kwargs.get('parameters', {})
    self.sessionManager = kwargs.get('sessionManager')
    self._initialization()

  def loadMetadata(self):
    """ Load metadata to cache if needed

        :return: S_OK()/S_ERROR()
    """
    return S_OK()

  def _initialization(self):
    """ Initialization """
    pass

  def setParameters(self, parameters):
    """ Set parameters

        :param dict parameters: parameters of the identity Provider
    """
    self.parameters = parameters

  def setManager(self, sessionManager):
    """ Set session manager

        :param object sessionManager: session manager
    """
    self.sessionManager = sessionManager

  def setLogger(self, logger):
    """ Set logger

        :param object logger: logger
    """
    self.log = logger

  def isSessionManagerAble(self):
    """ Check if session manager is available

        :return: S_OK()/S_ERROR()
    """
    if not self.sessionManager:
      try:
        #from DIRAC.FrameworkSystem.Client.AuthManagerClient import gSessionManager
        self.sessionManager = gSessionManager
      except Exception as e:
        return S_ERROR('Session manager is not available: %s' % e)
    return S_OK()

  def getTokenWithAuth(self, *args, **kwargs):
    """ Method to provide autherization flow on client side
    """
    return S_ERROR('getTokenWithAuth not implemented.')
