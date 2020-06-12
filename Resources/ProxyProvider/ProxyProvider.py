""" ProxyProvider base class for various proxy providers
"""
from DIRAC import S_OK, S_ERROR, gLogger

__RCSID__ = "$Id$"


class ProxyProvider(object):

  def __init__(self, parameters=None, proxyManager=None):
    """ C'or
    
        :param dict parameters: parameters of the Proxy Provider
        :param object proxyManager: proxy manager
    """
    self.log = gLogger.getSubLogger(self.__class__.__name__)
    self.parameters = parameters

  def setParameters(self, parameters):
    """ Set parameters

        :param dict parameters: parameters of the proxy Provider
    """
    self.parameters = parameters
  
  def setManager(self, proxyManager):
    """ Set proxy manager

        :param object proxyManager: proxy manager
    """
    self.sessionManager = sessionManager

  def isProxyManagerAble(self):
    """ Check if proxy manager able

        :return: S_OK()/S_ERROR()
    """
    if not self.proxyManager:
      try:
        from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
        self.proxyManager = ProxyManagerClient()
      except Exception as e:
        return S_ERROR('Proxy manager not able: %s' % e)
    return S_OK()

  def checkStatus(self, userDN):
    """ Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK()/S_ERROR()
    """
    return S_OK()

  def generateDN(self, **kwargs):
    """ Generate new DN

        :param dict kwargs: user description dictionary

        :return: S_OK(str)/S_ERROR() -- contain DN
    """
    return S_ERROR("Not implemented in %s", self.name)
