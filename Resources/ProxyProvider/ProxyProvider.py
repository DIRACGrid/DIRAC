""" ProxyProvider base class for various proxy providers
"""

__RCSID__ = "$Id$"


class ProxyProvider(object):

  def __init__(self, parameters=None):

    self.parameters = parameters
    self.name = None
    if parameters:
      self.name = parameters.get('ProxyProviderName')

  def setParameters(self, parameters):
    self.parameters = parameters
    self.name = parameters.get('ProxyProviderName')

  def checkStatus(self, userDN):
    """ Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dictionary contain fields:
                  - 'Status' with ready to work status[ready, needToAuth]
    """
    return S_OK({'Status': 'ready'})
  
  def generateDN(self, **kwargs):
    """ Generate new DN

        :param dict kwargs: user description dictionary

        :return: S_OK(str)/S_ERROR() -- contain DN
    """
    return S_ERROR('%s work only with ready user DN.')
