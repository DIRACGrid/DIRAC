""" ProxyProvider base class for various proxy providers
"""

from DIRAC import gConfig

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


def getProxyProviderConfigDict(ppName):
  """ Get the proxy provider configuration parameters

      :param str ppName: proxy provider name

      :return: dict
  """
  ppConfigDict = {}
  if ppName:
    result = gConfig.getOptionsDict('/Resources/ProxyProviders/%s' % ppName)
    if result['OK']:
      ppConfigDict = result['Value']
  return ppConfigDict
