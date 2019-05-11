""" ProxyProvider base class for various proxy providers
"""

from DIRAC import gConfig

__RCSID__ = "$Id$"


class ProxyProvider(object):

  def __init__(self, parameters=None):

    self.parameters = parameters

  def setParameters(self, parameters):
    self.parameters = parameters


def getProxyProviderConfigDict(ppName):
  """ Get the proxy provider configuration parameters

      :param str ppName: proxy provider name
  """
  ppConfigDict = {}
  if ppName:
    result = gConfig.getOptionsDict('/Resources/ProxyProviders/%s' % ppName)
    if result['OK']:
      ppConfigDict = result['Value']
  return ppConfigDict
