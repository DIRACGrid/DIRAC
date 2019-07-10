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
