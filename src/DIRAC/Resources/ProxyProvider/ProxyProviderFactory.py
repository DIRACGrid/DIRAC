########################################################################
# File :   ProxyProviderFactory.py
# Author : A.T.
########################################################################

"""  The Proxy Provider Factory instantiates ProxyProvider objects
     according to their configuration
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getInfoAboutProviders

__RCSID__ = "$Id$"


class ProxyProviderFactory(object):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger(__name__)

  #############################################################################
  def getProxyProvider(self, proxyProvider):
    """ This method returns a ProxyProvider instance corresponding to the supplied
        name.

        :param str proxyProvider: the name of the Proxy Provider

        :return: S_OK(ProxyProvider)/S_ERROR()
    """
    if not proxyProvider:
      return S_ERROR('Provider name not set.')
    result = getInfoAboutProviders(of='Proxy', providerName=proxyProvider, option='all', section='all')
    if not result['OK']:
      return result
    ppDict = result['Value']
    ppDict['ProviderName'] = proxyProvider
    ppType = ppDict.get('ProviderType')
    if not ppType:
      return S_ERROR('Cannot find information for ProxyProvider %s' % proxyProvider)
    self.log.verbose('Creating ProxyProvider of %s type with the name %s' % (ppType, proxyProvider))
    subClassName = "%sProxyProvider" % (ppType)

    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject('Resources.ProxyProvider.%s' % subClassName, subClassName)
    if not result['OK']:
      self.log.error('Failed to load object', '%s: %s' % (subClassName, result['Message']))
      return result

    ppClass = result['Value']
    try:
      pProvider = ppClass()
      pProvider.setParameters(ppDict)
    except Exception as x:
      msg = 'ProxyProviderFactory could not instantiate %s object: %s' % (subClassName, str(x))
      self.log.exception()
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(pProvider)
