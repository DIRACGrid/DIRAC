########################################################################
# File :   ProxyProviderFactory.py
# Author : A.T.
########################################################################

"""  The Proxy Provider Factory instantiates ProxyProvider objects
     according to their configuration
"""
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ObjectLoader
from DIRAC.Resources.ProxyProvider.ProxyProvider import getProxyProviderConfigDict

__RCSID__ = "$Id$"


class ProxyProviderFactory(object):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('ProxyProviderFactory')

  #############################################################################
  def getProxyProvider(self, proxyProvider):
    """This method returns a ProxyProvider instance corresponding to the supplied
       name.

       :param str proxyProvider: the name of the Proxy Provider

       :return: S_OK/S_ERROR with ProxyProvider object as Value
    """
    ppDict = getProxyProviderConfigDict(proxyProvider)
    ppDict['ProxyProviderName'] = proxyProvider
    ppType = ppDict.get('ProxyProviderType')
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
    except BaseException as x:
      msg = 'ProxyProviderFactory could not instantiate %s object: %s' % (subClassName, str(x))
      self.log.exception()
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(pProvider)
