########################################################################
# File :   IdProviderFactory.py
# Author : A.T.
########################################################################

"""  The Identity Provider Factory instantiates IdProvider objects
     according to their configuration
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ObjectLoader, ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProviderInfo

__RCSID__ = "$Id$"


gCacheMetadata = ThreadSafe.Synchronizer()

class IdProviderFactory(object):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('IdProviderFactory')
    self.cacheMetadata = DictCache()

  @gCacheMetadata
  def getMetadata(self, idP):
    return self.cacheMetadata.get(idP)

  @gCacheMetadata
  def addMetadata(self, idP, data, time=24 * 3600):
    if data:
      self.cacheMetadata.add(idP, time, data)

  #############################################################################
  def getIdProvider(self, idProvider, sessionManager=None):
    """ This method returns a IdProvider instance corresponding to the supplied
        name.

        :param str idProvider: the name of the Identity Provider
        :param object sessionManager: session manager

        :return: S_OK(IdProvider)/S_ERROR()
    """
    if isinstance(idProvider, dict):
      pDict = idProvider
    else:
      result = getProviderInfo(idProvider)
      if not result['OK']:
        self.log.error('Failed to read configuration', '%s: %s' % (idProvider, result['Message']))
        return result
      pDict = result['Value']
      pDict['ProviderName'] = idProvider
    pDict['sessionManager'] = sessionManager
    pType = pDict['ProviderType']

    self.log.verbose('Creating IdProvider of %s type with the name %s' % (pType, idProvider))
    subClassName = "%sIdProvider" % (pType)

    result = ObjectLoader().loadObject('Resources.IdProvider.%s' % subClassName)
    if not result['OK']:
      self.log.error('Failed to load object', '%s: %s' % (subClassName, result['Message']))
      return result

    pClass = result['Value']
    try:
      meta = self.getMetadata(idProvider)
      if meta:
        pDict.update(meta)
      provider = pClass(**pDict)
      if not meta and hasattr(provider, 'metadata'):
        # result = provider.loadMetadata()
        # if not result['OK']:
        #   return result
        # self.addMetadata(idProvider, result['Value'])
        self.addMetadata(idProvider, provider.metadata)
      # provider.setParameters(pDict)
      # provider.setManager(sessionManager)
    except Exception as x:
      msg = 'IdProviderFactory could not instantiate %s object: %s' % (subClassName, str(x))
      self.log.exception()
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(provider)
