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

import jwt

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ObjectLoader, ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProviderInfo, getSettingsNamesForIdPIssuer
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthorizationServerMetadata
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import DEFAULT_CLIENTS

__RCSID__ = "$Id$"

gCacheMetadata = ThreadSafe.Synchronizer()


class IdProviderFactory(object):

  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('IdProviderFactory')
    self.cacheMetadata = DictCache()

  @gCacheMetadata
  def getMetadata(self, idP):
    return self.cacheMetadata.get(idP) or {}

  @gCacheMetadata
  def addMetadata(self, idP, data, time=24 * 3600):
    if data:
      self.cacheMetadata.add(idP, time, data)

  def getIdProviderForToken(self, token):
    """ This method returns a IdProvider instance corresponding to the supplied
        issuer in a token.

        :param str token: token

        :return: S_OK(IdProvider)/S_ERROR()
    """
    data = {}

    # Read token without verification to get issuer
    issuer = jwt.decode(token, options=dict(verify_signature=False))['iss'].strip('/')

    result = getSettingsNamesForIdPIssuer(issuer)
    if result['OK']:
      return self.getIdProvider(result['Value'][0])

    _result = getAuthorizationServerMetadata()
    if not _result['OK']:
      return _result
    if issuer == _result['Value'].get('issuer', '').strip('/'):
      return self.getIdProvider(DEFAULT_CLIENTS.keys()[0])

    return result

  def getIdProvider(self, name, **kwargs):
    """ This method returns a IdProvider instance corresponding to the supplied
        name.

        :param str name: the name of the Identity Provider

        :return: S_OK(IdProvider)/S_ERROR()
    """
    self.log.debug('Search %s configuration..' % name)
    pDict = DEFAULT_CLIENTS.get(name, {})
    if pDict:
      result = getAuthorizationServerMetadata()
      if not result['OK']:
        return result
      pDict.update(result['Value'])
    pDict.update(kwargs)

    result = getProviderInfo(name)
    if not result['OK']:
      if not pDict:
        self.log.error('Failed to read configuration', '%s: %s' % (name, result['Message']))
        return result
      gLogger.debug(result['Message'])
    else:
      pDict.update(result['Value'])
    pDict['ProviderName'] = name

    pType = pDict['ProviderType']

    self.log.verbose('Creating IdProvider of %s type with the name %s' % (pType, name))
    subClassName = "%sIdProvider" % (pType)

    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject('Resources.IdProvider.%s' % subClassName, subClassName)
    if not result['OK']:
      self.log.error('Failed to load object', '%s: %s' % (subClassName, result['Message']))
      return result

    pClass = result['Value']
    try:
      provider = pClass(**pDict)
    except Exception as x:
      msg = 'IdProviderFactory could not instantiate %s object: %s' % (subClassName, str(x))
      self.log.exception()
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(provider)
