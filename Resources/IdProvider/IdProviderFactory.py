########################################################################
# File :   IdProviderFactory.py
# Author : A.T.
########################################################################

"""  The Identtity Provider Factory instantiates IdProvider objects
     according to their configuration
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProviderInfo

__RCSID__ = "$Id$"


class IdProviderFactory(object):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('IdProviderFactory')

  #############################################################################
  def getIdProvider(self, idProvider, sessionManager=None):
    """ This method returns a IdProvider instance corresponding to the supplied
        name.

        :param basestring idProvider: the name of the Identity Provider

        :return: S_OK(IdProvider)/S_ERROR()
    """
    result = getProviderInfo(idProvider)
    if not result['OK']:
      return result
    pDict = result['Value']
    pDict['ProviderName'] = idProvider
    pType = pDict['ProviderType']

    self.log.verbose('Creating IdProvider of %s type with the name %s' % (pType, idProvider))
    subClassName = "%sIdProvider" % (pType)

    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject('Resources.IdProvider.%s' % subClassName, subClassName)
    if not result['OK']:
      self.log.error('Failed to load object', '%s: %s' % (subClassName, result['Message']))
      return result

    pClass = result['Value']
    try:
      provider = pClass()
      provider.setParameters(pDict)
      provider.setManager(sessionManager)
    except Exception as x:
      msg = 'IdProviderFactory could not instantiate %s object: %s' % (subClassName, str(x))
      self.log.exception()
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(provider)
