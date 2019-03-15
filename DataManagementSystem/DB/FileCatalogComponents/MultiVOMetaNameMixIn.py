""" DIRAC Multi VO MixIn class to manage file metadata and directory for multiple VO.
"""
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.MetaNameMixIn import MetaNameMixIn
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class MultiVOMetaNameMixIn(MetaNameMixIn):
  """
  MULti-VO MetaName MixIn implementation.
  """

  def getMetaName(self, meta, credDict):
    """
    Return a fully-qualified metadata name based on client-suplied metadata name and
    client credentials. User VO is added to the metadata passed in.

    :param meta: metadata name
    :param credDict: client credentials
    :return: fully-qualified metadata name
    """

    return meta + self.getMetaNameSuffix(credDict)

  def getMetaNameSuffix(self, credDict):
    """
    Get a VO specific suffix from user credentials.

    :param credDict: user credentials
    :return: VO specific suffix
    """
    vo = Registry.getGroupOption(credDict['group'], 'VO')
    return '_' + vo.replace('-', '_')

  def stripSuffix(self, metaDict, credDict):
    """
    Strip the suffix from all keys which contain it, removing all other keys.

    :param metaDict: original dict
    :param credDict: user credential dictionary
    :return: a new dict with modified keys
    """

    suffix = self.getMetaNameSuffix(credDict)
    smetaDict = {key.rsplit(suffix, 1)[0]: value for key, value in metaDict.iteritems()
                 if key.endswith(suffix)}
    return smetaDict
