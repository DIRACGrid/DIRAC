""" DIRAC base MixIn class to manage file metadata and directory names.
"""
from __future__ import division


class MetaNameMixIn(object):

  def getMetaName(self, meta, credDict):
    """
    Return a metadata name based on client supplied meta name and client credentials
    For the base class it just returns the name passed in.
    This method is a pass-through and is meant to be overwritten by derived classes.

    :param meta:  meta name
    :param credDict: client credentials
    :return: meta name
    """

    return meta

  def getMetaNameSuffix(self, credDict):
    """
    Get meta name suffix based on client credentials. The method is needed to be able
    to return metadata w/o a suffix to the client.
    This method is a pass-through and is meant to be overwritten by derived classes.

    :param credDict: client credentials
    :return: the suffix. And empty string for a base class.
    """

    return ''

  def stripSuffix(self, metaDict, credDict):
    """
    Strip suffix pass through, just return the metadata dictionary.

    :param metaDict: meta dictionary to modify.
    :param credDict: credential dictionary.
    :return: unchanged metaDict
    """
    return metaDict
