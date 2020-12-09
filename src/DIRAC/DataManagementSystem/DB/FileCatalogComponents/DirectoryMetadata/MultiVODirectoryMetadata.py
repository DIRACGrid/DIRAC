""" DIRAC Multi VO FileCatalog plugin class to manage directory metadata for multiple VO.
"""
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryMetadata.DirectoryMetadata import DirectoryMetadata

VO_SUFFIX_SEPARATOR = "___"

# Metadata names mangling utilities


def _getMetaName(meta, credDict):
  """
  Return a fully-qualified metadata name based on client-supplied metadata name and
  client credentials. User VO is added to the metadata passed in.

  :param meta: metadata name
  :param credDict: client credentials
  :return: fully-qualified metadata name
  """

  return meta + _getMetaNameSuffix(credDict)


def _getMetaNameDict(metaDict, credDict):
  """
  Return a dictionary with fully-qualified metadata name keys based on client-supplied metadata name and
  client credentials. User VO is added to the metadata passed in.

  :param meta: metadata name
  :param credDict: client credentials
  :return: fully-qualified metadata name
  """

  fMetaDict = {}
  for meta, value in metaDict.items():
    fMetaDict[_getMetaName(meta, credDict)] = value
  return fMetaDict


def _getMetaNameSuffix(credDict):
  """
  Get a VO specific suffix from user credentials.

  :param credDict: user credentials
  :return: VO specific suffix
  """
  vo = Registry.getGroupOption(credDict['group'], 'VO')
  return VO_SUFFIX_SEPARATOR + vo.replace('-', '_').replace('.', '_')


def _stripSuffix(metaDict, credDict):
  """
  Strip the suffix from all keys which contain it, removing all other keys.

  :param metaDict: original dict
  :param credDict: user credential dictionary
  :return: a new dict with modified keys
  """

  suffix = _getMetaNameSuffix(credDict)
  smetaDict = {key.rsplit(suffix, 1)[0]: value for key, value in metaDict.items()
               if key.endswith(suffix)}
  return smetaDict


class MultiVODirectoryMetadata(DirectoryMetadata):
  """
  Multi-VO FileCatalog plugin implementation.
  """

  def __init__(self, database=None):
    super(MultiVODirectoryMetadata, self).__init__(database=database)

  def addMetadataField(self, pName, pType, credDict):
    """
    Add a new metadata parameter to the Metadata Database.
    Modified to use fully qualified metadata names.

    :param str pName: parameter name
    :param str pType: parameter type in the MySQL notation
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    fname = _getMetaName(pName, credDict)
    return super(MultiVODirectoryMetadata, self).addMetadataField(fname, pType, credDict)

  def deleteMetadataField(self, pName, credDict):
    """ Remove metadata field.
        Table name is now fully qualified

        :param str pName: parameter name
        :param dict credDict: client credential dictionary
        :return: standard Dirac result object
    """
    fname = _getMetaName(pName, credDict)
    return super(MultiVODirectoryMetadata, self).deleteMetadataField(fname, credDict)

  def getMetadataFields(self, credDict):
    """ Get all the defined metadata fields

        :param dict credDict: client credential dictionary
        :return: standard Dirac result object
    """
    result = super(MultiVODirectoryMetadata, self)._getMetadataFields(credDict)
    if not result['OK']:
      return result

    metaDict = _stripSuffix(result['Value'], credDict)
    return S_OK(metaDict)

  def setMetadata(self, dPath, metaDict, credDict):
    """ Set the value of a given metadata field for the the given directory path

        :param str dPath: directory path
        :param dict metaDict: dictionary with the user metadata
        :param dict credDict: client credential dictionary

        :return: standard Dirac result object
    """
    fMetaDict = _getMetaNameDict(metaDict, credDict)
    return super(MultiVODirectoryMetadata, self).setMetadata(dPath, fMetaDict, credDict)

  def removeMetadata(self, dPath, metaList, credDict):
    """
    Remove the specified metadata for the given directory for users own VO.

    :param str dPath: directory path
    :param dict metaList: metadata names list
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    metaList = [_getMetaName(meta, credDict) for meta in metaList]
    result = super(MultiVODirectoryMetadata, self).removeMetadata(dPath, metaList, credDict)
    if not result['OK']:
      if "FailedMetadata" in result:
        failedDict = _stripSuffix(result['FailedMetadata'], credDict)
        result['FailedMetadata'] = failedDict
      return result

    return S_OK()

  def setMetaParameter(self, dPath, metaName, metaValue, credDict):
    """
        Set an meta parameter - metadata which is not used in the the data
        search operations.

        :param str dPath: directory path
        :param str metaName: metadata name
        :param str metaValue: metadata value
        :param dict credDict: client credential dictionary
        :return: standard Dirac result object
    """
    fname = _getMetaName(metaName, credDict)
    return super(MultiVODirectoryMetadata, self).setMetaParameter(dPath, fname, metaValue, credDict)

  def getDirectoryMetadata(self, path, credDict, inherited=True, ownData=True):
    """
    Get metadata for the given directory aggregating metadata for the directory itself
    and for all the parent directories if inherited flag is True. Get also the non-indexed
    metadata parameters.

    :param str path: directory path
    :param dict credDict: client credential dictionary
    :param bool inherited: include parent directories if True
    :param bool ownData:
    :return: standard Dirac result object + additional MetadataOwner \
    and MetadataType dict entries if the operation is successful.
    """

    result = super(MultiVODirectoryMetadata, self).getDirectoryMetadata(path, credDict, inherited, ownData)
    if not result['OK']:
      return result

    # Strip off the VO suffix
    result['Value'] = _stripSuffix(result['Value'], credDict)
    result['MetadataOwner'] = _stripSuffix(result['MetadataOwner'], credDict)
    result['MetadataType'] = _stripSuffix(result['MetadataType'], credDict)

    return result

  def findDirIDsByMetadata(self, metaDict, dPath, credDict):
    """ Find Directories satisfying the given metadata and being subdirectories of
        the given path
    """
    fMetaDict = _getMetaNameDict(metaDict, credDict)
    return super(MultiVODirectoryMetadata, self).findDirIDsByMetadata(fMetaDict, dPath, credDict)
