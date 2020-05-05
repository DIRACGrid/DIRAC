""" DIRAC Multi VO FileCatalog plugin class to manage file metadata for multiple VO.
"""
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileMetadata.FileMetadata import FileMetadata
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryMetadata.MultiVODirectoryMetadata import \
    _getMetaName, _getMetaNameDict, _stripSuffix


class MultiVOFileMetadata(FileMetadata):
  """
  Multi-VO FileCatalog plugin implementation.
  """

  def __init__(self, database=None):
    super(MultiVOFileMetadata, self).__init__(database=database)

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
    return super(MultiVOFileMetadata, self).addMetadataField(fname, pType, credDict)

  def deleteMetadataField(self, pName, credDict):
    """ Remove metadata field.
        Table name is now fully qualified

        :param str pName: parameter name
        :param dict credDict: client credential dictionary
        :return: standard Dirac result object
    """
    fname = _getMetaName(pName, credDict)
    return super(MultiVOFileMetadata, self).deleteMetadataField(fname, credDict)

  def getFileMetadataFields(self, credDict):
    """ Get all the defined metadata fields

        :param dict credDict: client credential dictionary
        :return: standard Dirac result object
    """
    result = super(MultiVOFileMetadata, self)._getFileMetadataFields(credDict)
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
    return super(MultiVOFileMetadata, self).setMetadata(dPath, fMetaDict, credDict)

  def removeMetadata(self, dPath, metaList, credDict):
    """
    Remove the specified metadata for the given directory for users own VO.

    :param str dPath: directory path
    :param dict metaList: metadata names list
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    metaList = [_getMetaName(meta, credDict) for meta in metaList]
    result = super(MultiVOFileMetadata, self).removeMetadata(dPath, metaList, credDict)
    if not result['OK']:
      if "FailedMetadata" in result:
        failedDict = _stripSuffix(result['FailedMetadata'], credDict)
        result['FailedMetadata'] = failedDict
      return result

    return S_OK()

  def setFileMetaParameter(self, path, metaName, metaValue, credDict):
    """
        Set an meta parameter - metadata which is not used in the the data
        search operations.

        :param str path: directory path
        :param str metaName: metadata name
        :param str metaValue: metadata value
        :param dict credDict: client credential dictionary
        :return: standard Dirac result object
    """
    fName = _getMetaName(metaName, credDict)
    return super(MultiVOFileMetadata, self).setFileMetaParameter(path, fName, metaValue, credDict)

  def getFileUserMetadata(self, path, credDict):
    """
    Get metadata for the given file.

    :param str path:  file path
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    result = super(MultiVOFileMetadata, self).getFileUserMetadata(path, credDict)
    if not result['OK']:
      return result

    result['Value'] = _stripSuffix(result['Value'], credDict)
    return result

  def findFilesByMetadata(self, metaDict, path, credDict):
    """ Find Files satisfying the given metadata

        :param dict metaDict: dictionary with the metaquery parameters
        :param str path: Path to search into
        :param dict credDict: Dictionary with the user credentials

        :return: S_OK/S_ERROR, Value ID:LFN dictionary of selected files
    """

    fMetaDict = _getMetaNameDict(metaDict, credDict)
    return super(MultiVOFileMetadata, self).findFilesByMetadata(fMetaDict, path, credDict)
