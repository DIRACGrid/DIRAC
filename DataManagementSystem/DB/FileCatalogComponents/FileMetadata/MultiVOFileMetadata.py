""" DIRAC Multi VO FileCatalog plugin class to manage file metadata for multiple VO.
"""
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileMetadata.FileMetadata import FileMetadata
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.MultiVOMetaNameMixIn import MultiVOMetaNameMixIn


class MultiVOFileMetadata(MultiVOMetaNameMixIn, FileMetadata):
  """
  MULti-VO FileCatalog plugin implementation.
  """

  def __init__(self, database=None):
    super(MultiVOFileMetadata, self).__init__(database=database)
