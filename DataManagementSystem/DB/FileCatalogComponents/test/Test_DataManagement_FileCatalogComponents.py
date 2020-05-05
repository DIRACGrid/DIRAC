""" Test class for FileCatalogComponents
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=protected-access

# imports
from mock import MagicMock

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryManager.DirectoryTreeBase import DirectoryTreeBase
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryManager.DirectoryLevelTree import DirectoryLevelTree
# from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectorySimpleTree import DirectorySimpleTree
# from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryFlatTree import DirectoryFlatTree
# from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryNodeTree import DirectoryNodeTree

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManager.FileManagerBase import FileManagerBase

dbMock = MagicMock()
ugManagerMock = MagicMock()
ugManagerMock.getUserAndGroupID.return_value = {'OK': True, 'Value': ('l_uid', 'l_gid')}
dbMock.ugManager = ugManagerMock


####################################################################################
# TreeBase

dtb = DirectoryTreeBase()
dtb.db = dbMock


def test_Base_makeDirectory():
  res = dtb.makeDirectory('/path', {})
  assert res['OK'] is False  # this will need to be implemented on a derived class


####################################################################################
# LevelTree

dlt = DirectoryLevelTree()
dlt.db = dbMock


def test_Level_makeDirectory():
  res = dlt.makeDirectory('/path', {})
  assert res['OK'] is True  # this will need to be implemented on a derived class


####################################################################################
# SimpleTree
# FIXME: this fails... is it a genuine failure?

# dst = DirectorySimpleTree()
# dst.db = dbMock


# def test_Simple_makeDirectory():
#   res = dst.makeDirectory('/path', {})
#   assert res['OK'] is True  # this will need to be implemented on a derived class


####################################################################################
# FlatTree
# FIXME: this fails... is it a genuine failure?

# dft = DirectoryFlatTree()
# dft.db = dbMock


# def test_Flat_makeDirectory():
#   res = dft.makeDirectory('/path', {})
#   assert res['OK'] is True  # this will need to be implemented on a derived class


####################################################################################
# NodeTree
# FIXME: this fails... is it a genuine failure?

# dnt = DirectoryNodeTree()
# dnt.db = dbMock


# def test_Node_makeDirectory():
#   res = dnt.makeDirectory('/path', {})

#   assert res['OK'] is True  # this will need to be implemented on a derived class


####################################################################################
####################################################################################
# FileManagerBase

fmb = FileManagerBase()
fmb.db = dbMock


def test_Base_addFile():
  res = fmb.addFile({}, {})
  assert res['OK'] is True  # this will need to be implemented on a derived class, but it anyway returns S_OK()

  res = fmb.addFile({'aa': 'aaa/bbb'}, {})
  assert res['OK'] is True  # this will need to be implemented on a derived class, but it anyway returns S_OK()
  assert 'aa' in res['Value']['Failed']
