""" Test class for FileCatalogComponents
"""

# pylint: disable=protected-access

# imports
from mock import MagicMock

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryTreeBase import DirectoryTreeBase
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryLevelTree import DirectoryLevelTree
# from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryNodeTree import DirectoryNodeTree

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
# NodeTree
# FIXME: this fails... is it a genuine failure?

# dnt = DirectoryNodeTree()
# dnt.db = dbMock


# def test_Node_makeDirectory():
#   res = dnt.makeDirectory('/path', {})

#   assert res['OK'] is True  # this will need to be implemented on a derived class
