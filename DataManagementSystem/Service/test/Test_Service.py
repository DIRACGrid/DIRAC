from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
# sut
from DIRAC.DataManagementSystem.Service.StorageElementHandler import getDiskSpace, getFreeDiskSpace, getTotalDiskSpace


def test_getDiskSpace():
  res = getDiskSpace("/")
  assert res['OK']

  res = getTotalDiskSpace()
  assert res['OK']

  res = getFreeDiskSpace()
  assert res['OK']
