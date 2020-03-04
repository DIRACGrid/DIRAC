# sut
from DIRAC.DataManagementSystem.Service.StorageElementHandler import getDiskSpace, getFreeDiskSpace, getTotalDiskSpace


def test_getDiskSpace():
  res = getDiskSpace("/")
  assert res['OK']

  res = getTotalDiskSpace()
  assert res['OK']
  total = res['Value']

  res = getTotalDiskSpace(ignoreMaxStorageSize=True)
  assert res['OK']
  assert(res >= total)

  res = getFreeDiskSpace()
  assert res['OK']
  free = res['OK']

  res = getFreeDiskSpace(ignoreMaxStorageSize=True)
  assert res['OK']
  assert(free <= res['Value'])
