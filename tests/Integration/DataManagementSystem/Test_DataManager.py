from __future__ import print_function

import os
import time
import pytest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult


@pytest.fixture
def dm():
  return DataManager()


@pytest.fixture
def fc():
  return FileCatalog()

@pytest.fixture
def tempFile():
  fileName = '/tmp/temporaryLocalFile'
  with open(fileName, 'w') as theTemp:
    theTemp.write(str(time.time()))
  return fileName


def checkPut(putRes, lfn):
  """.Check that the put was successful."""
  assert putRes['OK'], putRes.get('Message', 'All OK')
  assert 'Successful' in putRes['Value']
  assert lfn in putRes['Value']['Successful']
  assert putRes['Value']['Successful'][lfn]


def checkMultiple(res, lfn, sub):
  """Check if lfn is in Successful or Failed, given by sub."""
  assert res['OK'], res.get('Message', 'All OK')
  assert sub in res['Value']
  assert lfn in res['Value'][sub]
  assert res['Value'][sub][lfn]


def checkIsDir(isDir, trueOrFalse):
  """Check if directory exists or not."""
  assert isDir['OK'], isDir.get('Message', 'ALL OK')
  single = returnSingleResult(isDir)
  assert single['OK'], str(single)
  assert single['Value'] is trueOrFalse


def test_putAndRegister(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tPut and register test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegister/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  checkPut(putRes, lfn)
  # Check that the removal was successful
  checkMultiple(removeRes, lfn, 'Successful')


def test_putAndRegisterReplicate(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tReplication test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterReplicate/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  replicateRes = dm.replicateAndRegister(lfn, 'SE-2')  # ,sourceSE='',destPath='',localCache='')
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  checkPut(putRes, lfn)
  # Check that the replicate was successful
  checkMultiple(replicateRes, lfn, 'Successful')
  # Check that the removal was successful
  checkMultiple(removeRes, lfn, 'Successful')


def test_putAndRegisterGetReplicaMetadata(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tGet metadata test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterGetReplicaMetadata/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  metadataRes = dm.getReplicaMetadata(lfn, diracSE)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  checkPut(putRes, lfn)
  # Check that the metadata query was successful
  checkMultiple(metadataRes, lfn, 'Successful')
  metadataDict = metadataRes['Value']['Successful'][lfn]
  for key in ['Cached', 'Migrated', 'Size']:
    assert key in metadataDict
  # Check that the removal was successful
  checkMultiple(removeRes, lfn, 'Successful')


def test_putAndRegsiterGetAccessUrl(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tGet Access Url test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterGetAccessUrl/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  getAccessUrlRes = dm.getReplicaAccessUrl(lfn, diracSE)
  print(getAccessUrlRes)
  removeRes = dm.removeFile(lfn)
  checkPut(putRes, lfn)
  checkMultiple(getAccessUrlRes, lfn, 'Successful')
  checkMultiple(removeRes, lfn, 'Successful')


def test_putAndRegisterRemoveReplica(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tRemove replica test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterRemoveReplica/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  removeReplicaRes = dm.removeReplica(diracSE, lfn)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  checkPut(putRes, lfn)
  # Check that the replica removal failed, because it is the only copy
  checkMultiple(removeReplicaRes, lfn, 'Failed')
  # Check that the removal was successful
  checkMultiple(removeRes, lfn, 'Successful')


def test_registerFile(dm, tempFile):
  lfn = '/Jenkins/test/unit-test/DataManager/registerFile/testFile.%s' % time.time()
  physicalFile = 'srm://host:port/srm/managerv2?SFN=/sa/path%s' % lfn
  fileSize = 10000
  storageElementName = 'SE-1'
  fileGuid = makeGuid()
  checkSum = None
  fileTuple = (lfn, physicalFile, fileSize, storageElementName, fileGuid, checkSum)
  registerRes = dm.registerFile(fileTuple)
  removeFileRes = dm.removeFile(lfn)

  checkMultiple(registerRes, lfn, 'Successful')
  checkMultiple(removeFileRes, lfn, 'Successful')


def test_registerReplica(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tRegister replica test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/registerReplica/testFile.%s' % time.time()
  physicalFile = 'srm://host:port/srm/managerv2?SFN=/sa/path%s' % lfn
  fileSize = 10000
  storageElementName = 'SE-1'
  fileGuid = makeGuid()
  checkSum = None
  fileTuple = (lfn, physicalFile, fileSize, storageElementName, fileGuid, checkSum)
  registerRes = dm.registerFile(fileTuple)
  seName = 'SE-1'
  replicaTuple = (lfn, physicalFile, seName)
  registerReplicaRes = dm.registerReplica(replicaTuple)
  removeFileRes = dm.removeFile(lfn)

  checkMultiple(registerRes, lfn, 'Successful')
  checkMultiple(registerReplicaRes, lfn, 'Successful')
  checkMultiple(removeFileRes, lfn, 'Successful')


def test_putAndRegisterGet(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tGet file test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterGet/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  getRes = dm.getFile(lfn)
  removeRes = dm.removeFile(lfn)
  localFilePath = os.path.join(os.getcwd(), os.path.basename(lfn))
  if os.path.exists(localFilePath):
    os.remove(localFilePath)

  checkMultiple(putRes, lfn, 'Successful')
  checkMultiple(getRes, lfn, 'Successful')
  assert getRes['Value']['Successful'][lfn] == localFilePath
  checkMultiple(removeRes, lfn, 'Successful')


def test_cleanDirectory(dm, tempFile, fc):
  lfn = '/Jenkins/test/unit-test/DataManager/cleanDirectory/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  checkPut(putRes, lfn)
  removeRes = dm.removeFile(lfn)
  checkMultiple(removeRes, lfn, 'Successful')

  folderLFN = os.path.dirname(lfn)
  checkIsDir(fc.isDirectory(folderLFN), True)
  cleanRes = dm.cleanLogicalDirectory(folderLFN)
  assert cleanRes['OK'], cleanRes.get('Message', 'All OK')
  checkIsDir(fc.isDirectory(folderLFN), False)

  baseFolder = '/Jenkins/test'
  checkIsDir(fc.isDirectory(baseFolder), True)
  cleanRes = dm.cleanLogicalDirectory(baseFolder)
  assert cleanRes['OK'], cleanRes.get('Message', 'All OK')
  checkIsDir(fc.isDirectory(baseFolder), False)
