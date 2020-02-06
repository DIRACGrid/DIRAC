from __future__ import print_function

import os
import time
import pytest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Core.Utilities.File import makeGuid


@pytest.fixture
def dm():
  return DataManager()


@pytest.fixture
def tempFile():
  fileName = '/tmp/temporaryLocalFile'
  with open(fileName, 'w') as theTemp:
    theTemp.write(str(time.time()))
  return fileName


def test_putAndRegister(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tPut and register test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegister/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  assert putRes['OK'], putRes.get('Message', 'All OK')
  assert 'Successful' in putRes['Value']
  assert lfn in putRes['Value']['Successful']
  assert putRes['Value']['Successful'][lfn]
  # Check that the removal was successful
  assert removeRes['OK'], removeRes.get('Message', 'All OK')
  assert 'Successful' in removeRes['Value']
  assert lfn in removeRes['Value']['Successful']
  assert removeRes['Value']['Successful'][lfn]


def test_putAndRegisterReplicate(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tReplication test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterReplicate/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  replicateRes = dm.replicateAndRegister(lfn, 'SE-2')  # ,sourceSE='',destPath='',localCache='')
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  assert putRes['OK'], putRes.get('Message', 'All OK')
  assert 'Successful' in putRes['Value']
  assert lfn in putRes['Value']['Successful']
  assert putRes['Value']['Successful'][lfn]
  # Check that the replicate was successful
  assert replicateRes['OK'], replicateRes.get('Message', 'All OK')
  assert 'Successful' in replicateRes['Value']
  assert lfn in replicateRes['Value']['Successful']
  assert replicateRes['Value']['Successful'][lfn]
  # Check that the removal was successful
  assert removeRes['OK'], removeRes.get('Message', 'All OK')
  assert 'Successful' in removeRes['Value']
  assert lfn in removeRes['Value']['Successful']
  assert removeRes['Value']['Successful'][lfn]


def test_putAndRegisterGetReplicaMetadata(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tGet metadata test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterGetReplicaMetadata/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  metadataRes = dm.getReplicaMetadata(lfn, diracSE)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  assert putRes['OK'], putRes.get('Message', 'All OK')
  assert 'Successful' in putRes['Value']
  assert lfn in putRes['Value']['Successful']
  assert putRes['Value']['Successful'][lfn]
  # Check that the metadata query was successful
  assert metadataRes['OK'], metadataRes.get('Message', 'All OK')
  assert 'Successful' in metadataRes['Value']
  assert lfn in metadataRes['Value']['Successful']
  assert metadataRes['Value']['Successful'][lfn]
  metadataDict = metadataRes['Value']['Successful'][lfn]
  for key in ['Cached', 'Migrated', 'Size']:
    assert key in metadataDict
  # Check that the removal was successful
  assert removeRes['OK'], removeRes.get('Message', 'All OK')
  assert 'Successful' in removeRes['Value']
  assert lfn in removeRes['Value']['Successful']
  assert removeRes['Value']['Successful'][lfn]


def test_putAndRegsiterGetAccessUrl(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tGet Access Url test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterGetAccessUrl/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  getAccessUrlRes = dm.getReplicaAccessUrl(lfn, diracSE)
  print(getAccessUrlRes)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  assert putRes['OK'], putRes.get('Message', 'All OK')
  assert 'Successful' in putRes['Value']
  assert lfn in putRes['Value']['Successful']
  assert putRes['Value']['Successful'][lfn]
  # Check that the access url was successful
  assert getAccessUrlRes['OK'], getAccessUrlRes.get('Message', 'All OK')
  assert 'Successful' in getAccessUrlRes['Value']
  assert lfn in getAccessUrlRes['Value']['Successful']
  assert getAccessUrlRes['Value']['Successful'][lfn]
  # Check that the removal was successful
  assert removeRes['OK'], removeRes.get('Message', 'All OK')
  assert 'Successful' in removeRes['Value']
  assert lfn in removeRes['Value']['Successful']
  assert removeRes['Value']['Successful'][lfn]


def test_putAndRegisterRemoveReplica(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tRemove replica test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterRemoveReplica/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  removeReplicaRes = dm.removeReplica(diracSE, lfn)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  assert putRes['OK'], putRes.get('Message', 'All OK')
  assert 'Successful' in putRes['Value']
  assert lfn in putRes['Value']['Successful']
  assert putRes['Value']['Successful'][lfn]
  # Check that the replica removal failed, because it is the only copy
  assert removeReplicaRes['OK'], removeReplicaRes.get('Message', 'All OK')
  assert 'Failed' in removeReplicaRes['Value']
  assert lfn in removeReplicaRes['Value']['Failed']
  assert removeReplicaRes['Value']['Failed'][lfn]
  # Check that the removal was successful
  assert removeRes['OK'], removeRes.get('Message', 'All OK')
  assert 'Successful' in removeRes['Value']
  assert lfn in removeRes['Value']['Successful']
  assert removeRes['Value']['Successful'][lfn]


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

  # Check that the file registration was done correctly
  assert registerRes['OK'], registerRes.get('Message', 'All OK')
  assert 'Successful' in registerRes['Value']
  assert lfn in registerRes['Value']['Successful']
  assert registerRes['Value']['Successful'][lfn]
  # Check that the removal was successful
  assert removeFileRes['OK'], removeFileRes.get('Message', 'All OK')
  assert 'Successful' in removeFileRes['Value']
  assert lfn in removeFileRes['Value']['Successful']
  assert removeFileRes['Value']['Successful'][lfn]


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

  # Check that the file registration was done correctly
  assert registerRes['OK'], registerRes.get('Message', 'All OK')
  assert 'Successful' in registerRes['Value']
  assert lfn in registerRes['Value']['Successful']
  assert registerRes['Value']['Successful'][lfn]
  # Check that the replica registration was successful
  assert registerReplicaRes['OK'], registerReplicaRes.get('Message', 'All OK')
  assert 'Successful' in registerReplicaRes['Value']
  assert lfn in registerReplicaRes['Value']['Successful']
  assert registerReplicaRes['Value']['Successful'][lfn]
  # Check that the removal was successful
  assert removeFileRes['OK'], removeFileRes.get('Message', 'All OK')
  assert 'Successful' in removeFileRes['Value']
  assert lfn in removeFileRes['Value']['Successful']
  assert removeFileRes['Value']['Successful'][lfn]


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

  # Check that the put was successful
  assert putRes['OK'], putRes.get('Message', 'All OK')
  assert 'Successful' in putRes['Value']
  assert lfn in putRes['Value']['Successful']
  assert putRes['Value']['Successful'][lfn]
  # Check that the replica removal was successful
  assert getRes['OK'], getRes.get('Message', 'All OK')
  assert 'Successful' in getRes['Value']
  assert lfn in getRes['Value']['Successful']
  assert getRes['Value']['Successful'][lfn] == localFilePath
  # Check that the removal was successful
  assert removeRes['OK'], removeRes.get('Message', 'All OK')
  assert 'Successful' in removeRes['Value']
  assert lfn in removeRes['Value']['Successful']
  assert removeRes['Value']['Successful'][lfn]
