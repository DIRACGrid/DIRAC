"""This module contains tests for the DataManager client.

The tests upload files, remove them, replicate them, check metadata calls, and cleanDirectory

Requirements:

* Running FileCatalog instance, containing an LFN folder '/Jenkins', writable by the jenkins_user proxy
* Two running StorageElements SE-1 and SE-2
* A jenkins_user proxy

"""

from __future__ import print_function

import os
import pytest
import tempfile
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult


@pytest.fixture(scope='module')
def dm():
  return DataManager()


@pytest.fixture(scope='module')
def fc():
  return FileCatalog()


@pytest.fixture
def tempFile():
  """Create unique temprary file, will be cleaned up at the end."""
  theTemp = tempfile.NamedTemporaryFile('w')
  theTemp.write(str(time.time()))
  theTemp.seek(0)  # write the characters to file
  yield theTemp.name
  theTemp.close()


def assertResult(res, lfn, sub='Successful'):
  """Check if lfn is in Successful or Failed, given by sub."""
  assert res['OK'], res.get('Message', 'All OK')
  assert sub in res['Value']
  assert lfn in res['Value'][sub]
  assert res['Value'][sub][lfn]


def assertIsDir(isDir, trueOrFalse):
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
  assertResult(putRes, lfn)
  # Check that the removal was successful
  assertResult(removeRes, lfn)


def test_putAndRegisterReplicate(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tReplication test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterReplicate/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  replicateRes = dm.replicateAndRegister(lfn, 'SE-2')  # ,sourceSE='',destPath='',localCache='')
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  assertResult(putRes, lfn)
  # Check that the replicate was successful
  assertResult(replicateRes, lfn)
  # Check that the removal was successful
  assertResult(removeRes, lfn)


def test_putAndRegisterGetReplicaMetadata(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tGet metadata test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterGetReplicaMetadata/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  metadataRes = dm.getReplicaMetadata(lfn, diracSE)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  assertResult(putRes, lfn)
  # Check that the metadata query was successful
  assertResult(metadataRes, lfn)
  metadataDict = metadataRes['Value']['Successful'][lfn]
  for key in ['Cached', 'Migrated', 'Size']:
    assert key in metadataDict
  # Check that the removal was successful
  assertResult(removeRes, lfn)


def test_putAndRegsiterGetAccessUrl(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tGet Access Url test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterGetAccessUrl/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  getAccessUrlRes = dm.getReplicaAccessUrl(lfn, diracSE)
  print(getAccessUrlRes)
  removeRes = dm.removeFile(lfn)
  assertResult(putRes, lfn)
  assertResult(getAccessUrlRes, lfn)
  assertResult(removeRes, lfn)


def test_putAndRegisterRemoveReplica(dm, tempFile):
  print('\n\n#########################################################'
        '################\n\n\t\t\tRemove replica test\n')
  lfn = '/Jenkins/test/unit-test/DataManager/putAndRegisterRemoveReplica/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  removeReplicaRes = dm.removeReplica(diracSE, lfn)
  removeRes = dm.removeFile(lfn)

  # Check that the put was successful
  assertResult(putRes, lfn)
  # Check that the replica removal failed, because it is the only copy
  assertResult(removeReplicaRes, lfn, sub='Failed')
  # Check that the removal was successful
  assertResult(removeRes, lfn)


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

  assertResult(registerRes, lfn)
  assertResult(removeFileRes, lfn)


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

  assertResult(registerRes, lfn)
  assertResult(registerReplicaRes, lfn)
  assertResult(removeFileRes, lfn)


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

  assertResult(putRes, lfn)
  assertResult(getRes, lfn)
  assert getRes['Value']['Successful'][lfn] == localFilePath
  assertResult(removeRes, lfn)


def test_cleanDirectory(dm, tempFile, fc):
  lfn = '/Jenkins/test/unit-test/DataManager/cleanDirectory/testFile.%s' % time.time()
  diracSE = 'SE-1'
  putRes = dm.putAndRegister(lfn, tempFile, diracSE)
  assertResult(putRes, lfn)
  removeRes = dm.removeFile(lfn)
  assertResult(removeRes, lfn)

  folderLFN = os.path.dirname(lfn)
  assertIsDir(fc.isDirectory(folderLFN), True)
  cleanRes = dm.cleanLogicalDirectory(folderLFN)
  assert cleanRes['OK'], cleanRes.get('Message', 'All OK')
  assertIsDir(fc.isDirectory(folderLFN), False)

  baseFolder = '/Jenkins/test'
  assertIsDir(fc.isDirectory(baseFolder), True)
  cleanRes = dm.cleanLogicalDirectory(baseFolder)
  assert cleanRes['OK'], cleanRes.get('Message', 'All OK')
  assertIsDir(fc.isDirectory(baseFolder), False)
