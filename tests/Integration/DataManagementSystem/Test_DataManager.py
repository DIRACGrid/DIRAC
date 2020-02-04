from __future__ import print_function

import os
import time
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Core.Utilities.File import makeGuid


class DataManagerTestCase(unittest.TestCase):
  """Base class for the Data Manager test cases."""

  def setUp(self):
    self.dataManager = DataManager()
    self.fileName = '/tmp/temporaryLocalFile'
    with open(self.fileName, 'w') as theTemp:
      theTemp.write(str(time.time()))

  def test_putAndRegister(self):
    print('\n\n#########################################################'
          '################\n\n\t\t\tPut and register test\n')
    lfn = '/Jenkins/test/unit-test/DataManager/putAndRegister/testFile.%s' % time.time()
    diracSE = 'SE-1'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assertTrue(putRes['OK'], putRes.get('Message', 'All OK'))
    self.assertIn('Successful', putRes['Value'])
    self.assertIn(lfn, putRes['Value']['Successful'])
    self.assertTrue(putRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assertTrue(removeRes['OK'], removeRes.get('Message', 'All OK'))
    self.assertIn('Successful', removeRes['Value'])
    self.assertIn(lfn, removeRes['Value']['Successful'])
    self.assertTrue(removeRes['Value']['Successful'][lfn])

  def test_putAndRegisterReplicate(self):
    print('\n\n#########################################################'
          '################\n\n\t\t\tReplication test\n')
    lfn = '/jenkins/test/unit-test/DataManager/putAndRegisterReplicate/testFile.%s' % time.time()
    diracSE = 'SE-1'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    replicateRes = self.dataManager.replicateAndRegister(lfn,'SE-2') #,sourceSE='',destPath='',localCache='')
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assertTrue(putRes['OK'])
    self.assertTrue(putRes['Value'].has_key('Successful'))
    self.assertTrue(putRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(putRes['Value']['Successful'][lfn])
    # Check that the replicate was successful
    self.assertTrue(replicateRes['OK'])
    self.assertTrue(replicateRes['Value'].has_key('Successful'))
    self.assertTrue(replicateRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(replicateRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assertTrue(removeRes['OK'])
    self.assertTrue(removeRes['Value'].has_key('Successful'))
    self.assertTrue(removeRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(removeRes['Value']['Successful'][lfn])

  def test_putAndRegisterGetReplicaMetadata(self):
    print('\n\n#########################################################'
          '################\n\n\t\t\tGet metadata test\n')
    lfn = '/jenkins/test/unit-test/DataManager/putAndRegisterGetReplicaMetadata/testFile.%s' % time.time()
    diracSE = 'SE-1'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    metadataRes = self.dataManager.getReplicaMetadata(lfn,diracSE)
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assertTrue(putRes['OK'])
    self.assertTrue(putRes['Value'].has_key('Successful'))
    self.assertTrue(putRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(putRes['Value']['Successful'][lfn])
    # Check that the metadata query was successful
    self.assertTrue(metadataRes['OK'])
    self.assertTrue(metadataRes['Value'].has_key('Successful'))
    self.assertTrue(metadataRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(metadataRes['Value']['Successful'][lfn])
    metadataDict = metadataRes['Value']['Successful'][lfn]
    self.assertTrue(metadataDict.has_key('Cached'))
    self.assertTrue(metadataDict.has_key('Migrated'))
    self.assertTrue(metadataDict.has_key('Size'))
    # Check that the removal was successful
    self.assertTrue(removeRes['OK'])
    self.assertTrue(removeRes['Value'].has_key('Successful'))
    self.assertTrue(removeRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(removeRes['Value']['Successful'][lfn])


  def test_putAndRegsiterGetAccessUrl(self):
    print('\n\n#########################################################'
          '################\n\n\t\t\tGet Access Url test\n')
    lfn = '/jenkins/test/unit-test/DataManager/putAndRegisterGetAccessUrl/testFile.%s' % time.time()
    diracSE = 'SE-1'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    getAccessUrlRes = self.dataManager.getReplicaAccessUrl(lfn,diracSE)
    print(getAccessUrlRes)
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assertTrue(putRes['OK'])
    self.assertTrue(putRes['Value'].has_key('Successful'))
    self.assertTrue(putRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(putRes['Value']['Successful'][lfn])
    # Check that the access url was successful
    self.assertTrue(getAccessUrlRes['OK'])
    self.assertTrue(getAccessUrlRes['Value'].has_key('Successful'))
    self.assertTrue(getAccessUrlRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(getAccessUrlRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assertTrue(removeRes['OK'])
    self.assertTrue(removeRes['Value'].has_key('Successful'))
    self.assertTrue(removeRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(removeRes['Value']['Successful'][lfn])

  def test_putAndRegisterRemoveReplica(self):
    print('\n\n#########################################################'
          '################\n\n\t\t\tRemove replica test\n')
    lfn = '/jenkins/test/unit-test/DataManager/putAndRegisterRemoveReplica/testFile.%s' % time.time()
    diracSE = 'SE-1'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    removeReplicaRes = self.dataManager.removeReplica(diracSE,lfn)
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assertTrue(putRes['OK'])
    self.assertTrue(putRes['Value'].has_key('Successful'))
    self.assertTrue(putRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(putRes['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    self.assertTrue(removeReplicaRes['OK'])
    self.assertTrue(removeReplicaRes['Value'].has_key('Successful'))
    self.assertTrue(removeReplicaRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(removeReplicaRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assertTrue(removeRes['OK'])
    self.assertTrue(removeRes['Value'].has_key('Successful'))
    self.assertTrue(removeRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(removeRes['Value']['Successful'][lfn])

  def test_registerFile(self):
    lfn = '/jenkins/test/unit-test/DataManager/registerFile/testFile.%s' % time.time()
    physicalFile = 'srm://host:port/srm/managerv2?SFN=/sa/path%s' % lfn
    fileSize = 10000
    storageElementName = 'CERN-RAW'
    fileGuid = makeGuid()
    fileTuple = (lfn,physicalFile,fileSize,storageElementName,fileGuid)
    registerRes = self.dataManager.registerFile(fileTuple)
    # removeCatalogReplicaRes = self.dataManager.removeCatalogReplica(storageElementName,lfn)
    removeFileRes = self.dataManager.removeFile(lfn)

    # Check that the file registration was done correctly
    self.assertTrue(registerRes['OK'])
    self.assertTrue(registerRes['Value'].has_key('Successful'))
    self.assertTrue(registerRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(registerRes['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    # self.assertTrue(removeCatalogReplicaRes['OK'])
    # self.assertTrue(removeCatalogReplicaRes['Value'].has_key('Successful'))
    # self.assertTrue(removeCatalogReplicaRes['Value']['Successful'].has_key(lfn))
    # self.assertTrue(removeCatalogReplicaRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assertTrue(removeFileRes['OK'])
    self.assertTrue(removeFileRes['Value'].has_key('Successful'))
    self.assertTrue(removeFileRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(removeFileRes['Value']['Successful'][lfn])

  def test_registerReplica(self):
    print('\n\n#########################################################'
          '################\n\n\t\t\tRegister replica test\n')
    lfn = '/jenkins/test/unit-test/DataManager/registerReplica/testFile.%s' % time.time()
    physicalFile = 'srm://host:port/srm/managerv2?SFN=/sa/path%s' % lfn
    fileSize = 10000
    storageElementName = 'CERN-RAW'
    fileGuid = makeGuid()
    fileTuple = (lfn,physicalFile,fileSize,storageElementName,fileGuid)
    registerRes = self.dataManager.registerFile(fileTuple)
    seName = 'SE-1'
    replicaTuple = (lfn,physicalFile,seName)
    registerReplicaRes = self.dataManager.registerReplica(replicaTuple)
    # removeCatalogReplicaRes1 = self.dataManager.removeCatalogReplica(storageElementName,lfn)
    # removeCatalogReplicaRes2 = self.dataManager.removeCatalogReplica(seName,lfn)
    removeFileRes = self.dataManager.removeFile(lfn)

    # Check that the file registration was done correctly
    self.assertTrue(registerRes['OK'])
    self.assertTrue(registerRes['Value'].has_key('Successful'))
    self.assertTrue(registerRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(registerRes['Value']['Successful'][lfn])
    # Check that the replica registration was successful
    self.assertTrue(registerReplicaRes['OK'])
    self.assertTrue(registerReplicaRes['Value'].has_key('Successful'))
    self.assertTrue(registerReplicaRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(registerReplicaRes['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    # self.assertTrue(removeCatalogReplicaRes1['OK'])
    # self.assertTrue(removeCatalogReplicaRes1['Value'].has_key('Successful'))
    # self.assertTrue(removeCatalogReplicaRes1['Value']['Successful'].has_key(lfn))
    # self.assertTrue(removeCatalogReplicaRes1['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    # self.assertTrue(removeCatalogReplicaRes2['OK'])
    # self.assertTrue(removeCatalogReplicaRes2['Value'].has_key('Successful'))
    # self.assertTrue(removeCatalogReplicaRes2['Value']['Successful'].has_key(lfn))
    # self.assertTrue(removeCatalogReplicaRes2['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assertTrue(removeFileRes['OK'])
    self.assertTrue(removeFileRes['Value'].has_key('Successful'))
    self.assertTrue(removeFileRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(removeFileRes['Value']['Successful'][lfn])

  def test_putAndRegisterGet(self):
    print('\n\n#########################################################'
          '################\n\n\t\t\tGet file test\n')
    lfn = '/jenkins/test/unit-test/DataManager/putAndRegisterGet/testFile.%s' % time.time()
    diracSE = 'SE-1'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    getRes = self.dataManager.getFile(lfn)
    removeRes = self.dataManager.removeFile(lfn)
    localFilePath = "%s/%s" % (os.getcwd(),os.path.basename(lfn))
    if os.path.exists(localFilePath):
      os.remove(localFilePath)

    # Check that the put was successful
    self.assertTrue(putRes['OK'])
    self.assertTrue(putRes['Value'].has_key('Successful'))
    self.assertTrue(putRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(putRes['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    self.assertTrue(getRes['OK'])
    self.assertTrue(getRes['Value'].has_key('Successful'))
    self.assertTrue(getRes['Value']['Successful'].has_key(lfn))
    self.assertEqual(getRes['Value']['Successful'][lfn],localFilePath)
    # Check that the removal was successful
    self.assertTrue(removeRes['OK'])
    self.assertTrue(removeRes['Value'].has_key('Successful'))
    self.assertTrue(removeRes['Value']['Successful'].has_key(lfn))
    self.assertTrue(removeRes['Value']['Successful'][lfn])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(DataManagerTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  exit(testResult)