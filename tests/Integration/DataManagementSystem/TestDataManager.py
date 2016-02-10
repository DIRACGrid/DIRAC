# FIXME: to bring back to life
# TO-DO: to be took back to life, and moved to TestDIRAC

import unittest, time, os
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Core.Utilities.File import makeGuid

class ReplicaManagerTestCase(unittest.TestCase):
  """ Base class for the Replica Manager test cases
  """
  def setUp(self):
    self.dataManager = DataManager()
    self.fileName = '/tmp/temporaryLocalFile'
    file = open(self.fileName,'w')
    file.write("%s" % time.time())
    file.close()

  def test_putAndRegister(self):
    print '\n\n#########################################################################\n\n\t\t\tPut and register test\n'
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegister/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assert_(putRes['OK'])
    self.assert_(putRes['Value'].has_key('Successful'))
    self.assert_(putRes['Value']['Successful'].has_key(lfn))
    self.assert_(putRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assert_(removeRes['OK'])
    self.assert_(removeRes['Value'].has_key('Successful'))
    self.assert_(removeRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeRes['Value']['Successful'][lfn])

  def test_putAndRegisterReplicate(self):
    print '\n\n#########################################################################\n\n\t\t\tReplication test\n'
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegisterReplicate/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    replicateRes = self.dataManager.replicateAndRegister(lfn,'CNAF-DST') #,sourceSE='',destPath='',localCache='')
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assert_(putRes['OK'])
    self.assert_(putRes['Value'].has_key('Successful'))
    self.assert_(putRes['Value']['Successful'].has_key(lfn))
    self.assert_(putRes['Value']['Successful'][lfn])
    # Check that the replicate was successful
    self.assert_(replicateRes['OK'])
    self.assert_(replicateRes['Value'].has_key('Successful'))
    self.assert_(replicateRes['Value']['Successful'].has_key(lfn))
    self.assert_(replicateRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assert_(removeRes['OK'])
    self.assert_(removeRes['Value'].has_key('Successful'))
    self.assert_(removeRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeRes['Value']['Successful'][lfn])

  def test_putAndRegisterGetReplicaMetadata(self):
    print '\n\n#########################################################################\n\n\t\t\tGet metadata test\n'
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegisterGetReplicaMetadata/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    metadataRes = self.dataManager.getReplicaMetadata(lfn,diracSE)
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assert_(putRes['OK'])
    self.assert_(putRes['Value'].has_key('Successful'))
    self.assert_(putRes['Value']['Successful'].has_key(lfn))
    self.assert_(putRes['Value']['Successful'][lfn])
    # Check that the metadata query was successful
    self.assert_(metadataRes['OK'])
    self.assert_(metadataRes['Value'].has_key('Successful'))
    self.assert_(metadataRes['Value']['Successful'].has_key(lfn))
    self.assert_(metadataRes['Value']['Successful'][lfn])
    metadataDict = metadataRes['Value']['Successful'][lfn]
    self.assert_(metadataDict.has_key('Cached'))
    self.assert_(metadataDict.has_key('Migrated'))
    self.assert_(metadataDict.has_key('Size'))
    # Check that the removal was successful
    self.assert_(removeRes['OK'])
    self.assert_(removeRes['Value'].has_key('Successful'))
    self.assert_(removeRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeRes['Value']['Successful'][lfn])


  def test_putAndRegsiterGetAccessUrl(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Access Url test\n'
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegisterGetAccessUrl/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    getAccessUrlRes = self.dataManager.getReplicaAccessUrl(lfn,diracSE)
    print getAccessUrlRes
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assert_(putRes['OK'])
    self.assert_(putRes['Value'].has_key('Successful'))
    self.assert_(putRes['Value']['Successful'].has_key(lfn))
    self.assert_(putRes['Value']['Successful'][lfn])
    # Check that the access url was successful
    self.assert_(getAccessUrlRes['OK'])
    self.assert_(getAccessUrlRes['Value'].has_key('Successful'))
    self.assert_(getAccessUrlRes['Value']['Successful'].has_key(lfn))
    self.assert_(getAccessUrlRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assert_(removeRes['OK'])
    self.assert_(removeRes['Value'].has_key('Successful'))
    self.assert_(removeRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeRes['Value']['Successful'][lfn])

  def test_putAndRegisterRemoveReplica(self):
    print '\n\n#########################################################################\n\n\t\t\tRemove replica test\n'
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegisterRemoveReplica/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    removeReplicaRes = self.dataManager.removeReplica(diracSE,lfn)
    removeRes = self.dataManager.removeFile(lfn)

    # Check that the put was successful
    self.assert_(putRes['OK'])
    self.assert_(putRes['Value'].has_key('Successful'))
    self.assert_(putRes['Value']['Successful'].has_key(lfn))
    self.assert_(putRes['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    self.assert_(removeReplicaRes['OK'])
    self.assert_(removeReplicaRes['Value'].has_key('Successful'))
    self.assert_(removeReplicaRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeReplicaRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assert_(removeRes['OK'])
    self.assert_(removeRes['Value'].has_key('Successful'))
    self.assert_(removeRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeRes['Value']['Successful'][lfn])

  def test_registerFile(self):
    lfn = '/lhcb/test/unit-test/ReplicaManager/registerFile/testFile.%s' % time.time()
    physicalFile = 'srm://host:port/srm/managerv2?SFN=/sa/path%s' % lfn
    fileSize = 10000
    storageElementName = 'CERN-RAW'
    fileGuid = makeGuid()
    fileTuple = (lfn,physicalFile,fileSize,storageElementName,fileGuid)
    registerRes = self.dataManager.registerFile(fileTuple)
    removeCatalogReplicaRes = self.dataManager.removeCatalogReplica(storageElementName,lfn)
    removeFileRes = self.dataManager.removeFile(lfn)

    # Check that the file registration was done correctly
    self.assert_(registerRes['OK'])
    self.assert_(registerRes['Value'].has_key('Successful'))
    self.assert_(registerRes['Value']['Successful'].has_key(lfn))
    self.assert_(registerRes['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    self.assert_(removeCatalogReplicaRes['OK'])
    self.assert_(removeCatalogReplicaRes['Value'].has_key('Successful'))
    self.assert_(removeCatalogReplicaRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeCatalogReplicaRes['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value'].has_key('Successful'))
    self.assert_(removeFileRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeFileRes['Value']['Successful'][lfn])

  def test_registerReplica(self):
    print '\n\n#########################################################################\n\n\t\t\tRegister replica test\n'
    lfn = '/lhcb/test/unit-test/ReplicaManager/registerReplica/testFile.%s' % time.time()
    physicalFile = 'srm://host:port/srm/managerv2?SFN=/sa/path%s' % lfn
    fileSize = 10000
    storageElementName = 'CERN-RAW'
    fileGuid = makeGuid()
    fileTuple = (lfn,physicalFile,fileSize,storageElementName,fileGuid)
    registerRes = self.dataManager.registerFile(fileTuple)
    seName = 'GRIDKA-RAW'
    replicaTuple = (lfn,physicalFile,seName)
    registerReplicaRes = self.dataManager.registerReplica(replicaTuple)
    removeCatalogReplicaRes1 = self.dataManager.removeCatalogReplica(storageElementName,lfn)
    removeCatalogReplicaRes2 = self.dataManager.removeCatalogReplica(seName,lfn)
    removeFileRes = self.dataManager.removeFile(lfn)

    # Check that the file registration was done correctly
    self.assert_(registerRes['OK'])
    self.assert_(registerRes['Value'].has_key('Successful'))
    self.assert_(registerRes['Value']['Successful'].has_key(lfn))
    self.assert_(registerRes['Value']['Successful'][lfn])
    # Check that the replica registration was successful
    self.assert_(registerReplicaRes['OK'])
    self.assert_(registerReplicaRes['Value'].has_key('Successful'))
    self.assert_(registerReplicaRes['Value']['Successful'].has_key(lfn))
    self.assert_(registerReplicaRes['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    self.assert_(removeCatalogReplicaRes1['OK'])
    self.assert_(removeCatalogReplicaRes1['Value'].has_key('Successful'))
    self.assert_(removeCatalogReplicaRes1['Value']['Successful'].has_key(lfn))
    self.assert_(removeCatalogReplicaRes1['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    self.assert_(removeCatalogReplicaRes2['OK'])
    self.assert_(removeCatalogReplicaRes2['Value'].has_key('Successful'))
    self.assert_(removeCatalogReplicaRes2['Value']['Successful'].has_key(lfn))
    self.assert_(removeCatalogReplicaRes2['Value']['Successful'][lfn])
    # Check that the removal was successful
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value'].has_key('Successful'))
    self.assert_(removeFileRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeFileRes['Value']['Successful'][lfn])

  def test_putAndRegisterGet(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file test\n'
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegisterGet/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.dataManager.putAndRegister(lfn, self.fileName, diracSE)
    getRes = self.dataManager.getFile(lfn)
    removeRes = self.dataManager.removeFile(lfn)
    localFilePath = "%s/%s" % (os.getcwd(),os.path.basename(lfn))
    if os.path.exists(localFilePath):
      os.remove(localFilePath)

    # Check that the put was successful
    self.assert_(putRes['OK'])
    self.assert_(putRes['Value'].has_key('Successful'))
    self.assert_(putRes['Value']['Successful'].has_key(lfn))
    self.assert_(putRes['Value']['Successful'][lfn])
    # Check that the replica removal was successful
    self.assert_(getRes['OK'])
    self.assert_(getRes['Value'].has_key('Successful'))
    self.assert_(getRes['Value']['Successful'].has_key(lfn))
    self.assertEqual(getRes['Value']['Successful'][lfn],localFilePath)
    # Check that the removal was successful
    self.assert_(removeRes['OK'])
    self.assert_(removeRes['Value'].has_key('Successful'))
    self.assert_(removeRes['Value']['Successful'].has_key(lfn))
    self.assert_(removeRes['Value']['Successful'][lfn])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ReplicaManagerTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

