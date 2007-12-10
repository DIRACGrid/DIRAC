import unittest,types,time,os,shutil
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

class ReplicaManagerTestCase(unittest.TestCase):
  """ Base class for the Replica Manager test cases
  """
  def setUp(self):
    self.replicaManager = ReplicaManager()
    self.fileName = '/tmp/temporaryLocalFile'
    file = open(self.fileName,'w')
    file.write("%s" % time.time())
    file.close()

  def test_putAndRegister(self):
    print '\n\n#########################################################################\n\n\t\t\tPut and register test\n'
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegister/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.replicaManager.putAndRegister(lfn, self.fileName, diracSE)
    removeRes = self.replicaManager.removeFile(lfn)
   	
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
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegister/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.replicaManager.putAndRegister(lfn, self.fileName, diracSE)
    replicateRes = self.replicaManager.replicateAndRegister(lfn,'CNAF-DST') #,sourceSE='',destPath='',localCache='')
    removeRes = self.replicaManager.removeFile(lfn)

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
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegister/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.replicaManager.putAndRegister(lfn, self.fileName, diracSE)
    metadataRes = self.replicaManager.getReplicaMetadata(lfn,diracSE)
    removeRes = self.replicaManager.removeFile(lfn)

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
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegister/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.replicaManager.putAndRegister(lfn, self.fileName, diracSE)
    getAccessUrlRes = self.replicaManager.getReplicaAccessUrl(lfn,diracSE)
    print getAccessUrlRes
    removeRes = self.replicaManager.removeFile(lfn)

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
    lfn = '/lhcb/test/unit-test/ReplicaManager/putAndRegister/testFile.%s' % time.time()
    diracSE = 'GRIDKA-RAW'
    putRes = self.replicaManager.putAndRegister(lfn, self.fileName, diracSE)
    removeReplicaRes = self.replicaManager.removeReplica(lfn,diracSE)
    #removeRes = self.replicaManager.removeFile(lfn)
    
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

  
    #res = self.replicaManager.getFile(lfn)
    #print res


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ReplicaManagerTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

