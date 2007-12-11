import unittest,types,time
from DIRAC.Core.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.File import getSize

class StorageElementTestCase(unittest.TestCase):
  """ Base class for the StorageElement test cases
  """
  def setUp(self):
    self.storageElement = StorageElement('CERN-RAW')
    self.localSourceFile = "/etc/group"
    self.localFileSize = getSize(self.localSourceFile)
    self.destDirectory = "/lhcb/test/unit-test/StorageElement"
    self.alternativeDestFileName = "testFile.%s" % time.time()
    self.alternativeLocal = "/tmp/storageElementTestFile.%s" % time.time()

  def test_dump(self):
    print '\n\n#########################################################################\n\n\t\t\tDump test\n'
    self.storageElement.dump()

  def test_isValid(self):
    print '\n\n#########################################################################\n\n\t\t\tIs valid test\n'
    res = self.storageElement.isValid()
    self.assert_(res['OK'])

  def test_getRemoteProtocols(self):
    print '\n\n#########################################################################\n\n\t\t\tGet remote protocols test\n'
    res = self.storageElement.getRemoteProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_getLocalProtocols(self):
    print '\n\n#########################################################################\n\n\t\t\tGet local protocols test\n'
    res = self.storageElement.getLocalProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_getProtocols(self):
    print '\n\n#########################################################################\n\n\t\t\tGet protocols test\n'
    res = self.storageElement.getProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_isLocalSE(self):
    print '\n\n#########################################################################\n\n\t\t\tIs local SE test\n'
    res = self.storageElement.isLocalSE()
    self.assert_(res['OK'])
    self.assertFalse(res['Value'])

  def test_getStorageElementOption(self):
    print '\n\n#########################################################################\n\n\t\t\tGet storage element option test\n'
    res = self.storageElement.getStorageElementOption('StorageBackend')
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'Castor')

  def test_getStorageParameters(self):
    print '\n\n#########################################################################\n\n\t\t\tGet storage parameters test\n'
    res = self.storageElement.getStorageParameters('SRM2')
    self.assert_(res['OK'])
    resDict = res['Value']
    self.assertEqual(resDict['Protocol'],'srm')
    self.assertEqual(resDict['SpaceToken'], 'LHCb_RAW')
    self.assertEqual(resDict['WSUrl'], '/srm/managerv2?SFN=')
    self.assertEqual(resDict['Host'], 'srm-lhcb.cern.ch')
    self.assertEqual(resDict['Path'], '/castor/cern.ch/grid')
    self.assertEqual(resDict['ProtocolName'],'SRM2')
    self.assertEqual(resDict['Port'],'8443')

class PutFileTestCase(StorageElementTestCase):

  def test_putFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPut file test\n'
    res = self.storageElement.putFile(self.localSourceFile,self.destDirectory,alternativeFileName=self.alternativeDestFileName)
    self.assert_(res['OK'])
    self.assert_(res['Value'])
    sourceFile = res['Value']

    print '\n\n#########################################################################\n\n\t\t\tGet file test\n'
    res = self.storageElement.getFile(sourceFile,self.localFileSize,localPath=self.alternativeLocal)
    print res

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PutFileTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CreateFTSReqCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

