import unittest,types,time
from DIRAC.Core.Storage.StorageElement import StorageElement

class StorageElementTestCase(unittest.TestCase):
  """ Base class for the StorageElement test cases
  """
  def setUp(self):
    self.storageElement = StorageElement('CERN-RAW')

  def test_dump(self):
    self.storageElement.dump()

  def test_isValid(self):
    res = self.storageElement.isValid()
    self.assert_(res['OK'])

  def test_getRemoteProtocols(self):
    res = self.storageElement.getRemoteProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_getLocalProtocols(self):
    res = self.storageElement.getLocalProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_getProtocols(self):
    res = self.storageElement.getProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_isLocalSE(self):
    res = self.storageElement.isLocalSE()
    self.assert_(res['OK'])
    self.assertFalse(res['Value'])

  def test_getStorageElementOption(self):
    res = self.storageElement.getStorageElementOption('StorageBackend')
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'Castor')

  def test_getStorageParameters(self):
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
    localFile = '/etc/group'
    directory = '/lhcb/test/unit-test'
    res = self.storageElement.putFile(localFile,directory,'testFile.madeup')
    self.assert_(res['OK'])

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PutFileTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CreateFTSReqCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

