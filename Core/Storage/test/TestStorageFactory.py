import unittest,types,time
from DIRAC.Core.Storage.StorageFactory import StorageFactory

class StorageFactoryTestCase(unittest.TestCase):
  """ Base class for the StorageFactory test cases
  """
  def test_getStorageName(self):
    factory = StorageFactory()
    initialName = 'RAWFileDestination'
    res = factory.getStorageName(initialName)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'CERN-tape')

  def test_getStorage(self):

    storageDict = {}
    storageDict['StorageName'] = 'IN2P3-SRM2'
    storageDict['ProtocolName'] = 'SRM2'
    storageDict['Protocol'] = 'srm'
    storageDict['Host'] = 'ccsrmtestv2.in2p3.fr'
    storageDict['Port'] = '8443/srm/managerv2?SFN='
    storageDict['Path'] = '/pnfs/in2p3.fr/data'
    storageDict['SpaceToken'] = 'LHCb_FAKE'
    factory = StorageFactory()
    res = factory.getStorage(storageDict)
    self.assert_(res['OK'])
    storageStub = res['Value']
    res = storageStub.getParameters()
    self.assert_(res['OK'])
    parameterDict = res['Value']
    self.assertEqual(parameterDict,storageDict)

    res = storageStub.getPFNBase(withPort=False)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'srm://ccsrmtestv2.in2p3.fr/pnfs/in2p3.fr/data')
    res = storageStub.getPFNBase(withPort=True)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'srm://ccsrmtestv2.in2p3.fr:8443/srm/managerv2?SFN=/pnfs/in2p3.fr/data')

    res = storageStub.getUrl('/lhcb/production/DC06/test.file',withPort=False)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'srm://ccsrmtestv2.in2p3.fr/pnfs/in2p3.fr/data/lhcb/production/DC06/test.file')
    res = storageStub.getUrl('/lhcb/production/DC06/test.file',withPort=True)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'srm://ccsrmtestv2.in2p3.fr:8443/srm/managerv2?SFN=/pnfs/in2p3.fr/data/lhcb/production/DC06/test.file')

  def test_getStorages(self):
    factory = StorageFactory()
    storageName = 'IN2P3-SRM2'
    protocolList = ['SRM2']
    res = factory.getStorages(storageName,protocolList)
    self.assert_(res['OK'])
    storageStubs = res['Value']['StorageObjects']
    storageStub = storageStubs[0]

    storageDict = {}
    storageDict['StorageName'] = 'IN2P3-SRM2'
    storageDict['ProtocolName'] = 'SRM2'
    storageDict['Protocol'] = 'srm'
    storageDict['Host'] = 'ccsrmtestv2.in2p3.fr'
    storageDict['Port'] = '8443/srm/managerv2?SFN='
    storageDict['Path'] = '/pnfs/in2p3.fr/data'
    storageDict['SpaceToken'] = 'LHCb_FAKE'
    res = storageStub.getParameters()
    self.assert_(res['OK'])
    parameterDict = res['Value']
    self.assertEqual(parameterDict,storageDict)

    res = storageStub.getPFNBase(withPort=False)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'srm://ccsrmtestv2.in2p3.fr/pnfs/in2p3.fr/data')
    res = storageStub.getPFNBase(withPort=True)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'srm://ccsrmtestv2.in2p3.fr:8443/srm/managerv2?SFN=/pnfs/in2p3.fr/data')

    res = storageStub.getUrl('/lhcb/production/DC06/test.file',withPort=False)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'srm://ccsrmtestv2.in2p3.fr/pnfs/in2p3.fr/data/lhcb/production/DC06/test.file')
    res = storageStub.getUrl('/lhcb/production/DC06/test.file',withPort=True)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'srm://ccsrmtestv2.in2p3.fr:8443/srm/managerv2?SFN=/pnfs/in2p3.fr/data/lhcb/production/DC06/test.file')

    res = storageStub.remove('srm://ccsrmtestv2.in2p3.fr:8443/srm/managerv2?SFN=/pnfs/in2p3.fr/data/lhcb/production/DC06/test.file')

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(StorageFactoryTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CreateFTSReqCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

