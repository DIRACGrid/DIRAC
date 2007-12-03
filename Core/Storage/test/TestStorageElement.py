import unittest,types,time
from DIRAC.Core.Storage.StorageElement import StorageElement

class StorageElementTestCase(unittest.TestCase):
  """ Base class for the StorageElement test cases
  """
  def test_initializeSE(self):
    self.storageElement = StorageElement('CERN-RAW')
    self.storageElement.dump()
    print self.storageElement.getLocalProtocol()
    print self.storageElement.getRemoteProtocols()
    print self.storageElement.getLocalProtocols()
    print self.storageElement.getProtocols()
    print self.storageElement.getPrimaryProtocol()
    print self.storageElement.isLocalSE()

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(StorageElementTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CreateFTSReqCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

