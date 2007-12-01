import unittest,types,time
from DIRAC.Core.Storage.StorageFactory import StorageFactory

class StoragePlugInTestCase(unittest.TestCase):
  """ Base class for the StoragePlugin test cases
  """

  def test_setUp(self):
    """ Create test storage
    """
    factory = StorageFactory()
    res = factory.getStorages('CERN-RAW', ['SRM2'])
    self.assert_(res['OK'])
    print res

class PutFileTestCase(StoragePlugInTestCase):

  def test_putFile(self):
    print 1

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PutFileTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CreateFTSReqCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

