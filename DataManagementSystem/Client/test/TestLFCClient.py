import unittest,types,time
from DIRAC.DataManagementSystem.Client.LcgFileCatalogClient import LcgFileCatalogClient

class LFCClientTestCase(unittest.TestCase):
  """ Base class for the TransferDB test cases
  """
  def setUp(self):
    self.lfc = LcgFileCatalogClient()

class TestInitCase(LFCClientTestCase):

  def test_init(self):
    lfns = ['/lhcb/production/DC06/phys-lumi2/00001620/DST/0000/00001620_00000597_5.dst','/lhcb/production/DC06/phys-lumi2/00001620/DST/0000/00001620_00000598_5.dst','/lhcb/production/DC06/phys-lumi2/00001620/DST/0000/00001620_00000600_5.dst']
    print lfns
    res = self.LFCProxy.getReplicas(lfns)
    print res

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestInitCase)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

