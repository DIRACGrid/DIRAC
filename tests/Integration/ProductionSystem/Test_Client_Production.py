""" This is a test of the chain
    ProductionClient -> ProductionManagerHandler -> ProductionDB

    It supposes that the DB is present, and that the service is running
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient


class TestClientProductionTestCase(unittest.TestCase):

  def setUp(self):
    self.prodClient = ProductionClient()

  def tearDown(self):
    pass


class ProductionClientChain(TestClientProductionTestCase):

  def test_addAndRemove(self):
    # add
    res = self.prodClient.addProduction('MyprodName')
    self.assertTrue(res['OK'])
    prodID = res['Value']

    # try to add again (this should fail)
    res = self.prodClient.addProduction('MyprodName')
    self.assertFalse(res['OK'])

    # really delete
    res = self.prodClient.deleteProduction(prodID)
    self.assertTrue(res['OK'])

    # delete non existing one (fails)
    res = self.prodClient.deleteProduction(prodID)
    self.assertFalse(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientProductionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ProductionClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
