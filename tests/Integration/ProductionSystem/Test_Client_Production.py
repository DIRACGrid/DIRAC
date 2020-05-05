""" This is a test of the chain
    ProductionClient -> ProductionManagerHandler -> ProductionDB

    It supposes that the DB is present, and that the service is running
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import json

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
from DIRAC.ProductionSystem.Client.ProductionStep import ProductionStep


class TestClientProductionTestCase(unittest.TestCase):

  def setUp(self):
    self.prodClient = ProductionClient()

  def tearDown(self):
    pass


class ProductionClientChain(TestClientProductionTestCase):

  def test_addAndRemove(self):
    # add a production step
    prodStep = ProductionStep()
    res = self.prodClient.addProductionStep(prodStep)
    self.assertTrue(res['OK'])

    # get the updated production description
    prodDesc = self.prodClient.prodDescription

    # create the production starting from the production description
    res = self.prodClient.addProduction('prodName', json.dumps(prodDesc))
    self.assertTrue(res['OK'])
    prodID = res['Value']

    # try to add another production with the same Name (this should fail)
    res = self.prodClient.addProduction('prodName', json.dumps(prodDesc))
    self.assertFalse(res['OK'])

    # delete the production
    res = self.prodClient.deleteProduction(prodID)
    print(type(prodID))
    self.assertTrue(res['OK'])

    # delete non existing one (fails)
    res = self.prodClient.deleteProduction(prodID)
    self.assertFalse(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientProductionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ProductionClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
