""" Starting from the PDP object, it makes the full chain of policies

    It requires

    ResourceStatus
    {
      Policies
      {
        AlwaysActiveForResource
        {
          matchParams
          {
            element = Resource
          }
          policyType = AlwaysActive
        }
        AlwaysBannedForSE1SE2
        {
          matchParams
          {
            name = SE1,SE2
          }
          policyType = AlwaysBanned
        }
        AlwaysBannedForSite
        {
          matchParams
          {
            element = Site
          }
          policyType = AlwaysBanned
        }
        AlwaysBannedForSite2
        {
          matchParams
          {
            element = Site
            domain = test
          }
          policyType = AlwaysBanned
        }
      }
      PolicyActions
      {
        LogStatusAction
        {
        }
        LogPolicyResultAction
        {
        }
      }
    }

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

import unittest
import sys

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP


class PDPTestCase(unittest.TestCase):
  """ PDPTestCase
  """

  def setUp(self):
    """ test case set up
    """

    gLogger.setLevel('DEBUG')

  def tearDown(self):
    """ clean up
    """
    pass


class PDPDecision_Success(PDPTestCase):

  def test_site(self):

    pdp = PDP()

    # empty
    pdp.setup(None)
    res = pdp.takeDecision()
    self.assertTrue(res['OK'])

    # site
    decisionParams = {'element': 'Site',
                      'name': 'Site1',
                      'elementType': None,
                      'statusType': 'ReadAccess',
                      'status': 'Active',
                      'reason': None,
                      'tokenOwner': None}
    pdp.setup(decisionParams)
    res = pdp.takeDecision()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['policyCombinedResult']['Status'], 'Banned')

    # site2
    decisionParams = {'element': 'Site',
                      'name': 'Site2',
                      'elementType': 'CE',
                      'statusType': 'ReadAccess',
                      'status': 'Active',
                      'domain': 'test',
                      'reason': None,
                      'tokenOwner': None}
    pdp.setup(decisionParams)
    res = pdp.takeDecision()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['policyCombinedResult']['Status'], 'Banned')

    # mySE
    decisionParams = {'element': 'Resource',
                      'name': 'mySE',
                      'elementType': 'StorageElement',
                      'statusType': 'ReadAccess',
                      'status': 'Active',
                      'reason': None,
                      'tokenOwner': None}
    pdp.setup(decisionParams)
    res = pdp.takeDecision()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['policyCombinedResult']['Status'], 'Active')

    # SE1
    decisionParams = {'element': 'Resource',
                      'name': 'SE1',
                      'elementType': 'StorageElement',
                      'statusType': 'ReadAccess',
                      'status': 'Active',
                      'reason': None,
                      'tokenOwner': None}
    pdp.setup(decisionParams)
    res = pdp.takeDecision()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['policyCombinedResult']['Status'], 'Banned')

################################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PDPTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PDPDecision_Success))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
