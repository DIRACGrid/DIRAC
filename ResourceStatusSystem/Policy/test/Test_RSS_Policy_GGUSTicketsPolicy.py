''' Test_RSS_Policy_GGUSTicketsPolicy
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import DIRAC.ResourceStatusSystem.Policy.GGUSTicketsPolicy as moduleTested


class GGUSTicketsPolicy_TestCase(unittest.TestCase):

  def setUp(self):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.GGUSTicketsPolicy

  def tearDown(self):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass


class GGUSTicketsPolicy_Success(GGUSTicketsPolicy_TestCase):

  def test_instantiate(self):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual('GGUSTicketsPolicy', module.__class__.__name__)

  def test_evaluate(self):
    ''' tests the method _evaluate
    '''

    module = self.testClass()

    res = module._evaluate({'OK': False, 'Message': 'Bo!'})
    self.assertTrue(res['OK'])
    self.assertEqual('Error', res['Value']['Status'])
    self.assertEqual('Bo!', res['Value']['Reason'])

    res = module._evaluate({'OK': True, 'Value': None})
    self.assertTrue(res['OK'])
    self.assertEqual('Unknown', res['Value']['Status'])
    self.assertEqual('No values to take a decision', res['Value']['Reason'])

    res = module._evaluate({'OK': True, 'Value': []})
    self.assertTrue(res['OK'])
    self.assertEqual('Unknown', res['Value']['Status'])
    self.assertEqual('No values to take a decision', res['Value']['Reason'])

    res = module._evaluate({'OK': True, 'Value': [{'A': 1}]})
    self.assertTrue(res['OK'])
    self.assertEqual('Error', res['Value']['Status'])
    self.assertEqual('Expected OpenTickets key for GGUSTickets', res['Value']['Reason'])

    res = module._evaluate({'OK': True, 'Value': [{'OpenTickets': 0}]})
    self.assertTrue(res['OK'])
    self.assertEqual('Active', res['Value']['Status'])
    self.assertEqual('NO GGUSTickets unsolved', res['Value']['Reason'])

    self.assertRaises(KeyError, module._evaluate, {'OK': True, 'Value': [{'OpenTickets': 1}]})

    res = module._evaluate({'OK': True, 'Value': [{'OpenTickets': 1, 'Tickets': '1a'}]})
    self.assertTrue(res['OK'])
    self.assertEqual('Degraded', res['Value']['Status'])
    self.assertEqual('1 GGUSTickets unsolved: 1a', res['Value']['Reason'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTicketsPolicy_TestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTicketsPolicy_Success))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
