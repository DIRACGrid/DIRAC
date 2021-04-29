''' Test_RSS_Policy_PilotEfficiencyPolicy
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import DIRAC.ResourceStatusSystem.Policy.PilotEfficiencyPolicy as moduleTested

################################################################################


class PilotEfficiencyPolicy_TestCase(unittest.TestCase):

  def setUp(self):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.PilotEfficiencyPolicy

  def tearDown(self):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass


################################################################################

class PilotEfficiencyPolicy_Success(PilotEfficiencyPolicy_TestCase):

  def test_instantiate(self):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual('PilotEfficiencyPolicy', module.__class__.__name__)

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

    res = module._evaluate({'OK': True, 'Value': [{}]})
    self.assertTrue(res['OK'])
    self.assertEqual('Unknown', res['Value']['Status'])
    self.assertEqual('No values to take a decision', res['Value']['Reason'])

    res = module._evaluate({'OK': True, 'Value': [{'Aborted': 0, 'Deleted': 0,
                                                   'Done': 0, 'Failed': 0}]})
    self.assertTrue(res['OK'])
    self.assertEqual('Unknown', res['Value']['Status'])
    self.assertEqual('Not enough pilots to take a decision', res['Value']['Reason'])

    # Pilot efficiency is now available directly from the command result, in percent.
    # The key is 'PilotJobEff'  It is calculated as (total - aborted-failed)/total * 100

    result = {'Aborted': 10, 'Deleted': 0, 'Done': 10, 'Failed': 0}
    result['PilotJobEff'] = self._pilotEff(result)

    res = module._evaluate({'OK': True, 'Value': [result]})
    self.assertTrue(res['OK'])
    self.assertEqual('Banned', res['Value']['Status'])
    self.assertEqual('Pilots Efficiency of 50.00', res['Value']['Reason'])

    result = {'Aborted': 0, 'Deleted': 0, 'Done': 30, 'Failed': 10}
    result['PilotJobEff'] = self._pilotEff(result)

    res = module._evaluate({'OK': True, 'Value': [result]})
    self.assertTrue(res['OK'])
    self.assertEqual('Degraded', res['Value']['Status'])
    self.assertEqual('Pilots Efficiency of 75.00', res['Value']['Reason'])

    result = {'Aborted': 0, 'Deleted': 0, 'Done': 19, 'Failed': 1}
    result['PilotJobEff'] = self._pilotEff(result)

    res = module._evaluate({'OK': True, 'Value': [result]})
    self.assertTrue(res['OK'])
    self.assertEqual('Active', res['Value']['Status'])
    self.assertEqual('Pilots Efficiency of 95.00', res['Value']['Reason'])

  def _pilotEff(self, result):
    """
    Calculate pilot efficiency.

    :param result: the original dictionary of the form {status: count, status: count...}
    :return: pilot success rate in percent
    """
    return (sum(result.values()) - result['Aborted'] - result['Failed']) / sum(result.values()) * 100

################################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PilotEfficiencyPolicy_TestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotEfficiencyPolicy_Success))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
