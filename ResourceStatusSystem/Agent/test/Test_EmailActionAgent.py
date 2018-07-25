""" Test class for EmailAction / EmailAgent
"""

# imports
import unittest
import importlib
from mock import MagicMock

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Agent.EmailAgent import EmailAgent
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.EmailAction import EmailAction

gLogger.setLevel('DEBUG')


class TestCase(unittest.TestCase):
  """ Base class for the EmailAction / EmailAgent test cases
  """

  def setUp(self):
    self.mockAM = MagicMock()
    self.agent_m = importlib.import_module('DIRAC.ResourceStatusSystem.Agent.EmailAgent')
    self.agent_m.AgentModule = self.mockAM
    self.agent_m.FileReport = MagicMock()
    self.agent = EmailAgent()
    self.agent.log = gLogger
    self.agent.am_getOption = self.mockAM
    self.agent.log.setLevel('DEBUG')

    self.action_m = importlib.import_module('DIRAC.ResourceStatusSystem.PolicySystem.Actions.EmailAction')
    self.action_m.AgentModule = self.mockAM

    name = "LogStatusAction"
    decisionParams = {'status': 'Banned', 'reason': 'test', 'tokenOwner': None,
                      'active': 'Active', 'name': 'test1.test1.ch', 'element': 'Resource',
                      'elementType': 'StorageElement', 'statusType': 'ReadAccess'}

    enforcementResult = {'Status': 'Banned', 'Reason': 'test ###',
			 'PolicyAction': [('LogStatusAction', 'LogStatusAction'),
					  ('LogPolicyResultAction', 'LogPolicyResultAction')]}

    singlePolicyResults = [{'Status': 'Banned', 'Policy': {'command': ('DowntimeCommand', 'DowntimeCommand'),
							   'name': 'DT_END',
							   'args': {'hours': 0, 'onlyCache': True},
							   'type': 'DTOngoing',
							   'module': 'DowntimePolicy',
							   'description': 'Ongoing and scheduled down-times'},
			    'Reason': 'test'},
			   {'Status': 'Active', 'Policy': {'command': None,
							   'name': 'AlwaysActiveForResource',
							   'args': None,
							   'type': 'AlwaysActive',
							   'module': 'AlwaysActivePolicy',
							   'description': 'A Policy that always returns Active'},
                            'Reason': 'AlwaysActive'}]

    self.action = EmailAction(name, decisionParams, enforcementResult, singlePolicyResults)
    self.action.log = gLogger
    self.action.am_getOption = self.mockAM
    self.agent.log.setLevel('DEBUG')

    self.tc_mock = MagicMock()
    self.tm_mock = MagicMock()


class EmailActionSuccess(TestCase):

  def test__createTheDatabase(self):
    res = self.action.run()
    self.assertTrue(res['OK'])


class EmailAgentSuccess(TestCase):

  def test__getData(self):
    self.agent.diracAdmin = MagicMock()
    res = self.agent.execute()
    self.assertTrue(res['OK'])

#############################################################################
# Test Suite run
#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(EmailActionSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(EmailAgentSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
