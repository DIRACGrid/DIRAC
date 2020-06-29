""" This is a test for EmailAction and EmailAgent
    Requires DB to be present and ResourceStatusHandler to be working
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import importlib
import unittest
from mock import MagicMock

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.EmailAction import EmailAction
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.Core.Base.Script import parseCommandLine

parseCommandLine()
gLogger.setLevel('DEBUG')


class TestEmailActionAgentTestCase(unittest.TestCase):

  def setUp(self):

    self.rssClient = ResourceStatusClient()

    name = 'LogStatusAction'
    decisionParams = {'status': 'Banned', 'reason': 'test', 'tokenOwner': None,
                      'active': 'Active', 'name': 'ProductionSandboxSE', 'element': 'Resource',
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
    self.action.log.setLevel('DEBUG')

    self.mockAM = MagicMock()
    self.agent_m = importlib.import_module('DIRAC.ResourceStatusSystem.Agent.EmailAgent')
    self.agent_m.AgentModule = self.mockAM
    self.agent = self.agent_m.EmailAgent()
    self.agent.am_getOption = self.mockAM
    self.agent.log = gLogger
    self.agent.log.setLevel('DEBUG')

  def tearDown(self):
    pass


class EmailActionAgent(TestEmailActionAgentTestCase):

  def test__emailActionAgent(self):

    # clean up
    res = self.rssClient.delete('ResourceStatusCache')
    self.assertTrue(res['OK'])

    res = self.action.run()
    self.assertTrue(res['OK'])

    res = self.agent.execute()
    self.assertTrue(res['OK'])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestEmailActionAgentTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(EmailActionAgent))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
