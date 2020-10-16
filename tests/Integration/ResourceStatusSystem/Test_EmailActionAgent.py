""" This is a test for EmailAction and EmailAgent
    Requires DB to be present and ResourceStatusHandler to be working
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib
from mock import MagicMock

from DIRAC import gLogger
gLogger.setLevel('DEBUG')

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.ResourceStatusSystem.PolicySystem.Actions.EmailAction import EmailAction
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


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


action = EmailAction(name, decisionParams, enforcementResult, singlePolicyResults)
action.log = gLogger
action.log.setLevel('DEBUG')
mockAM = MagicMock()
agent_m = importlib.import_module('DIRAC.ResourceStatusSystem.Agent.EmailAgent')
agent_m.AgentModule = mockAM
agent = agent_m.EmailAgent()
agent.am_getOption = mockAM
agent.log = gLogger
agent.log.setLevel('DEBUG')


def test__emailActionAgent():

  rssClient = ResourceStatusClient()
  # clean up
  res = rssClient.delete('ResourceStatusCache')
  assert res['OK'] is True

  res = action.run()
  assert res['OK'] is True

  res = agent.execute()
  assert res['OK'] is True
