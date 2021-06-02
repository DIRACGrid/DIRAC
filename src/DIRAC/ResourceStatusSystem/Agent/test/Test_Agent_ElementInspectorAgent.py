""" Test class for ElementInspectorAgent
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# imports
from mock import MagicMock

# DIRAC Components
from DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent import ElementInspectorAgent
from DIRAC import gLogger

gLogger.setLevel('DEBUG')

# Mock Objects
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None
mockSM = MagicMock()

elemDict = {
    'status': 'status',
    'name': 'site',
    'site': 'site',
    'vO': 'all',
    'element': 'Site',
    'statusType': 'all',
    'elementType': 'Site'}


def test__execute(mocker):
  """ Testing JobCleaningAgent()._getAllowedJobTypes()
  """

  mocker.patch("DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent.AgentModule.__init__")
  mocker.patch("DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent.PEP")
  mocker.patch(
      "DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )

  elementInspectorAgent = ElementInspectorAgent()
  elementInspectorAgent.log = gLogger
  elementInspectorAgent.log.setLevel('DEBUG')
  elementInspectorAgent._AgentModule__configDefaults = mockAM
  elementInspectorAgent.initialize()

  result = elementInspectorAgent._execute(elemDict)

  assert result is None
