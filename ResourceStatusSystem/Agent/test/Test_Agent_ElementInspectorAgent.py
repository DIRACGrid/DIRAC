""" Test class for ElementInspectorAgent
"""
import Queue

# imports
import pytest
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

queueFilled = Queue.Queue()
queueFilled.put({'status': 'status',
                 'name': 'site',
                 'site': 'site',
                 'element': 'Site',
                 'statusType': 'all',
                 'elementType': 'Site'})


@pytest.mark.parametrize(
    "elementsToBeCheckedValue", [
        (Queue.Queue()),
        (queueFilled)
    ])
def test__execute(mocker, elementsToBeCheckedValue):
  """ Testing JobCleaningAgent()._getAllowedJobTypes()
  """

  mocker.patch("DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent.AgentModule.__init__")
  mocker.patch("DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent.AgentModule.am_getOption", side_effect=mockAM)

  elementInspectorAgent = ElementInspectorAgent()
  elementInspectorAgent.log = gLogger
  elementInspectorAgent.log.setLevel('DEBUG')
  elementInspectorAgent._AgentModule__configDefaults = mockAM
  elementInspectorAgent.initialize()
  elementInspectorAgent.elementsToBeChecked = elementsToBeCheckedValue

  result = elementInspectorAgent._execute()

  assert result == {'OK': True, 'Value': None}
