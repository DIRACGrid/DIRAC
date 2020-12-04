""" Test class for ElementInspectorAgent
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from six.moves import queue as Queue

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
  mocker.patch(
      "DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )

  elementInspectorAgent = ElementInspectorAgent()
  elementInspectorAgent.log = gLogger
  elementInspectorAgent.log.setLevel('DEBUG')
  elementInspectorAgent._AgentModule__configDefaults = mockAM
  elementInspectorAgent.initialize()
  elementInspectorAgent.elementsToBeChecked = elementsToBeCheckedValue

  result = elementInspectorAgent._execute()

  assert result == {'OK': True, 'Value': None}
