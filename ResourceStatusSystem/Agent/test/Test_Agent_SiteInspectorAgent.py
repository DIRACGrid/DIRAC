""" Test class for SiteInspectorAgent
"""
import Queue

# imports
import pytest
from mock import MagicMock

# DIRAC Components
from DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent import SiteInspectorAgent
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
    "sitesToBeCheckedValue", [
        (Queue.Queue()),
        (queueFilled)
    ])
def test__execute(mocker, sitesToBeCheckedValue):
  """ Testing SiteInspectorAgent.execute()
  """

  mocker.patch("DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent.AgentModule.__init__")
  mocker.patch("DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent.AgentModule.am_getOption", side_effect=mockAM)

  siteInspectorAgent = SiteInspectorAgent()
  siteInspectorAgent.log = gLogger
  siteInspectorAgent.log.setLevel('DEBUG')
  siteInspectorAgent._AgentModule__configDefaults = mockAM
  siteInspectorAgent.initialize()
  siteInspectorAgent.sitesToBeChecked = sitesToBeCheckedValue

  result = siteInspectorAgent._execute()

  assert result == {'OK': True, 'Value': None}
