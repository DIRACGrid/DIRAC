""" Test class for SiteInspectorAgent
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# imports
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

site = {
    'status': 'status',
    'name': 'site',
    'vO': 'some_vo',
    'site': 'site',
    'element': 'Site',
    'statusType': 'all',
    'elementType': 'Site'}


def test__execute(mocker):
  """ Testing SiteInspectorAgent.execute()
  """

  mocker.patch("DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent.AgentModule.__init__")
  mocker.patch("DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent.PEP")
  mocker.patch(
      "DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )

  siteInspectorAgent = SiteInspectorAgent()
  siteInspectorAgent.log = gLogger
  siteInspectorAgent.log.setLevel('DEBUG')
  siteInspectorAgent._AgentModule__configDefaults = mockAM
  siteInspectorAgent.initialize()

  result = siteInspectorAgent._execute(site)

  assert result is None
