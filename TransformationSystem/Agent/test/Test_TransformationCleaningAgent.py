#!/bin/env python
"""
Tests for TransformationCleaningAgent module
"""

import pytest

from mock import MagicMock as Mock, patch

from DIRAC.TransformationSystem.Agent import TransformationCleaningAgent
from DIRAC.tests.Utilities.assertingUtils import checkAgentOptions

MODNAME = "DIRAC.TransformationSystem.Agent.TransformationCleaningAgent"

# pylint: disable=redefined-outer-name


@pytest.fixture
def transformationCleaningAgent():
  """Return an instance of the TransformationCleaningAgent."""
  with patch(MODNAME + ".AgentModule.__init__", new=Mock()):
    agent = TransformationCleaningAgent.TransformationCleaningAgent(agentName="Transformation/testing",
                                                                    loadName="Transformation/testing")
    # as we ignore the init from the baseclass some agent variables might not be present so we set them here
    # in any case with this we can check that log is called with proper error messages
    agent.log = Mock()
  return agent


def test_AgentOptions(transformationCleaningAgent):
  """Check that all options in ConfigTemplate are found in the initialize method, including default values."""
  tca = transformationCleaningAgent
  tca.am_getOption = Mock()
  tca.initialize()
  checkAgentOptions(tca.am_getOption, 'TransformationSystem', 'TransformationCleaningAgent',
                    ignoreOptions=['PollingTime', 'EnableFlag', 'shifterProxy'])
