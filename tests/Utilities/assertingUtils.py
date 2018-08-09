"""Functions that assert conditions."""

import logging
import os
from pprint import pformat

from mock import call

import DIRAC
from DIRAC.Core.Utilities.CFG import CFG

LOG = logging.getLogger(__name__)


def checkAgentOptions(getOptionMock, systemName, agentName,
                      ignoreOptions=None, extension='DIRAC'):
  """Ensure that all the agent options are properly documented.

  :param getOptionMock: Mock object for agentmodule.get_amOption function
  :param str systemName: name of the **System**
  :param str agentName: name of the **Agent**
  :param list ignoreOptions: list of options to ignore
  :param str extension: name of the DIRAC **Extension** where the Agent comes from
  """
  if ignoreOptions is None:
    ignoreOptions = []

  config = CFG()
  # get the location where DIRAC is in from basefolder/DIRAC/__ini__.py
  configFilePath = os.path.join(os.path.dirname(os.path.dirname(DIRAC.__file__)),
                                extension, systemName, 'ConfigTemplate.cfg')
  config.loadFromFile(configFilePath)
  optionsDict = config.getAsDict('Agents/%s' % agentName)
  for option, value in optionsDict.iteritems():
    if isinstance(value, basestring) and value.lower() in ('no', 'false'):
      optionsDict[option] = False
    if isinstance(value, basestring) and value.lower() in ('yes', 'true'):
      optionsDict[option] = True
    if isinstance(value, basestring) and ',' in value:
      optionsDict[option] = [val.strip() for val in value.split(',')]

    try:
      optionsDict[option] = int(value)
    except ValueError:
      pass

  LOG.info("Calls: %s", pformat(getOptionMock.mock_calls))
  for option, value in optionsDict.iteritems():
    LOG.info("Checking option %s with value %r", option, value)
    if option in ignoreOptions:
      continue
    if not isinstance(value, bool) and not value:  # empty string, list, dict ...
      assert any(call(option, null) in getOptionMock.mock_calls for null in ({}, set(), [], ''))
    else:
      assert call(option, value) in getOptionMock.mock_calls
