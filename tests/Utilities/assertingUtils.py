"""Functions that assert conditions."""

import logging
import os
from pprint import pformat

from mock import call

import DIRAC
from DIRAC.Core.Utilities.CFG import CFG

LOG = logging.getLogger(__name__)


def _parseOption(outDict, inDict, optionPrefix=''):
  """Parse the ConfigTemplates options.

  Handle some special cases.
  """
  LOG.info("Parsing into %s, from %s, prefix %r", outDict, inDict, optionPrefix)
  for option, value in inDict.iteritems():
    optionName = "/".join([optionPrefix, option]).strip('/')
    LOG.info("Parsing %r with %r", optionName, value)
    if isinstance(value, basestring) and value.lower() in ('no', 'false'):
      outDict[optionName] = False
    elif isinstance(value, basestring) and value.lower() in ('yes', 'true'):
      outDict[optionName] = True
    elif isinstance(value, basestring) and ',' in value:
      outDict[optionName] = [val.strip() for val in value.split(',')]
    elif isinstance(value, dict):
      _parseOption(outDict, value, optionPrefix=optionName)
    elif isinstance(value, basestring):
      outDict[optionName] = value
      if value.isdigit():
        try:
          outDict[optionName] = int(value)
        except ValueError:
          pass
      else:
        try:
          outDict[optionName] = float(value)
        except ValueError:
          pass


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

  # add some options that can be set, see the AgentModule for all of them
  ignoreOptions.extend(['PollingTime', 'Status', 'Enabled', 'MaxCycles', 'LogOutputs', 'ControlDirectory'])
  ignoreOptions = list(set(ignoreOptions))
  config = CFG()

  LOG.info("Testing %s/%s, ignoring options %s", systemName, agentName, ignoreOptions)

  # get the location where DIRAC is in from basefolder/DIRAC/__ini__.py
  configFilePath = os.path.join(os.path.dirname(os.path.dirname(DIRAC.__file__)),
                                extension, systemName, 'ConfigTemplate.cfg')
  config.loadFromFile(configFilePath)
  optionsDict = config.getAsDict('Agents/%s' % agentName)
  outDict = {}
  _parseOption(outDict, optionsDict)
  optionsDict = outDict
  LOG.info("Calls: %s", pformat(getOptionMock.call_args_list))
  LOG.info("Options found in ConfigTemplate: %s ", list(optionsDict.keys()))

  # check that values in ConfigTemplate are used
  for option, value in optionsDict.iteritems():
    if any(ignoreOp in option for ignoreOp in ignoreOptions):
      LOG.info("From Agent: ignoring option %r with value %r, (%s)", option, value, type(value))
      continue
    LOG.info("Looking for call to option %r with value %r, (%s)", option, value, type(value))
    if not isinstance(value, bool) and not value:  # empty string, list, dict ...
      assert any(call(option, null) in getOptionMock.call_args_list for null in ({}, set(), [], '', 0))
    else:
      assert call(option, value) in getOptionMock.call_args_list

  # check that options used in the agent are in the ConfigTemplates
  for opCall in getOptionMock.call_args_list:
    optionArguments = opCall[0]
    if len(optionArguments) != 2:
      continue
    optionName = optionArguments[0]
    optionValue = optionArguments[1]
    if optionName in ignoreOptions:
      LOG.info("From Template: ignoring option %r with %r", optionName, optionValue)
      continue
    LOG.info("Checking Template option %r with %r", optionName, optionValue)
    assert optionName in optionsDict
    if not optionsDict[optionName]:
      assert not optionValue
      continue
    assert optionsDict[optionName] == optionValue
