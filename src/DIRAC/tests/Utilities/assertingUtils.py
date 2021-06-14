"""Functions that assert conditions for use in pytest tests.

The AgentOptionsTest can be used to check consistency of options for Agents.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import inspect
import importlib
import logging
import os

from pprint import pformat

from mock import patch, call, MagicMock as Mock
from diraccfg import CFG

import DIRAC

LOG = logging.getLogger(__name__)


def _parseOption(outDict, inDict, optionPrefix=''):
  """Parse the ConfigTemplates options.

  Handle some special cases.
  """
  LOG.info("Parsing into %s, from %s, prefix %r", outDict, inDict, optionPrefix)
  for option, value in inDict.items():
    optionName = "/".join([optionPrefix, option]).strip('/')
    LOG.info("Parsing %r with %r", optionName, value)
    if isinstance(value, six.string_types) and value.lower() in ('no', 'false'):
      outDict[optionName] = False
    elif isinstance(value, six.string_types) and value.lower() in ('yes', 'true'):
      outDict[optionName] = True
    elif isinstance(value, six.string_types) and ',' in value:
      outDict[optionName] = [val.strip() for val in value.split(',')]
    elif isinstance(value, dict):
      _parseOption(outDict, value, optionPrefix=optionName)
    elif isinstance(value, six.string_types):
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


def AgentOptionsTest(agentPath, options, mocker):
  """Test the consistency of options in ConfigTemplate and initialize of the agent.

  :param str agentPath: Module where the agent can be found, e.g. DIRAC.Core.Agent.CoreAgent
  :param list ignoreOptions: list of options to ignore during checks
  :param mocker: the mocker fixture from pytest-mock
  """
  agentPathSplit = agentPath.split('.')
  systemName = agentPathSplit[1]
  agentName = agentPathSplit[-1]

  agentModule = importlib.import_module(agentPath)
  LOG.info("Agents: %s %s", agentPath, agentModule)
  agentClass = None
  agentLocation = os.path.dirname(agentModule.__file__)

  options = options if options is not None else {}
  ignoreOptions = options.get('IgnoreOptions', [])
  specialMocks = options.get('SpecialMocks', {})

  # mock everything but the agentClass
  for name, member in inspect.getmembers(agentModule):
    LOG.info("Mocking? %s, %s, %s, isclass(%s)", name, callable(member), type(member), inspect.isclass(member))
    if name != 'AgentModule' and '_AgentModule__executeModuleCycle' in dir(member):
      LOG.info("Found the agent class %s, %s", name, member)
      agentClass = member
      continue
    elif name == 'AgentModule':
      continue
    if callable(member) or inspect.ismodule(member):
      LOG.info("Mocking: %s, %s, %s", name, member, type(member))
      mocker.patch(agentPath + "." + name, new=Mock())

  if specialMocks is not None:
    for name, retVal in specialMocks.items():
      mocker.patch(agentPath + "." + name, new=Mock(return_value=retVal))

  def returnDefault(*args):
    if len(args) > 1:
      LOG.debug("ReturningDefault: %s, %s", args, type(args[1]))
      return args[1]
    LOG.debug("ReturningDefault: None")
    return None

  getOptionMock = Mock(name="am_getOption", side_effect=returnDefault)

  def instrument(*args, **kwargs):
    """Mock some functions that come from the AgentModule and are not present otherwise."""
    args[0].am_getControlDirectory = Mock()
    args[0].am_getOption = getOptionMock
    args[0].am_getModuleParam = Mock()
    args[0].am_getWorkDirectory = Mock()
    args[0].am_setOption = Mock()
    args[0].log = Mock()
    return None
  initMock = Mock(side_effect=instrument)

  class MockAgentModule(object):
    def __init__(self, *args, **kwargs):
      instrument(self)

  patchBase = patch.object(agentClass, '__bases__', (MockAgentModule,))
  with \
          patchBase, \
          patch(agentPath + ".AgentModule.__init__", new=initMock), \
          patch("DIRAC.Core.Base.AgentModule.AgentModule.am_getOption", new=getOptionMock):
    patchBase.is_local = True
    agentInstance = agentClass(agentName="sys/name", loadName="sys/name")
    instrument(agentInstance)

  for func in ['initialize', 'beginExecution']:
    if hasattr(agentInstance, func):
      getattr(agentInstance, func)()
  checkAgentOptions(getOptionMock, systemName, agentName, agentLocation, ignoreOptions=ignoreOptions)


def checkAgentOptions(getOptionMock, systemName, agentName, agentLocation,
                      ignoreOptions=None):
  """Ensure that all the agent options are properly documented.

  :param getOptionMock: Mock object for agentmodule.get_amOption function
  :param str systemName: name of the **System**
  :param str agentName: name of the **Agent**
  :param list ignoreOptions: list of options to ignore
  """
  if ignoreOptions is None:
    ignoreOptions = []

  # add some options that can be set, see the AgentModule for all of them
  ignoreOptions.extend(['PollingTime', 'Status', 'Enabled',
                        'MaxCycles', 'LogOutputs', 'ControlDirectory',
                        'shifterProxy'])
  ignoreOptions = list(set(ignoreOptions))
  config = CFG()

  LOG.info("Testing %s/%s, ignoring options %s", systemName, agentName, ignoreOptions)

  # expect the ConfigTemplate one level above the agent module
  configFilePath = os.path.join(agentLocation, '..', 'ConfigTemplate.cfg')
  config.loadFromFile(configFilePath)
  optionsDict = config.getAsDict('Agents/%s' % agentName)
  outDict = {}
  _parseOption(outDict, optionsDict)
  optionsDict = outDict
  LOG.info("Calls: %s", pformat(getOptionMock.call_args_list))
  LOG.info("Options found in ConfigTemplate: %s ", list(optionsDict.keys()))

  # check that values in ConfigTemplate are used
  for option, value in optionsDict.items():
    if any(ignoreOp in option for ignoreOp in ignoreOptions):
      LOG.info("From Agent: ignoring option %r with value %r, (%s)", option, value, type(value))
      continue
    LOG.info("Looking for call to option %r with value %r, (%s)", option, value, type(value))
    if not isinstance(value, bool) and not value:  # empty string, list, dict ...
      assert any(call(option, null) in getOptionMock.call_args_list for null in ({}, set(), [], '', 0, None))
    else:
      assert call(option, value) in getOptionMock.call_args_list or \
          call(option, [value]) in getOptionMock.call_args_list

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
    assert optionsDict[optionName] == optionValue or [optionsDict[optionName]] == optionValue
