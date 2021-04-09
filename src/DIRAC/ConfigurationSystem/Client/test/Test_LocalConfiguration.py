""" Unit tests for PathFinder only for functions that I added
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import mock
import pytest
import random

from DIRAC import S_OK, S_ERROR
import DIRAC.ConfigurationSystem.Client.LocalConfiguration


doc_description = """
This is an example of a typical team description.

With indents and explanatory inserts::

  * possible option
  * possible option

UnprocessedUnit:
  Some interesting information

"""
doc_usage = """
Usage:
  my-script [options] ...
"""
doc_gen_opts = """
General options:
  -o  --option <value>         : Option=value to add
  -s  --section <value>        : Set base section for relative parsed options
  -c  --cert <value>           : Use server certificate to connect to Core Services
  -d  --debug                  : Set debug mode (-ddd is extra debug)
  -   --cfg=                   : Load additional config file
  -   --autoreload             : Automatically restart if there's any change in the module
  -   --license                : Show DIRAC's LICENSE
  -h  --help                   : Shows this help
"""
doc_options = """
Options:
  -H  --host <value>  : host
  -V  --vo <value>    : vo
"""
doc_arguments = """
Arguments:
  info:  some info
"""
doc_example = """
Example:
  $ my-script -H dirac
  Hello dirac
"""

doc_blocks = [doc_usage, doc_options, doc_gen_opts, doc_arguments, doc_example]

opts = ()
args = []
output = []
result = None
exitCode = None


def mock_notice(*a):
  global output
  output.append(' '.join(a))


def mock_exit(c=0):
  global exitCode
  exitCode = c


def mock_gnu(*a):
  global opts, args
  return opts, args


def action(a):
  global result
  result = a if a else 'no'
  return S_OK()


@pytest.fixture
def localCFG(monkeypatch):
  monkeypatch.setattr(DIRAC.ConfigurationSystem.Client.LocalConfiguration.DIRAC, "exit", mock_exit)
  monkeypatch.setattr(DIRAC.ConfigurationSystem.Client.LocalConfiguration.getopt, "gnu_getopt", mock_gnu)
  monkeypatch.setattr(DIRAC.ConfigurationSystem.Client.LocalConfiguration.gLogger, "notice", mock_notice)
  localCFG = DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration()
  # It's local test, do not contact Configuration Server
  localCFG.disableCS()
  return localCFG


@pytest.mark.parametrize("blocks_order", [doc_blocks for i in range(10) if not random.shuffle(doc_blocks)])
def test_script_head(localCFG, blocks_order):
  global output
  output = []
  localCFG.setUsageMessage('\n'.join([doc_description, '\n'.join(blocks_order)]))
  localCFG.showHelp()
  assert '\n'.join(output) == ''.join([doc_description, doc_usage, doc_gen_opts, doc_arguments, doc_example])


def test_register_options(localCFG):
  global output, opts, args, result
  args = []
  output = []
  localCFG.setUsageMessage('\n'.join([doc_description, '\n'.join(doc_blocks)]))
  localCFG.registerCmdOpt("O:", "Option=", "my option", action)
  localCFG.registerCmdOpt("A", "AnotherOpt", "my another option", action)
  localCFG.registerCmdOpt("N", "NoActionOp", "my noaction option")
  localCFG.showHelp()
  optBlock = "\nOptions:\n  %s" % '\n  '.join(["-O  --Option <value>         : my option",
                                               "-A  --AnotherOpt             : my another option",
                                               "-N  --NoActionOp             : my noaction option"])
  assert optBlock in '\n'.join(output)

  # Check action result
  for opts, fn_res in [([('-O', 'y')], 'y'),
                       ([('--Option', 'y')], 'y'),
                       ([('-A', None)], 'no'),
                       ([('--AnotherOpt', None)], 'no'),
                       ([('-N', None)], None)]:
    result = None
    localCFG.isParsed = False
    localCFG.initialized = False
    assert localCFG.loadUserData() == S_OK()
    assert result == fn_res
    assert localCFG.getUnprocessedSwitches() == ([('N', None)] if opts[0] == ('-N', None) else [])


sArg = ('SingArg', 'SingArg: my single argument', "  SingArg:  my single argument")
fArg = ('<ThisArg|ThatArg>', ('ThisArg: my this argument', 'ThatArg: my that argument'),
        "  ThisArg:  my this argument\n  ThatArg:  my that argument")
lArg = ('ListArg [ListArg]', ['ListArg: my list of arguments'], "  ListArg:  my list of arguments")
aValues = ['yes', 'no']

list_optional = (lArg, False, None, 'defVal')
float_optional = (fArg, False, None, 'defVal')
single_optional = (sArg, False, None, 'defVal')
list_mandatory = (lArg, True, None, 'defVal')
float_mandatory = (fArg, True, None, 'defVal')
single_mandatory = (sArg, True, None, 'defVal')
list_checkvalues = (lArg, True, aValues, 'defVal')
float_checkvalues = (fArg, True, aValues, 'defVal')
single_checkvalues = (sArg, True, aValues, 'defVal')

params = [([list_optional, float_optional, single_optional], []),
          ([float_optional, list_optional, single_optional], []),
          ([list_mandatory, list_mandatory], []),
          ([single_mandatory, single_optional, single_mandatory], []),
          ([single_mandatory, float_mandatory, list_optional],
          [(['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'], ['a', 'b', ['c', 'd']]),
           (['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', ['c']]),
           (['a', 'b'], ['a', 'b'], ['a', 'b', 'defVal']),
           (['a'], [], ())]),
          ([single_mandatory, float_mandatory, list_mandatory],
          [(['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'], ['a', 'b', ['c', 'd']]),
           (['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', ['c']]),
           (['a', 'b'], [], ())]),
          ([single_mandatory, list_mandatory, float_mandatory],
          [(['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'], ['a', ['b', 'c'], 'd']),
           (['a', 'b', 'c'], ['a', 'b', 'c'], ['a', ['b'], 'c']),
           (['a', 'b'], [], ())])]


@pytest.mark.parametrize("argsData, expected", params)
def test_register_arguments(localCFG, argsData, expected):
  global output, opts, args, result, exitCode
  output = []
  opts = ()
  exitCode = 0
  useBlock = " [options] ..."
  argBlock = "\nArguments:"

  if not expected:
    with pytest.raises(Exception):
      for arg, mandatory, values, default in argsData:
        localCFG.registerCmdArg(arg[1], mandatory, values, default)
  else:
    for arg, mandatory, values, default in argsData:
      useBlock += ' ' + arg[0]
      for a in arg[2].split('\n'):
        argBlock += '\n' + a
        if values:
          argBlock += ' [%s]' % ', '.join(values)
        if default:
          argBlock += ' [default: defVal]'
        if not mandatory:
          argBlock += ' (optional)'
      localCFG.registerCmdArg(arg[1], mandatory, values, default)

  if expected:
    localCFG.showHelp()

    assert argBlock in '\n'.join(output)
    assert useBlock in '\n'.join(output)

    # Check action result
    for args, getargs, getgroupargs in expected:
      localCFG.isParsed = False
      localCFG.initialized = False
      assert localCFG.loadUserData() == S_OK()
      if not getargs:
        localCFG.getPositionalArguments()
        assert exitCode == 1
      else:
        assert localCFG.getPositionalArguments() == getargs
        assert localCFG.getPositionalArguments(True) == getgroupargs
