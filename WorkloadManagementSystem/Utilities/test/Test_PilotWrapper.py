""" This is a test of the creation of the pilot wrapper
"""

# pylint: disable=protected-access, invalid-name, no-self-use

import os
import base64
import bz2

from DIRAC.WorkloadManagementSystem.Utilities.PilotWrapper import pilotWrapperScript


def test_scriptEmpty():
  """ test script creation
  """
  res = pilotWrapperScript()

  assert 'cmd = "python dirac-pilot.py "' in res
  assert 'os.environ["someName"]="someValue"' not in res


def test_scriptoptions():
  """ test script creation
  """

  res = pilotWrapperScript(
      pilotFilesCompressedEncodedDict={'dirac-install.py': 'someContentOfDiracInstall',
                                       'someOther.py': 'someOtherContent'},
      pilotOptions="-c 123 --foo bar")

  assert "with open('dirac-install.py', 'w') as fd:" in res
  assert 'os.environ["someName"]="someValue"' not in res


def test_scriptReal():
  """ test script creation
  """
  diracInstall = os.path.join(os.getcwd(), 'Core/scripts/dirac-install.py')
  with open(diracInstall, "r") as fd:
    diracInstall = fd.read()
  diracInstallEncoded = base64.b64encode(bz2.compress(diracInstall, 9))

  diracPilot = os.path.join(os.getcwd(), 'WorkloadManagementSystem/PilotAgent/dirac-pilot.py')
  with open(diracPilot, "r") as fd:
    diracPilot = fd.read()
  diracPilotEncoded = base64.b64encode(bz2.compress(diracPilot, 9))

  diracPilotTools = os.path.join(os.getcwd(), 'WorkloadManagementSystem/PilotAgent/pilotTools.py')
  with open(diracPilotTools, "r") as fd:
    diracPilotTools = fd.read()
  diracPilotToolsEncoded = base64.b64encode(bz2.compress(diracPilotTools, 9))

  diracPilotCommands = os.path.join(os.getcwd(), 'WorkloadManagementSystem/PilotAgent/pilotCommands.py')
  with open(diracPilotCommands, "r") as fd:
    diracPilotCommands = fd.read()
  diracPilotCommandsEncoded = base64.b64encode(bz2.compress(diracPilotCommands, 9))

  res = pilotWrapperScript(
      pilotFilesCompressedEncodedDict={'dirac-install.py': diracInstallEncoded,
                                       'dirac-pilot.py': diracPilotEncoded,
                                       'pilotTools.py': diracPilotToolsEncoded,
                                       'pilotCommands.py': diracPilotCommandsEncoded},
      pilotOptions="-c 123 --foo bar")

  assert "with open('dirac-pilot.py', 'w') as fd:" in res
  assert "with open('dirac-install.py', 'w') as fd:" in res
  assert 'os.environ["someName"]="someValue"' not in res


def test_scriptWithEnvVars():
  """ test script creation
  """
  res = pilotWrapperScript(
      pilotFilesCompressedEncodedDict={'dirac-install.py': 'someContentOfDiracInstall',
                                       'someOther.py': 'someOtherContent'},
      pilotOptions="-c 123 --foo bar",
      envVariables={'someName': 'someValue',
                    'someMore': 'oneMore'})

  assert 'os.environ["someName"]="someValue"' in res


def test_scriptPilot3():
  """ test script creation
  """
  res = pilotWrapperScript(
      pilotFilesCompressedEncodedDict={'proxy': 'thisIsSomeProxy'},
      pilotOptions="-c 123 --foo bar",
      envVariables={'someName': 'someValue',
                    'someMore': 'oneMore'},
      location='lhcb-portal.cern.ch')

  assert 'os.environ["someName"]="someValue"' in res
  assert 'lhcb-portal.cern.ch' in res
