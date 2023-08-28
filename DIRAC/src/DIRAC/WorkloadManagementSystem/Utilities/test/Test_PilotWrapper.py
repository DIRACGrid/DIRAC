""" This is a test of the creation of the pilot wrapper
"""

import os
import base64
import bz2

from DIRAC.WorkloadManagementSystem.Utilities.PilotWrapper import pilotWrapperScript


def test_scriptEmpty():
    """test script creation"""
    res = pilotWrapperScript()

    assert "$py dirac-pilot.py " in res
    assert 'os.environ["someName"]="someValue"' not in res


def test_scriptoptions():
    """test script creation"""

    res = pilotWrapperScript(
        pilotFilesCompressedEncodedDict={
            "dirac_boh.py": "someContentOfDiracInstall",
            "someOther.py": "someOtherContent",
        },
        pilotOptions="-c 123 --foo bar",
    )

    assert "with open('dirac_boh.py', 'wb') as fd:" in res
    assert 'os.environ["someName"]="someValue"' not in res


def test_scriptReal():
    """test script creation"""

    diracPilot = os.path.join(os.getcwd(), "src/DIRAC/Core/Base/API.py")  # just some random file
    with open(diracPilot, "rb") as fd:
        diracPilot = fd.read()
    diracPilotEncoded = base64.b64encode(bz2.compress(diracPilot, 9))

    diracPilotTools = os.path.join(os.getcwd(), "src/DIRAC/Core/Base/API.py")  # just some random file
    with open(diracPilotTools, "rb") as fd:
        diracPilotTools = fd.read()
    diracPilotToolsEncoded = base64.b64encode(bz2.compress(diracPilotTools, 9))

    diracPilotCommands = os.path.join(os.getcwd(), "src/DIRAC/Core/Base/API.py")  # just some random file
    with open(diracPilotCommands, "rb") as fd:
        diracPilotCommands = fd.read()
    diracPilotCommandsEncoded = base64.b64encode(bz2.compress(diracPilotCommands, 9))

    res = pilotWrapperScript(
        pilotFilesCompressedEncodedDict={
            "dirac-pilot.py": diracPilotEncoded,
            "pilotTools.py": diracPilotToolsEncoded,
            "pilotCommands.py": diracPilotCommandsEncoded,
        },
        pilotOptions="-c 123 --foo bar",
    )

    assert "with open('dirac-pilot.py', 'wb') as fd:" in res
    assert 'os.environ["someName"]="someValue"' not in res


def test_scriptWithEnvVars():
    """test script creation"""
    res = pilotWrapperScript(
        pilotFilesCompressedEncodedDict={
            "dirac_boh.py": "someContentOfDiracInstall",
            "someOther.py": "someOtherContent",
        },
        pilotOptions="-c 123 --foo bar",
        envVariables={"someName": "someValue", "someMore": "oneMore"},
    )

    assert 'os.environ["someName"]="someValue"' in res


def test_scriptPilot3():
    """test script creation"""
    res = pilotWrapperScript(
        pilotFilesCompressedEncodedDict={"proxy": "thisIsSomeProxy"},
        pilotOptions="-c 123 --foo bar",
        envVariables={"someName": "someValue", "someMore": "oneMore"},
        location="lhcb-portal.cern.ch",
    )

    assert 'os.environ["someName"]="someValue"' in res
    assert "lhcb-portal.cern.ch" in res
