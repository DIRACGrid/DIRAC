########################################################################
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/11 18:04:25
########################################################################

""" :mod: SubprocessTests
    =======================

    .. module: SubprocessTests
    :synopsis: unittest for Subprocess module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for Subprocess module
"""
import time
import platform
from os.path import dirname, join
from subprocess import Popen

import pytest

from DIRAC.Core.Utilities.Subprocess import systemCall, shellCall, pythonCall, getChildrenPIDs, Subprocess

# Mark this entire module as slow
pytestmark = pytest.mark.slow

cmd = ["sleep", "2"]


def pyfunc(_name):
    time.sleep(2)


@pytest.mark.parametrize("timeout, expected", [(False, True), (3, True), (1, False)])
def test_calls(timeout, expected):
    ret = systemCall(timeout, cmdSeq=cmd)
    assert ret["OK"] == expected

    ret = shellCall(timeout, cmdSeq=" ".join(cmd))
    assert ret["OK"] == expected

    ret = pythonCall(timeout, pyfunc, "something")
    assert ret["OK"] == expected


def test_getChildrenPIDs():
    import os

    os.system("echo $PWD")
    mainProcess = Popen(["python", join(dirname(__file__), "ProcessesCreator.py")])
    time.sleep(1)
    res = getChildrenPIDs(mainProcess.pid)
    # Depends on the start method, 'fork' produces 3 processes, 'spawn' produces 4
    assert len(res) in [3, 4]
    for p in res:
        assert isinstance(p, int)

    mainProcess.wait()


@pytest.mark.skipif(platform.system() != "Linux", reason="Requires GNU extensions to echo")
def test_decodingCommandOutput():
    retVal = systemCall(10, ["echo", "-e", "-n", r"\xdf"])
    assert retVal["OK"]
    assert retVal["Value"] == (0, "\ufffd", "")

    retVal = systemCall(10, ["echo", "-e", r"\xdf"])
    assert retVal["OK"]
    assert retVal["Value"] == (0, "\ufffd\n", "")

    sp = Subprocess()
    retVal = sp.systemCall(r"""python -c 'import os; os.fdopen(2, "wb").write(b"\xdf")'""", shell=True)
    assert retVal["OK"]
    assert retVal["Value"] == (0, "", "\ufffd")
