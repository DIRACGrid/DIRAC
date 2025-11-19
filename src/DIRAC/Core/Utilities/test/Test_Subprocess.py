########################################################################
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/11 18:04:25
########################################################################

""":mod: SubprocessTests
=======================

.. module: SubprocessTests
:synopsis: unittest for Subprocess module
.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

unittest for Subprocess module
"""

import platform
import time
from os.path import dirname, join
from subprocess import Popen

import psutil
import pytest

from DIRAC.Core.Utilities.Subprocess import Subprocess, getChildrenPIDs, pythonCall, shellCall, systemCall

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


@pytest.fixture
def subprocess_instance():
    """Provides a Subprocess instance for testing."""
    subp = Subprocess()
    return subp


@pytest.fixture
def dummy_child():
    """Spawn a dummy process tree: parent -> child."""
    # Start a shell that sleeps, with a subprocess child
    parent = Popen(["bash", "-c", "sleep 10 & wait"])
    time.sleep(0.2)  # give it a moment to start
    yield parent
    # Ensure cleanup
    try:
        parent.terminate()
        parent.wait(timeout=1)
    except Exception:
        pass


def test_kill_child_process_tree(subprocess_instance, dummy_child):
    """Test that killChild kills both parent and its children."""
    subprocess_instance.childPID = dummy_child.pid
    parent_proc = psutil.Process(subprocess_instance.childPID)

    # Sanity check: parent should exist
    assert parent_proc.is_running()

    # It should have at least one sleeping child
    children = parent_proc.children(recursive=True)
    assert children, "Expected dummy process to have at least one child"

    # Kill the tree
    gone, alive = subprocess_instance.killChild(recursive=True)

    # Verify the parent and children are terminated
    for p in gone:
        assert not p.is_running(), f"Process {p.pid} still alive"
    for p in alive:
        assert not p.is_running(), f"Process {p.pid} still alive"

    # Verify parent is gone
    with pytest.raises(psutil.NoSuchProcess):
        psutil.Process(subprocess_instance.childPID)
