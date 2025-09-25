""" Test for Profiler.py
"""
import time
from os.path import dirname, join
from subprocess import Popen

import pytest

import DIRAC
from DIRAC.Core.Utilities.Profiler import Profiler

# Mark this entire module as slow
pytestmark = pytest.mark.slow


def test_base():
    p = Profiler()
    res = p.pid()
    assert res["OK"] is False
    res = p.status()
    assert res["OK"] is False

    mainProcess = Popen(
        [
            "python",
            join(dirname(DIRAC.__file__), "tests/Utilities/ProcessesCreator_withChildren.py"),
        ]
    )
    time.sleep(1)
    p = Profiler(mainProcess.pid)
    res = p.pid()
    assert res["OK"] is True, res
    res = p.status()
    assert res["OK"] is True, res
    res = p.runningTime()
    assert res["OK"] is True, res
    assert res["Value"] > 0

    res = p.memoryUsage()
    assert res["OK"] is True, res
    assert res["Value"] > 0
    resWC = p.memoryUsage(withChildren=True)
    assert resWC["OK"] is True, res
    assert resWC["Value"] > 0
    assert resWC["Value"] >= res["Value"]

    res = p.vSizeUsage()
    assert res["OK"] is True, res
    assert res["Value"] > 0
    resWC = p.vSizeUsage(withChildren=True)
    assert resWC["OK"] is True, res
    assert resWC["Value"] > 0
    assert resWC["Value"] >= res["Value"]

    res = p.vSizeUsage()
    assert res["OK"] is True, res
    assert res["Value"] > 0
    resWC = p.vSizeUsage(withChildren=True)
    assert resWC["OK"] is True, res
    assert resWC["Value"] > 0
    assert resWC["Value"] >= res["Value"]

    res = p.numThreads()
    assert res["OK"] is True, res
    assert res["Value"] > 0
    resWC = p.numThreads(withChildren=True)
    assert resWC["OK"] is True, res
    assert resWC["Value"] > 0
    assert resWC["Value"] >= res["Value"]

    res = p.cpuPercentage()
    assert res["OK"] is True, res
    assert res["Value"] >= 0
    resWC = p.cpuPercentage(withChildren=True)
    assert resWC["OK"] is True, res
    assert resWC["Value"] >= 0
    assert resWC["Value"] >= res["Value"]


@pytest.mark.flaky(reruns=10)
def test_cpuUsage():
    mainProcess = Popen(
        [
            "python",
            join(dirname(DIRAC.__file__), "tests/Utilities/ProcessesCreator_withChildren.py"),
        ]
    )
    time.sleep(2)
    p = Profiler(mainProcess.pid)
    res = p.pid()
    assert res["OK"] is True, res
    res = p.status()
    assert res["OK"] is True, res

    # user
    res = p.cpuUsageUser()
    assert res["OK"] is True, res
    assert res["Value"] > 0
    resC = p.cpuUsageUser(withChildren=True)
    assert resC["OK"] is True
    assert resC["Value"] > 0
    assert resC["Value"] >= res["Value"]

    res = p.cpuUsageUser()
    assert res["OK"] is True, res
    assert res["Value"] > 0
    resC = p.cpuUsageUser(withChildren=True)
    assert resC["OK"] is True
    assert resC["Value"] > 0
    assert resC["Value"] >= res["Value"]

    resT = p.cpuUsageUser(withTerminatedChildren=True)
    assert resT["OK"] is True
    assert resT["Value"] > 0
    assert resT["Value"] >= res["Value"]

    resTC = p.cpuUsageUser(withChildren=True, withTerminatedChildren=True)
    assert resTC["OK"] is True
    assert resTC["Value"] > 0
    assert resTC["Value"] >= res["Value"]

    # system
    res = p.cpuUsageSystem()
    assert res["OK"] is True, res
    assert res["Value"] >= 0
    resWC = p.cpuUsageSystem(withChildren=True)
    assert resWC["OK"] is True, res
    assert resWC["Value"] >= 0
    assert resWC["Value"] >= res["Value"]

    res = p.cpuUsageSystem()
    assert res["OK"] is True, res
    assert res["Value"] > 0
    resC = p.cpuUsageSystem(withChildren=True)
    assert resC["OK"] is True
    assert resC["Value"] > 0
    assert resC["Value"] >= res["Value"]

    resT = p.cpuUsageSystem(withTerminatedChildren=True)
    assert resT["OK"] is True
    assert resT["Value"] > 0
    assert resT["Value"] >= res["Value"]

    resTC = p.cpuUsageSystem(withChildren=True, withTerminatedChildren=True)
    assert resTC["OK"] is True
    assert resTC["Value"] > 0
    assert resTC["Value"] >= res["Value"]

    # After this the main process will no-longer exist
    mainProcess.wait()

    res = p.cpuUsageUser()
    assert res["OK"] is False
    assert res["Errno"] == 3
