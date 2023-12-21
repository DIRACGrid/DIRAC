""" JobWrapper test
"""
import os
import pytest

from DIRAC import gLogger

from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper
from DIRAC.Core.Security.ProxyInfo import getProxyInfo


@pytest.fixture
def setup():
    gLogger.setLevel("DEBUG")

    # get proxy
    proxyInfo = getProxyInfo(disableVOMS=True)
    proxyChain = proxyInfo["Value"]["chain"]
    proxyDumped = proxyChain.dumpAllToString()
    payloadProxy = proxyDumped["Value"]

    yield payloadProxy


def test_CreateAndSubmit(setup):
    jobParams = {
        "JobID": "1",
        "JobType": "Merge",
        "CPUTime": "1000000",
        "Executable": "dirac-jobexec",
        "Arguments": "helloWorld.xml -o LogLevel=DEBUG --cfg pilot.cfg",
        "InputSandbox": ["helloWorld.xml", "exe-script.py"],
    }
    resourceParams = {}
    optimizerParams = {}

    ceFactory = ComputingElementFactory()
    ceInstance = ceFactory.getCE("InProcess")
    assert ceInstance["OK"]
    computingElement = ceInstance["Value"]

    if "pilot.cfg" in os.listdir("."):
        jobParams.setdefault("ExtraOptions", "pilot.cfg")
        res = createJobWrapper(
            2, jobParams, resourceParams, optimizerParams, extraOptions="pilot.cfg", logLevel="DEBUG"
        )
    else:
        res = createJobWrapper(2, jobParams, resourceParams, optimizerParams, logLevel="DEBUG")
    assert res["OK"], res.get("Message")
    wrapperFile = res["Value"]["JobExecutablePath"]

    res = computingElement.submitJob(wrapperFile, setup)
    assert res["OK"], res.get("Message")
    assert res["Value"] == 0, res.get("Value")
