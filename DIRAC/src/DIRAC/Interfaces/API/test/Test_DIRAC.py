""" Unit tests for the Dirac interface module
"""
# pylint: disable=no-member, protected-access, missing-docstring
import logging

from pprint import pformat
import pytest
from unittest.mock import MagicMock, call

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC import S_OK

logging.basicConfig()
LOG = logging.getLogger("TestDirac")
LOG.setLevel(logging.ERROR)


@pytest.fixture
def dirac():
    d = Dirac()
    d.log = MagicMock(name="Log")
    d.log.debug = d.log
    d.log.info = d.log
    d.log.notice = d.log
    d.log.verbose = d.log
    d.log.error = d.log
    d.log.warn = d.log
    return d


@pytest.fixture
def job():
    from DIRAC.Interfaces.API.Job import Job

    job = Job(stdout="printer", stderr="/dev/null")
    job.setInputSandbox(["LFN:/vo/user/i/initial/important.tar.gz", "/abspath/absfile.xml", "file_in_pwd.xml"])
    return job


@pytest.fixture
def osmock():
    os = MagicMock(return_value=False, name="OS")
    os.environ = dict()
    os.environ["DIRAC"] = "/root/dirac"

    def expandMock(*args, **kwargs):
        return args[0]

    def existsMock(*args, **kwargs):
        print("exists", args)
        if any(f in args[0] for f in ("jobDescription", "/pwd/file_in_pwd.xml")):
            return True
        if "absfile" in args[0] and args[0].startswith("/abspath"):
            return True
        return False

    def joinMock(*args):
        path = "/"
        for l in args:
            path += l.strip("/") + "/"
        return path.rstrip("/")

    os.getcwd.side_effect = ["/pwd/"] + ["/pwd/tempFolder/"] * 100

    os.path.expandvars.side_effect = expandMock
    os.path.exists.side_effect = existsMock
    os.path.isabs.side_effect = lambda x: x.startswith("/")
    os.path.isdir.return_value = False
    os.path.join.side_effect = joinMock
    os.path.basename.side_effect = lambda x: x.rsplit("/", 1)[-1]
    return os


@pytest.fixture
def confMock():
    gConf = MagicMock(name="gConfig")

    def getVal(*args, **kwargs):
        if len(args) == 2:
            return args[1]
        return "defaultValue"

    gConf.getValue.side_effect = getVal
    return gConf


def test_basicJob(dirac):
    jdl = "Parameter=Value;Parameter2=Value2"
    ret = dirac._Dirac__getJDLParameters(jdl)
    assert ret["OK"]
    assert "Parameter" in ret["Value"]
    assert ret["Value"]["Parameter"] == "Value"
    assert "Parameter2" in ret["Value"]
    assert ret["Value"]["Parameter2"] == "Value2"


def test_JobJob(dirac, job):
    ret = dirac._Dirac__getJDLParameters(job)
    assert ret["OK"]
    assert ret["Value"]["StdOutput"] == "printer"
    assert ret["Value"]["StdError"] == "/dev/null"


def test_runLocal(dirac, job, mocker, osmock, confMock):
    mocker.patch("DIRAC.Interfaces.API.Dirac.os", new=osmock)
    mocker.patch("DIRAC.Interfaces.API.Dirac.tarfile", new=MagicMock(return_value=False))
    mocker.patch("builtins.open")
    mocker.patch("DIRAC.Interfaces.API.Dirac.gConfig", new=confMock)
    tempMock = mocker.patch("DIRAC.Interfaces.API.Dirac.tempfile")
    tempMock.mkdtemp.return_value = "tempFolder"
    shMock = mocker.patch("DIRAC.Interfaces.API.Dirac.shutil", new=MagicMock(name="Shutil"))
    sysMock = mocker.patch("DIRAC.Interfaces.API.Dirac.systemCall")
    sysMock.return_value = S_OK([0, "No output", "No errors"])
    dirac.getFile = MagicMock(return_value=S_OK())
    ret = dirac.runLocal(job)
    LOG.info("dirac log calls: %s", dirac.log.call_args_list)
    LOG.info("CallStack: %s", pformat(ret.get("CallStack", {})))
    try:
        assert sysMock.call_args_list[0][1]["cmdSeq"] == ["dirac-jobexec", "jobDescription.xml", "-o", "LogLevel=DEBUG"]
    except AssertionError:
        assert sysMock.call_args_list[0][1]["cmdSeq"] == [
            "/root/dirac/scripts/dirac-jobexec",
            "jobDescription.xml",
            "-o",
            "LogLevel=DEBUG",
        ]
    assert ret.get("Message", None) is None
    assert ret["OK"]
    assert call("/abspath/absfile.xml", "/pwd/tempFolder/") in shMock.copy.call_args_list
    assert call("/pwd/file_in_pwd.xml", "/pwd/tempFolder/") in shMock.copy.call_args_list
    assert call("/vo/user/i/initial/important.tar.gz") in dirac.getFile.call_args_list
