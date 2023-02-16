"""This module contains tests for the (DIP) StorageElement

The tests upload gets a directory

Requirements:

* Running StorageElement instance, containing an LFN folder '/Jenkins', writable by the jenkins_user proxy
* A jenkins_user proxy

"""


import os
import sys
import tempfile
import time

import pytest

from pprint import pprint, pformat

from DIRAC.Core.Base.Script import parseCommandLine

parseCommandLine()

from DIRAC.Core.Security.ProxyInfo import getProxyInfo  # noqa: module-import-not-at-top-of-file
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup  # noqa: module-import-not-at-top-of-file


@pytest.fixture(scope="module")
def se():
    from DIRAC.Resources.Storage.StorageElement import StorageElementItem as StorageElement

    return StorageElement("SE-1")


try:
    res = getProxyInfo()
    if not res["OK"]:
        raise Exception(res["Message"])

    proxyInfo = res["Value"]
    username = proxyInfo["username"]
    vo = getVOForGroup(proxyInfo["group"])

    if not vo:
        raise ValueError("Proxy has no VO")

    LFN_PATH = f"/{vo}/test/unit-test/StorageElement/"

except Exception as e:  # pylint: disable=broad-except
    print(repr(e))
    sys.exit(2)


@pytest.fixture
def tempFile():
    """Create unique temprary file, will be cleaned up at the end."""
    with tempfile.NamedTemporaryFile("w") as theTemp:
        theTemp.write(str(time.time()))
        theTemp.flush()
        yield theTemp.name


def assertResult(res, lfn, sub="Successful"):
    """Check if lfn is in Successful or Failed, given by sub."""
    assert res["OK"], res.get("Message", "All OK")
    assert sub in res["Value"]
    assert res["Value"][sub], f"Did not find {sub!r} in result"
    assert lfn in res["Value"][sub]
    assert "Files" in res["Value"][sub][lfn]
    assert "Size" in res["Value"][sub][lfn]


def test_getDirectory(se, tempFile, tmp_path):
    print("\n\n#########################################################################\n\n\t\t\tSE.GetDirectory\n")
    targetDir = os.path.join(LFN_PATH, "myGetDir")
    targetLFN = os.path.join(targetDir, os.path.basename(tempFile))
    putFileRes = se.putFile({targetLFN: tempFile})
    pprint(putFileRes)
    localDir = os.path.join(tmp_path, "myGetDir")
    print("is not a dir?", localDir, os.path.isdir(localDir))
    assert not os.path.isdir(localDir)
    getRes = se.getDirectory(targetDir, localPath=localDir)
    assertResult(getRes, lfn=targetDir)
    assert os.path.isdir(localDir)


def test_putDirectory(se, tempFile, tmp_path):
    print("\n\n#########################################################################\n\n\t\t\tSE.PutDirectory\n")
    targetDir = os.path.join(LFN_PATH, "myPutDir")
    localDir = os.path.join(tmp_path, "myPutDir")
    os.mkdir(localDir)
    with open(os.path.join(localDir, os.path.basename(tempFile)), "w") as newFile:
        newFile.write("foo bar" + (str(time.time())))
    getRes = se.putDirectory({targetDir: localDir})
    # Check that the get was successful
    pprint(getRes)
    assertResult(getRes, lfn=targetDir)
    assert os.path.isdir(localDir)
    assert se.listDirectory(targetDir)


def test_getOccupancy(se):
    print("\n\n#########################################################################\n\n\t\t\tSE.PutDirectory\n")
    res = se.getOccupancy()
    assert res["OK"], pformat(res)
