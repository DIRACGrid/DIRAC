import pytest
import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.FrameworkSystem.Client.UserProfileClient import UserProfileClient

up = UserProfileClient("Web/application/desktop")
key = "key"
value = "value"


def _userInfo():
    retVal = getProxyInfo()
    assert retVal["OK"], retVal
    proxyInfo = retVal["Value"]
    currentUser = proxyInfo["username"]
    currentDN = proxyInfo["DN"]
    currentGroup = proxyInfo["group"]
    userVO = getVOForGroup(currentGroup)

    return currentUser, currentDN, currentGroup, userVO


def test_storeRetrieveDelete():
    _, _, currentGroup, _ = _userInfo()

    retVal = up.storeVar(key, value, {"ReadAccess": "ALL", "PublishAccess": "ALL"})
    assert retVal["OK"], retVal
    assert retVal["Value"] == 1

    try:
        retVal = up.retrieveVar(key)
        assert retVal["OK"], retVal
        assert retVal["Value"] == value

        retVal = up.listAvailableVars()
        assert retVal["OK"], retVal
        assert retVal["Value"] == [["adminusername", currentGroup, "vo", key]]

        retVal = up.getUserProfiles()
        assert retVal["OK"], retVal
        assert retVal["Value"] == {"Web/application/desktop": {key: "s5:value"}}

        retVal = up.getVarPermissions(key)
        assert retVal["OK"], retVal
        assert retVal["Value"] == {"PublishAccess": "ALL", "ReadAccess": "ALL"}
    finally:
        retVal = up.deleteVar(key)
        assert retVal["OK"], retVal
        assert retVal["Value"] == 1


def test_retrieveAllVars():
    retVal = up.retrieveAllVars()
    assert retVal["OK"], retVal
    assert retVal["Value"] == {}

    retVal = up.storeVar(key, value, {"ReadAccess": "ALL", "PublishAccess": "ALL"})
    assert retVal["OK"], retVal

    try:
        retVal = up.retrieveAllVars()
        assert retVal["OK"], retVal
        assert retVal["Value"] == {key: value}
    finally:
        retVal = up.deleteVar(key)
        assert retVal["OK"], retVal


def test_listStatesForWeb():
    retVal = up.listStatesForWeb()
    assert retVal["OK"], retVal
    assert retVal["Value"] == []

    retVal = up.listStatesForWeb({"PublishAccess": "ALL"})
    assert retVal["OK"], retVal
    assert retVal["Value"] == []


@pytest.mark.parametrize(
    "readAccess, publishAccess",
    [
        ["ALL", "USER"],
        ["ALL", "GROUP"],
        ["ALL", "VO"],
        ["ALL", "ALL"],
    ],
)
def test_setVarPermissions(readAccess, publishAccess):
    retVal = up.storeVar(key, value, {"ReadAccess": "USER", "PublishAccess": "ALL"})
    assert retVal["OK"], retVal
    newPermissions = {"ReadAccess": readAccess, "PublishAccess": publishAccess}

    try:
        retVal = up.setVarPermissions(key, newPermissions)
        assert retVal["OK"], retVal
        assert retVal["Value"] == 1

        retVal = up.getVarPermissions(key)
        assert retVal["OK"], retVal
        assert retVal["Value"] == newPermissions
    finally:
        retVal = up.deleteVar(key)
        assert retVal["OK"], retVal


def test_retrieveVarFromUser():
    currentUser, _, currentGroup, _ = _userInfo()

    retVal = up.storeVar(key, value, {"ReadAccess": "ALL", "PublishAccess": "ALL"})
    assert retVal["OK"], retVal

    try:
        retVal = up.retrieveVarFromUser(currentUser, currentGroup, key)
        assert retVal["OK"], retVal
        assert retVal["Value"] == value
    finally:
        retVal = up.deleteVar(key)
        assert retVal["OK"], retVal
