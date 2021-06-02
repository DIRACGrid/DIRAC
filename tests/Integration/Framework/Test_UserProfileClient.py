from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC.Core.Security import ProxyInfo
from DIRAC.FrameworkSystem.Client.UserProfileClient import UserProfileClient

import pytest

parseCommandLine()

up = UserProfileClient("Web/application/desktop")
key = "key"
value = "value"


def _userInfo():
  retVal = ProxyInfo.getProxyInfo()
  assert retVal["OK"], retVal
  proxyInfo = retVal["Value"]
  currentUser = proxyInfo["username"]
  currentDN = proxyInfo["DN"]
  currentGroup = proxyInfo["group"]

  return currentUser, currentDN, currentGroup


def test_storeRetrieveDelete():
  retVal = up.storeVar(key, value, {"ReadAccess": "ALL", "PublishAccess": "ALL"})
  assert retVal["OK"], retVal
  assert retVal["Value"] == 1

  retVal = up.retrieveVar(key)
  assert retVal["OK"], retVal
  assert retVal["Value"] == value

  retVal = up.listAvailableVars()
  assert retVal["OK"], retVal
  assert retVal["Value"] == (("adminusername", "prod", "undefined", key),)

  retVal = up.getUserProfiles()
  assert retVal["OK"], retVal
  assert retVal["Value"] == {"Web/application/desktop": {key: "s5:value"}}

  retVal = up.getVarPermissions(key)
  assert retVal["OK"], retVal
  assert retVal["Value"] == {"PublishAccess": "ALL", "ReadAccess": "ALL"}

  retVal = up.deleteVar(key)
  assert retVal["OK"], retVal
  assert retVal["Value"] == 1


def test_retrieveAllVars():
  retVal = up.retrieveAllVars()
  assert retVal["OK"], retVal
  assert retVal["Value"] == {}

  retVal = up.storeVar(key, value, {"ReadAccess": "ALL", "PublishAccess": "ALL"})
  assert retVal["OK"], retVal

  retVal = up.retrieveAllVars()
  assert retVal["OK"], retVal
  assert retVal["Value"] == {key: value}

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
    ]
)
def test_setVarPermissions(readAccess, publishAccess):
  retVal = up.storeVar(key, value, {"ReadAccess": "USER", "PublishAccess": "ALL"})
  assert retVal["OK"], retVal
  newPermissions = {"ReadAccess": readAccess, "PublishAccess": publishAccess}

  retVal = up.setVarPermissions(key, newPermissions)
  assert retVal["OK"], retVal
  assert retVal["Value"] == 1

  retVal = up.getVarPermissions(key)
  assert retVal["OK"], retVal
  assert retVal["Value"] == newPermissions

  retVal = up.deleteVar(key)
  assert retVal["OK"], retVal


def test_retrieveVarFromUser():
  currentUser, currentDN, currentGroup = _userInfo()

  retVal = up.storeVar(key, value, {"ReadAccess": "ALL", "PublishAccess": "ALL"})
  assert retVal["OK"], retVal

  retVal = up.retrieveVarFromUser(currentUser, currentGroup, key)
  assert retVal["OK"], retVal
  assert retVal["Value"] == value

  retVal = up.deleteVar(key)
  assert retVal["OK"], retVal
