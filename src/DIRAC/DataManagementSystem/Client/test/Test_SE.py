import itertools
import os
import tempfile
import pytest
from unittest import mock

from diraccfg import CFG

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Resources.Storage.StorageElement import StorageElementItem


class fake_SRM2Plugin(StorageBase):
    def putFile(self, lfns, sourceSize=0):
        return S_OK({"Successful": dict.fromkeys(lfns, "srm:putFile"), "Failed": {}})

    def getTransportURL(self, path, protocols=False):
        return S_OK({"Successful": dict.fromkeys(path, "srm:getTransportURL"), "Failed": {}})


class fake_XROOTPlugin(StorageBase):
    def putFile(self, lfns, sourceSize=0):
        return S_OK({"Successful": dict.fromkeys(lfns, "root:putFile"), "Failed": {}})

    def getTransportURL(self, path, protocols=False):
        return S_OK({"Successful": dict.fromkeys(path, "root:getTransportURL"), "Failed": {}})


class fake_GSIFTPPlugin(StorageBase):
    def putFile(self, lfns, sourceSize=0):
        return S_OK({"Successful": dict.fromkeys(lfns, "gsiftp:putFile"), "Failed": {}})

    def getTransportURL(self, path, protocols=False):
        return S_OK({"Successful": dict.fromkeys(path, "gsiftp:getTransportURL"), "Failed": {}})


def mock_StorageFactory_generateStorageObject(
    self, storageName, pluginName, parameters, hideExceptions=False
):  # pylint: disable=unused-argument
    """Generate fake storage object"""
    storageObj = StorageBase(storageName, parameters)
    storageObj.pluginName = pluginName

    return S_OK(storageObj)

@pytest.fixture(scope="function")
def setup_environment(monkeypatch):
    def mock_init(self, useProxy=False, vo=None):
        self.proxy = useProxy
        self.resourceStatus = mock.MagicMock()
        self.vo = vo
        self.remoteProtocolSections = []
        self.localProtocolSections = []
        self.name = ""
        self.options = {}
        self.protocols = {}
        self.storages = {}

    monkeypatch.setattr(
        "DIRAC.Resources.Storage.StorageFactory.StorageFactory.__init__", mock_init
    )

    monkeypatch.setattr(
        "DIRAC.Resources.Storage.StorageFactory.StorageFactory._StorageFactory__generateStorageObject",
        mock_StorageFactory_generateStorageObject
    )

    monkeypatch.setattr(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        lambda self: S_OK(True)
    )

    monkeypatch.setattr(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation",
        lambda self: None
    )

    # Create test configuration file
    testCfgFileName = os.path.join(tempfile.gettempdir(), "test_StorageElement.cfg")
    cfgContent = """
    DIRAC
    {
      Setup=TestSetup
    }
    Resources{
      StorageElements{
        StorageA
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
          }
        }
        StorageB
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          AccessProtocol.0
          {
            Host =
            PluginName = SRM2
            Protocol = srm
            Path =
          }
        }
        StorageC
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          AccessProtocol.0
          {
            Host =
            PluginName = XROOT
            Protocol = root
            Path =
          }
        }
        StorageD
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          AccessProtocol.0
          {
            Host =
            PluginName = SRM2
            Protocol = srm
            Path =
          }
          AccessProtocol.1
          {
            Host =
            PluginName = XROOT
            Protocol = root
            Path =
          }
        }
        StorageE
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          WriteProtocols = root
          WriteProtocols += srm
          AccessProtocol.0
          {
            Host =
            PluginName = SRM2
            Protocol = srm
            Path =
          }
          AccessProtocol.1
          {
            Host =
            PluginName = XROOT
            Protocol = root
            Path =
          }
        }
        StorageX
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          WriteProtocols = gsiftp
          AccessProtocols = root
          AccessProtocol.0
          {
            Host =
            PluginName = GSIFTP
            Protocol = gsiftp
            Path =
          }
          AccessProtocol.1
          {
            Host =
            PluginName = XROOT
            Protocol = root
            Path =
          }
        }
        StorageY
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          AccessProtocols = gsiftp
          AccessProtocols += srm
          AccessProtocol.0
          {
            Host =
            PluginName = GSIFTP
            Protocol = gsiftp
            Path =
          }
          AccessProtocol.1
          {
            Host =
            PluginName = SRM2
            Protocol = srm
            Path =
          }
        }
        StorageZ
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          AccessProtocols = root
          AccessProtocols += srm
          WriteProtocols = root
          WriteProtocols += srm
          AccessProtocol.0
          {
            Host =
            PluginName = ROOT
            Protocol = root
            Path =
          }
          AccessProtocol.1
          {
            Host =
            PluginName = SRM2
            Protocol = srm
            Path =
          }
        }
      }

    }
    Operations{
      Defaults
      {
        DataManagement{
          AccessProtocols = fakeProto
          AccessProtocols += root
          WriteProtocols = srm
        }
      }
    }
    """

    with open(testCfgFileName, "w") as f:
        f.write(cfgContent)

    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()

    gConfig = ConfigurationClient(
        fileToLoadList=[testCfgFileName]
    )

    seA = StorageElementItem("StorageA")
    seA.vo = "lhcb"
    seB = StorageElementItem("StorageB")
    seB.vo = "lhcb"
    seC = StorageElementItem("StorageC")
    seC.vo = "lhcb"
    seD = StorageElementItem("StorageD")
    seD.vo = "lhcb"
    seE = StorageElementItem("StorageE")
    seE.vo = "lhcb"
    seX = StorageElementItem("StorageX")
    seX.vo = "lhcb"
    seY = StorageElementItem("StorageY")
    seY.vo = "lhcb"
    seZ = StorageElementItem("StorageZ")
    seZ.vo = "lhcb"

    yield seA, seB, seC, seD, seE, seX, seY, seZ

    os.remove(testCfgFileName)
    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()


@pytest.mark.usefixtures("setup_environment")
def test_negociateProtocolWithOtherSE(setup_environment):
    seA, seB, seC, seD, seE, seX, seY, seZ = setup_environment

    res = seA.negociateProtocolWithOtherSE(seB)
    assert res["OK"]
    assert res["Value"] == ["file"]

    res = seB.negociateProtocolWithOtherSE(seA)
    assert res["OK"]
    assert res["Value"] == ["file"]

    res = seA.negociateProtocolWithOtherSE(seC)
    assert res["OK"]
    assert res["Value"] == []

    res = seC.negociateProtocolWith
