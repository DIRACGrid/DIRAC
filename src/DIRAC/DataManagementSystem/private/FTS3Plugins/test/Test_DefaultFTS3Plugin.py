""" This modules contains a lot of tests for multiHop. It can be used as a configuration reference"""

import copy
import os
import tempfile
from unittest import mock

import pytest
from diraccfg import CFG

import DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient
from DIRAC.DataManagementSystem.private.FTS3Plugins.DefaultFTS3Plugin import DefaultFTS3Plugin
from DIRAC.Resources.Storage.StorageBase import StorageBase

# pylint: disable=redefined-outer-name


# Used to mock StoragePlugins instantiation
def mock_StorageFactory_generateStorageObject(
    self, storageName, pluginName, parameters, hideExceptions=False
):  # pylint: disable=unused-argument
    """Generate fake storage object"""
    storageObj = StorageBase(storageName, parameters)
    storageObj.pluginName = pluginName

    return S_OK(storageObj)


# Describe by hand the full matrix as we expect it to be from
# the config
# This needs to be updated if the config is updated
FULL_MATRIX = {
    "CERN-DST": {
        "CERN-DST": "GlobalDefault",
        "CERN-RAW": "GlobalDefault",
        "RAL-DST": "GlobalDefault",
        "RAL-RAW": "GlobalDefault",
        "CNAF-DST": "DefaultToCNAF-Disk",
        "CNAF_MC-DST": "DefaultToCNAF-Disk",
        "IN2P3-DST": "DefaultToIN2P3-DST",
    },
    "CERN-RAW": {
        "CERN-DST": None,
        "CERN-RAW": "DefaultFromCERN-RAW",
        "RAL-DST": "DefaultFromCERN-RAW",
        "RAL-RAW": "DefaultFromCERN-RAW",
        "CNAF-DST": "CERN-RAW-CNAF-DST",
        "CNAF_MC-DST": "CERN-RAW-CNAF-Disk",
        "IN2P3-DST": None,
    },
    "RAL-DST": {
        "CERN-DST": "GlobalDefault",
        "CERN-RAW": "GlobalDefault",
        "RAL-DST": "GlobalDefault",
        "RAL-RAW": "GlobalDefault",
        "CNAF-DST": "DefaultToCNAF-Disk",
        "CNAF_MC-DST": "DefaultToCNAF-Disk",
        "IN2P3-DST": "DefaultToIN2P3-DST",
    },
    "RAL-RAW": {
        "CERN-DST": None,
        "CERN-RAW": None,
        "RAL-DST": None,
        "RAL-RAW": None,
        "CNAF-DST": "RAL-Tape-CNAF-DST",
        "CNAF_MC-DST": "RAL-Tape-CNAF-Disk",
        "IN2P3-DST": None,
    },
    "CNAF-DST": {
        "CERN-DST": "GlobalDefault",
        "CERN-RAW": "GlobalDefault",
        "RAL-DST": "GlobalDefault",
        "RAL-RAW": "GlobalDefault",
        "CNAF-DST": "DefaultToCNAF-Disk",
        "CNAF_MC-DST": "DefaultToCNAF-Disk",
        "IN2P3-DST": "DefaultToIN2P3-DST",
    },
    "CNAF_MC-DST": {
        "CERN-DST": "GlobalDefault",
        "CERN-RAW": "GlobalDefault",
        "RAL-DST": "GlobalDefault",
        "RAL-RAW": "GlobalDefault",
        "CNAF-DST": "DefaultToCNAF-Disk",
        "CNAF_MC-DST": "DefaultToCNAF-Disk",
        "IN2P3-DST": "DefaultToIN2P3-DST",
    },
    "IN2P3-DST": {
        "CERN-DST": "GlobalDefault",
        "CERN-RAW": "GlobalDefault",
        "RAL-DST": "GlobalDefault",
        "RAL-RAW": "GlobalDefault",
        "CNAF-DST": "IN2P3-DST-CNAF-DST",
        "CNAF_MC-DST": "DefaultToCNAF-Disk",
        "IN2P3-DST": "DefaultToIN2P3-DST",
    },
}


@pytest.fixture(scope="module", autouse=True)
def generateConfig():
    """
    This generates the test configuration once for the module, and removes it when done
    """

    # Clean first the config from potential other leaking tests
    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()

    testCfgFileName = os.path.join(tempfile.gettempdir(), "test_FTS3Plugin.cfg")
    cfgContent = """
    Resources
    {
      StorageElementBases
      {
        CERN-Disk
        {
          BackendType =  EOS
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
          }
        }
        CERN-Tape
        {
          BackendType =  CTA
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
          }
        }
        RAL-Disk
        {
          BackendType =  Echo
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
          }
        }
        RAL-Tape
        {
          BackendType =  Castor
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
          }
        }
        CNAF-Disk
        {
          BackendType =  EOS
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
          }
        }
        CNAF-Tape
        {
          BackendType =  CTA
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
        }
        }
        IN2P3-Disk
        {
          BackendType =  EOS
          AccessProtocol.0
          {
            Host =
            PluginName = File
            Protocol = file
            Path =
          }
        }
      }
      StorageElements
      {
        CERN-DST
        {
          BaseSE = CERN-Disk
        }
        CERN-RAW
        {
          BaseSE = CERN-Tape
        }
        RAL-DST
        {
          BaseSE = RAL-Disk
        }
        RAL-RAW
        {
          BaseSE = RAL-Tape
        }
        CNAF-DST
        {
          BaseSE = CNAF-Disk
        }
        CNAF_MC-DST
        {
          BaseSE = CNAF-Disk
        }
        IN2P3-DST
        {
          BaseSE = IN2P3-Disk
        }
      }

    }
    Operations{
      Defaults
      {
        DataManagement
        {
          MultiHopMatrixOfShame
          {
            # Used for any source which does not have a more specific rule
            Default
            {
              # Default -> Default basically means "anything else than all the other defined routes"
              Default = GlobalDefault
              # Hop between "anything else" and IN3P3-DST
              IN2P3-DST = DefaultToIN2P3-DST
              # Hop between "anything else" and any SE inheriting from CNAF-Disk
              CNAF-Disk = DefaultToCNAF-Disk
            }
            # Any transfer starting from CERN-RAW
            CERN-RAW
            {
              # CERN-RAW -> anywhere else
              Default = DefaultFromCERN-RAW
              # Do not use multihop between CERN-RAW and SE inheriting from CERN-Disk
              CERN-Disk = disabled
              # CERN-RAW -> any SE inheriting from CNAF-Disk
              CNAF-Disk = CERN-RAW-CNAF-Disk
              # CERN-RAW->CNAF-DST (takes precedence over CERN-RAW -> CNAF-Disk)
              CNAF-DST = CERN-RAW-CNAF-DST
              # CERN-RAW -> IN2P3-DST
              IN2P3-DST = disabled
            }
            # Any transfer starting from any SE inheriting from RAL-Tape
            RAL-Tape
            {
              # RAL-Tape -> anywhere else: do not use multihop
              Default = disabled
              # any SE inheriting from RAL-Tape -> any SE inheriting from CNAF-Disk
              CNAF-Disk = RAL-Tape-CNAF-Disk
              # any SE inheriting from RAL-Tape -> CNAF-DST (takes precedence over the previous rule)
              CNAF-DST = RAL-Tape-CNAF-DST
            }
            # Any transfer starting from IN2P3-DST
            IN2P3-DST
            {
              # IN2P2-DST -> CNAF-DST
              CNAF-DST = IN2P3-DST-CNAF-DST
            }
          }
        }
      }
    }
  """
    with open(testCfgFileName, "w") as f:
        f.write(cfgContent)
    # Load the configuration
    ConfigurationClient(fileToLoadList=[testCfgFileName])  # we replace the configuration by our own one.
    yield

    try:
        os.remove(testCfgFileName)
    except OSError:
        pass
    # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
    # not to conflict with other tests that might be using a local dirac.cfg
    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()


@pytest.fixture(scope="function")
def fts3Plugin(monkeypatch):
    """
    Generate a DefaultFTS3Plugin object, and mock a few of the internals of the StorageElement object
    to make the instantiation easy
    """

    monkeypatch.setattr(
        DIRAC.Resources.Storage.StorageFactory.StorageFactory,
        "_StorageFactory__generateStorageObject",
        mock_StorageFactory_generateStorageObject,
    )

    monkeypatch.setattr(
        DIRAC.Resources.Storage.StorageElement.StorageElementItem, "_StorageElementItem__isLocalSE", lambda: S_OK(True)
    )

    monkeypatch.setattr(
        DIRAC.Resources.Storage.StorageElement.StorageElementItem, "addAccountingOperation", lambda: None
    )

    def mock_init(self, useProxy=False, vo=None):
        self.proxy = False
        self.proxy = useProxy
        self.resourceStatus = mock.MagicMock()
        self.vo = vo
        self.remoteProtocolSections = []
        self.localProtocolSections = []
        self.name = ""
        self.options = {}
        self.protocols = {}
        self.storages = {}

    monkeypatch.setattr(DIRAC.Resources.Storage.StorageFactory.StorageFactory, "__init__", mock_init)

    fts3Plugin = DefaultFTS3Plugin()

    return fts3Plugin


def test_multiHop_specificLink(fts3Plugin):
    """When we have a configuration for the exact match (src,dst)"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("CERN-RAW", "CNAF-DST")
    assert hopName == "CERN-RAW-CNAF-DST"


def test_multiHop_specificSrc_baseSEDst(fts3Plugin):
    """When we have a specific source and a baseSE dest"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("CERN-RAW", "CNAF_MC-DST")
    assert hopName == "CERN-RAW-CNAF-Disk"


def test_multiHop_specificSrc_disableBaseSE(fts3Plugin):
    """When we disable it for a given baseSE dest"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("CERN-RAW", "CERN-DST")
    assert not hopName


def test_multiHop_specificSrc_disableDest(fts3Plugin):
    """When we disable it for a given baseSE dest"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("CERN-RAW", "IN2P3-DST")
    assert not hopName


def test_multiHop_specificSrc_defaultDst(fts3Plugin):
    """When we have the src defined, the dst is not, but we have a default"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("CERN-RAW", "RAL-DST")
    assert hopName == "DefaultFromCERN-RAW"


def test_multiHop_baseSESrc_specificDst(fts3Plugin):
    """When we have a baseSE source and a specific dest"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("RAL-RAW", "CNAF-DST")
    assert hopName == "RAL-Tape-CNAF-DST"


def test_multiHop_baseSESrc_baseSEDst(fts3Plugin):
    """When we have the both source and dest as BaseSE"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("RAL-RAW", "CNAF_MC-DST")
    assert hopName == "RAL-Tape-CNAF-Disk"


def test_multiHop_baseSESrc_disabledDefault(fts3Plugin):
    """When we have the src defined, the dst is not, and the default is disabled"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("RAL-RAW", "IN2P3-DST")
    assert not hopName


def test_multiHop_globalDefault(fts3Plugin):
    """When we have nothing matching, but a global default"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("CNAF-DST", "CERN-DST")
    assert hopName == "GlobalDefault"


def test_multiHop_srcSE_noLocalDefault(fts3Plugin):
    """When we have the source matching, but no local default, we should fall on the global default"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("IN2P3-DST", "CNAF-DST")
    assert hopName == "IN2P3-DST-CNAF-DST"
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("IN2P3-DST", "RAL-DST")
    assert hopName == "GlobalDefault"


def test_multiHop_DefaultSrc_specificDst(fts3Plugin):
    """When we have a rul for specific dest but all source"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("CNAF-DST", "IN2P3-DST")
    assert hopName == "DefaultToIN2P3-DST"


def test_multiHop_DefaultSrc_baseSEDst(fts3Plugin):
    """When we have a rule for specific dest but all source"""
    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure("IN2P3-DST", "CNAF_MC-DST")
    assert hopName == "DefaultToCNAF-Disk"


def test_full_matrix_isComplete():
    """Make sure that the full matrix written by hand
    has all the sources and destinations"""

    allSEs = sorted(DIRAC.gConfig.getSections("/Resources/StorageElements")["Value"])
    assert sorted(FULL_MATRIX) == allSEs
    for dsts in FULL_MATRIX.values():
        assert sorted(dsts) == allSEs


@pytest.mark.parametrize("dst", FULL_MATRIX)
@pytest.mark.parametrize("src", FULL_MATRIX)
def test_full_matrix(fts3Plugin, src, dst):
    """This is sort of redundant with all the previous test,
    but here we cover the hole matrix"""

    hopName = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure(src, dst)
    assert hopName == FULL_MATRIX[src][dst]


def test_copyReturnsSameInstance(fts3Plugin):
    """The plugin is shared, so copies should return the same instance"""
    assert copy.copy(fts3Plugin) is fts3Plugin
    assert copy.deepcopy(fts3Plugin) is fts3Plugin


FAKE_FTS3_SERVERS = {
    "server_0": "https://server0.cern.ch:8446",
    "server_1": "https://server1.cern.ch:8446",
}


@pytest.fixture(scope="function")
def mockFTSServers(monkeypatch):
    """Mock the list of FTS3 servers, and the RSS status of the servers.

    Returns the dict of server status, which can be altered by the test
    """
    serverStatus = dict.fromkeys(FAKE_FTS3_SERVERS, True)

    monkeypatch.setattr(
        DIRAC.DataManagementSystem.private.FTS3Plugins.DefaultFTS3Plugin,
        "getFTS3ServerDict",
        lambda: S_OK(FAKE_FTS3_SERVERS),
    )
    monkeypatch.setattr(
        DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy,
        "_getFTSServerStatus",
        lambda self, ftsServer: S_OK(serverStatus[ftsServer]),
    )
    return serverStatus


def test_selectFTS3Server(fts3Plugin, mockFTSServers):
    """The selected server is one of the configured ones"""
    assert fts3Plugin.selectFTS3Server() in FAKE_FTS3_SERVERS.values()


def test_selectFTS3Server_policyFromVO(fts3Plugin, mockFTSServers, monkeypatch):
    """The ServerPolicy is taken from the Operations of the plugin VO"""
    fts3Plugin.vo = "myVO"
    opsMock = mock.MagicMock()
    opsMock.return_value.getValue.return_value = "Sequence"
    monkeypatch.setattr(DIRAC.DataManagementSystem.private.FTS3Plugins.DefaultFTS3Plugin, "Operations", opsMock)

    # Sequence policy: servers are taken in turn
    assert fts3Plugin.selectFTS3Server() == FAKE_FTS3_SERVERS["server_0"]
    assert fts3Plugin.selectFTS3Server() == FAKE_FTS3_SERVERS["server_1"]
    assert fts3Plugin.selectFTS3Server() == FAKE_FTS3_SERVERS["server_0"]

    opsMock.assert_called_once_with(vo="myVO")
    opsMock.return_value.getValue.assert_called_once_with("DataManagement/FTSPlacement/FTS3/ServerPolicy", "Random")


def test_selectFTS3Server_skipInactive(fts3Plugin, mockFTSServers):
    """Servers that are not active are not selected"""
    mockFTSServers["server_0"] = False
    for _ in range(10):
        assert fts3Plugin.selectFTS3Server() == FAKE_FTS3_SERVERS["server_1"]


def test_selectFTS3Server_noActiveServer(fts3Plugin, mockFTSServers):
    """ValueError is raised if no server is active"""
    for server in mockFTSServers:
        mockFTSServers[server] = False
    with pytest.raises(ValueError):
        fts3Plugin.selectFTS3Server()


def test_selectFTS3Server_noServerDict(fts3Plugin, monkeypatch):
    """ValueError is raised if the servers cannot be obtained"""
    monkeypatch.setattr(
        DIRAC.DataManagementSystem.private.FTS3Plugins.DefaultFTS3Plugin,
        "getFTS3ServerDict",
        lambda: S_ERROR("No FTS3 endpoints"),
    )
    with pytest.raises(ValueError):
        fts3Plugin.selectFTS3Server()
