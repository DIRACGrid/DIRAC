import os
import tempfile
import pytest


from diraccfg import CFG
import DIRAC
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import S_OK
from DIRAC.Resources.Storage.StorageFactory import StorageFactory

from DIRAC.tests.Utilities.utils import generateDIRACConfig

CFG_CONTENT = """
    DIRAC
    {
      VirtualOrganization = lhcb
    }
    Resources
    {
      StorageElementBases
      {
        CERN-BASE-WITH-TWO-SAME-PLUGINS
        {
          BackendType = Eos
          SEType = T0D1
          AccessProtocol.1
          {
            Host = srm-eoslhcb.cern.ch
            Port = 8443
            PluginName = GFAL2_SRM2
            Protocol = srm
            Access = remote
            WSUrl = /srm/v2/server?SFN:
          }
          AccessProtocol.2
          {
            Host = eoslhcb.cern.ch
            Port = 8443
            PluginName = GFAL2_SRM2
            Protocol = root
            Access = remote
            WSUrl = /srm/v2/server?SFN:
          }
        }
        CERN-ABSTRACT
        {
          BackendType = Eos
          SEType = T0D1
          AccessProtocol.1
          {
            Host = srm-eoslhcb.cern.ch
            Port = 8443
            PluginName = GFAL2_SRM2
            Protocol = srm
            Access = remote
            WSUrl = /srm/v2/server?SFN:
          }
          AccessProtocol.2
          {
            Host = eoslhcb.cern.ch
            PluginName = GFAL2_XROOT
            Protocol = root
            Access = remote
          }
        }
        CERN-BASE
        {
          BackendType = Eos
          SEType = T0D1
          AccessProtocol.1
          {
            Host = srm-eoslhcb.cern.ch
            Port = 8443
            PluginName = GFAL2_SRM2
            Protocol = srm
            Path = /eos/lhcb/grid/prod
            Access = remote
            SpaceToken = LHCb-EOS
            WSUrl = /srm/v2/server?SFN:
          }
        }
      }
      StorageElements
      {
        CERN-BASE-WRONGLOCATION
        {
          BackendType = Eos
          SEType = T0D1
          AccessProtocol.1
          {
            Host = srm-eoslhcb.cern.ch
            Port = 8443
            PluginName = GFAL2_SRM2
            Protocol = srm
            Path = /eos/lhcb/grid/prod
            Access = remote
            SpaceToken = LHCb-EOS
            WSUrl = /srm/v2/server?SFN:
          }
        }
        CERN-WRONGLOCATION
        {
          BaseSE = CERN-BASE-WRONGLOCATION
          AccessProtocol.1
          {
            PluginName = GFAL2_SRM2
            Path = /eos/lhcb/grid/prod
          }
        }
        CERN-WRONGLOCATION-ALIAS
        {
          Alias = CERN-BASE-WRONGLOCATION
        }
        CERN-SIMPLE
        {
          BackendType = Eos
          SEType = T0D1
          RemoteAccessProtocol
          {
            Host = srm-eoslhcb.cern.ch
            Port = 8443
            PluginName = GFAL2_SRM2
            Protocol = srm
            Path = /eos/lhcb/grid/prod
            Access = remote
            SpaceToken = LHCb-EOS
            WSUrl = /srm/v2/server?SFN:
          }
          LocalAccessProtocol
          {
            Host = eoslhcb.cern.ch
            PluginName = File
            Protocol = file
            Path = /eos/lhcb/grid/prod
            Access = local
            SpaceToken = LHCb-EOS
          }
        }
        CERN-USER
        {
          BaseSE = CERN-BASE
          PledgedSpace = 205
          AccessProtocol.1
          {
            PluginName = GFAL2_SRM2
            Path = /eos/lhcb/grid/user
            SpaceToken = LHCb_USER
          }
        }
        CERN-DST
        {
          BaseSE = CERN-BASE
          AccessProtocol.1
          {
            PluginName = GFAL2_SRM2
            Path = /eos/lhcb/grid/prod
          }
        }
        CERN-NO-DEF
        {
          BaseSE = CERN-BASE
        }
        CERN-NO-PLUGIN-NAME
        {
          BaseSE = CERN-BASE
          AccessProtocol.1
          {
            Path = /eos/lhcb/grid/user
          }
        }
        CERN-BAD-PLUGIN-NAME
        {
          BaseSE = CERN-BASE
          AccessProtocol.1
          {
            PluginName = AnotherPluginName
            Path = /eos/lhcb/grid/prod
            Access = local
          }
        }
        CERN-REDEFINE-PLUGIN-NAME
        {
          BaseSE = CERN-BASE
          AccessProtocol.OtherName
          {
            PluginName = GFAL2_SRM2
            Path = /eos/lhcb/grid/other
            Access = remote
          }
        }
        CERN-USE-PLUGIN-AS-PROTOCOL-NAME
        {
          BaseSE = CERN-BASE
          GFAL2_SRM2
          {
            Host = srm-eoslhcb.cern.ch
            Port = 8443
            Protocol = srm
            Path = /eos/lhcb/grid/user
            Access = remote
            SpaceToken = LHCb-EOS
            WSUrl = /srm/v2/server?SFN:
          }
        }
        CERN-USE-PLUGIN-AS-PROTOCOL-NAME-WITH-PLUGIN-NAME
        {
          BaseSE = CERN-BASE
          GFAL2_SRM2
          {
            Host = srm-eoslhcb.cern.ch
            Port = 8443
            Protocol = srm
            Path = /eos/lhcb/grid/user
            Access = remote
            SpaceToken = LHCb-EOS
            PluginName = GFAL2_XROOT
            WSUrl = /srm/v2/server?SFN:
          }
        }
        CERN-CHILD-INHERIT-FROM-BASE-WITH-TWO-SAME-PLUGINS
        {
          BaseSE = CERN-BASE-WITH-TWO-SAME-PLUGINS
          AccessProtocol.1
          {
            Path = /eos/lhcb/grid/user
          }
        }
        CERN-MORE
        {
          BaseSE = CERN-BASE
          AccessProtocol.1
          {
            PluginName = GFAL2_SRM2
            Path = /eos/lhcb/grid/user
          }
          AccessProtocol.More
          {
            Host = srm-eoslhcb.cern.ch
            Port = 8443
            PluginName = Extra
            Protocol = srm
            Path = /eos/lhcb/grid/prod
            Access = remote
            SpaceToken = LHCb-EOS
          }
        }
        CERN-CHILD
        {
          BaseSE = CERN-ABSTRACT
          AccessProtocol.1
          {
            PluginName = GFAL2_SRM2
            Path = /eos/lhcb/grid/user
            SpaceToken = LHCb_USER
          }
          AccessProtocol.2
          {
            PluginName = GFAL2_XROOT
            Path = /eos/lhcb/grid/xrootuser
          }
        }
      }
    }
"""


@pytest.fixture(scope="module", autouse=True)
def loadCS():
    """Load the CFG_CONTENT as a DIRAC Configuration for this module"""
    with generateDIRACConfig(CFG_CONTENT, "test_StorageFactory.cfg"):
        yield


def mock_StorageFactory__generateStorageObject(*args, **kwargs):
    """Don't really load the plugin, just create an object"""
    # We create this FakeStorage object because if we just
    # return a plain object, we get a lot of AttributeErrors
    # later in the test
    class FakeStorage:
        pass

    return S_OK(FakeStorage())


def mock_resourceStatus_getElementStatus(self, seName, elementType="StorageElement"):
    """We shut up RSS"""
    return S_OK({seName: {}})


@pytest.fixture(scope="function", autouse=True)
def monkeypatchForAllTest(monkeypatch):
    """ " This fixture will run for all test method and will mockup
    a few DMS methods
    """
    monkeypatch.setattr(
        DIRAC.Resources.Storage.StorageFactory.StorageFactory,
        "_StorageFactory__generateStorageObject",
        mock_StorageFactory__generateStorageObject,
    )

    monkeypatch.setattr(
        DIRAC.ResourceStatusSystem.Client.ResourceStatus.ResourceStatus,
        "getElementStatus",
        mock_resourceStatus_getElementStatus,
    )


mandatoryProtocolOptions = {
    "Access": "",
    "Host": "",
    "Path": "",
    "Port": "",
    "Protocol": "",
    "SpaceToken": "",
    "WSUrl": "",
}


def test_standalone():
    """Test loading a storage element with everything defined in itself.
    It should have two storage plugins
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-SIMPLE")

    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["LocalProtocolSections"] == ["LocalAccessProtocol"]
    assert storages["RemoteProtocolSections"] == ["RemoteAccessProtocol"]

    assert len(storages["ProtocolOptions"]) == 2
    assert len(storages["StorageObjects"]) == 2

    assert storages["StorageOptions"] == {"BackendType": "Eos", "SEType": "T0D1"}


def test_simple_inheritance_overwrite():
    """In this test, we load a storage element CERN-USER that inherits from CERN-BASE,
    add a storage option, redefine the path and the space token
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-USER")

    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1"]

    # There should be a single protocol
    assert len(storages["ProtocolOptions"]) == 1
    # There should be one storage object
    assert len(storages["StorageObjects"]) == 1

    protocolDetail = storages["ProtocolOptions"][0]
    # These are the values we expect
    assert protocolDetail["Access"] == "remote"
    assert protocolDetail["Host"] == "srm-eoslhcb.cern.ch"
    assert protocolDetail["Path"] == "/eos/lhcb/grid/user"
    assert protocolDetail["PluginName"] == "GFAL2_SRM2"
    assert protocolDetail["Port"] == "8443"
    assert protocolDetail["Protocol"] == "srm"
    assert protocolDetail["SpaceToken"] == "LHCb_USER"
    assert protocolDetail["WSUrl"] == "/srm/v2/server?SFN:"

    assert storages["StorageOptions"] == {
        "BackendType": "Eos",
        "SEType": "T0D1",
        "PledgedSpace": "205",
        "BaseSE": "CERN-BASE",
    }


def test_simple_inheritance():
    """In this test, we load a storage element CERN-DST that inherits from CERN-BASE,
    and redefine the same value for Path and PluginName
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-DST")

    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1"]

    # There should be a single protocol
    assert len(storages["ProtocolOptions"]) == 1
    # There should be one storage object
    assert len(storages["StorageObjects"]) == 1

    protocolDetail = storages["ProtocolOptions"][0]
    # These are the values we expect
    assert protocolDetail["Access"] == "remote"
    assert protocolDetail["Host"] == "srm-eoslhcb.cern.ch"
    assert protocolDetail["Path"] == "/eos/lhcb/grid/prod"
    assert protocolDetail["PluginName"] == "GFAL2_SRM2"
    assert protocolDetail["Port"] == "8443"
    assert protocolDetail["Protocol"] == "srm"
    assert protocolDetail["SpaceToken"] == "LHCb-EOS"
    assert protocolDetail["WSUrl"] == "/srm/v2/server?SFN:"

    assert storages["StorageOptions"] == {"BackendType": "Eos", "SEType": "T0D1", "BaseSE": "CERN-BASE"}


def test_pure_inheritance():
    """In this test, we load a storage element CERN-NO-DEF that inherits from CERN-BASE,
    but does not redefine ANYTHING. We expect it to be just like the parent
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-NO-DEF")

    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1"]

    # There should be a single protocol
    assert len(storages["ProtocolOptions"]) == 1
    # There should be one storage object
    assert len(storages["StorageObjects"]) == 1

    protocolDetail = storages["ProtocolOptions"][0]
    # These are the values we expect
    assert protocolDetail["Access"] == "remote"
    assert protocolDetail["Host"] == "srm-eoslhcb.cern.ch"
    assert protocolDetail["Path"] == "/eos/lhcb/grid/prod"
    assert protocolDetail["PluginName"] == "GFAL2_SRM2"
    assert protocolDetail["Port"] == "8443"
    assert protocolDetail["Protocol"] == "srm"
    assert protocolDetail["SpaceToken"] == "LHCb-EOS"
    assert protocolDetail["WSUrl"] == "/srm/v2/server?SFN:"

    assert storages["StorageOptions"] == {"BackendType": "Eos", "SEType": "T0D1", "BaseSE": "CERN-BASE"}


def test_no_plugin_name():
    """In this test, we load a storage element CERN-NO-PLUGIN-NAME that inherits from CERN-BASE,
    and redefine the same protocol but with no PluginName
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-NO-PLUGIN-NAME")

    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1"]

    expectedProtocols = [
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/user",
            "PluginName": "GFAL2_SRM2",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "/srm/v2/server?SFN:",
        }
    ]

    assert storages["ProtocolOptions"] == expectedProtocols


def test_bad_plugin_name():
    """In this test, we load a storage element CERN-BAD-PLUGIN-NAME that inherits from CERN-BASE,
    and redefine the same protocol but with a different PluginName.
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-BAD-PLUGIN-NAME")
    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == []
    assert storages["LocalProtocolSections"] == ["AccessProtocol.1"]

    expectedProtocols = [
        {
            "Access": "local",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/prod",
            "PluginName": "AnotherPluginName",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "/srm/v2/server?SFN:",
        }
    ]

    assert storages["ProtocolOptions"] == expectedProtocols


def test_redefine_plugin_name():
    """In this test, we load a storage element CERN-REDEFINE-PLUGIN-NAME that inherits from CERN-BASE,
    and uses the same Plugin with a different section.
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-REDEFINE-PLUGIN-NAME")
    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1", "AccessProtocol.OtherName"]

    expectedProtocols = [
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/prod",
            "PluginName": "GFAL2_SRM2",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "/srm/v2/server?SFN:",
        },
        {
            "Access": "remote",
            "Host": "",
            "Path": "/eos/lhcb/grid/other",
            "PluginName": "GFAL2_SRM2",
            "Port": "",
            "Protocol": "",
            "SpaceToken": "",
            "WSUrl": "",
        },
    ]

    assert storages["ProtocolOptions"] == expectedProtocols


def test_use_plugin_as_protocol_name():
    """In this test, we load a storage element CERN-USE-PLUGIN-AS-PROTOCOL-NAME that inherits from CERN-BASE,
    and uses a protocol named as a plugin name, the plugin name is not present.
    """
    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-USE-PLUGIN-AS-PROTOCOL-NAME")
    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1", "GFAL2_SRM2"]

    expectedProtocols = [
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/prod",
            "PluginName": "GFAL2_SRM2",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "/srm/v2/server?SFN:",
        },
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/user",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "/srm/v2/server?SFN:",
        },
    ]

    assert storages["ProtocolOptions"] == expectedProtocols


def test_use_plugin_as_protocol_name_with_plugin_name():
    """In this test, we load a storage element CERN-USE-PLUGIN-AS-PROTOCOL-NAME that inherits from CERN-BASE,
    and uses a protocol named as a plugin name, the plugin name is also present.
    """
    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-USE-PLUGIN-AS-PROTOCOL-NAME-WITH-PLUGIN-NAME")
    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1", "GFAL2_SRM2"]

    expectedProtocols = [
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/prod",
            "PluginName": "GFAL2_SRM2",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "/srm/v2/server?SFN:",
        },
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/user",
            "PluginName": "GFAL2_XROOT",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "/srm/v2/server?SFN:",
        },
    ]

    assert storages["ProtocolOptions"] == expectedProtocols


def test_more_protocol():
    """In this test, we load a storage element CERN-MORE that inherits from CERN-BASE,
    and adds an extra protocol
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-MORE")
    assert storages["OK"], storages
    storages = storages["Value"]

    assert set(storages["RemoteProtocolSections"]) == {"AccessProtocol.1", "AccessProtocol.More"}

    expectedProtocols = [
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/prod",
            "PluginName": "Extra",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "",
        },
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/user",
            "PluginName": "GFAL2_SRM2",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb-EOS",
            "WSUrl": "/srm/v2/server?SFN:",
        },
    ]

    assert sorted(storages["ProtocolOptions"], key=lambda x: x["PluginName"]) == expectedProtocols


def test_child_inherit_from_base_with_two_same_plugins():
    """In this test, we load a storage element CERN-CHILD-INHERIT-FROM-BASE-WITH-TWO-SAME-PLUGINS that inherits
    from CERN-BASE-WITH-TWO-SAME-PLUGINS, using two identical plugin names in two sections.
    """
    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-CHILD-INHERIT-FROM-BASE-WITH-TWO-SAME-PLUGINS")
    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1", "AccessProtocol.2"]

    expectedProtocols = [
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/user",
            "PluginName": "GFAL2_SRM2",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "",
            "WSUrl": "/srm/v2/server?SFN:",
        },
        {
            "Access": "remote",
            "Host": "eoslhcb.cern.ch",
            "Path": "",
            "PluginName": "GFAL2_SRM2",
            "Port": "8443",
            "Protocol": "root",
            "SpaceToken": "",
            "WSUrl": "/srm/v2/server?SFN:",
        },
    ]

    assert storages["ProtocolOptions"] == expectedProtocols


def test_baseSE_in_SEDefinition():
    """In this test, a storage inherits from a baseSE which is declared in the
    StorageElements section instead of the BaseStorageElements section.
    It used to be possible, but we remove this compatibility layer.
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-WRONGLOCATION")

    assert not storages["OK"], storages


def test_aliasSE_in_SEDefinition():
    """In this test, a storage aliases a baseSE which is declared in the
    StorageElements section. That should remain possible
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-WRONGLOCATION-ALIAS")

    assert storages["OK"], storages


def test_pure_abstract():
    """In this test, we load a storage element CERN-CHILD that inherits from CERN-ABSTRACT.
    CERN-ABSTRACT has two uncomplete protocols, and CERN-CHILD defines them
    """

    sf = StorageFactory(vo="lhcb")
    storages = sf.getStorages("CERN-CHILD")
    assert storages["OK"], storages
    storages = storages["Value"]

    assert storages["RemoteProtocolSections"] == ["AccessProtocol.1", "AccessProtocol.2"]

    expectedProtocols = [
        {
            "Access": "remote",
            "Host": "srm-eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/user",
            "PluginName": "GFAL2_SRM2",
            "Port": "8443",
            "Protocol": "srm",
            "SpaceToken": "LHCb_USER",
            "WSUrl": "/srm/v2/server?SFN:",
        },
        {
            "Access": "remote",
            "Host": "eoslhcb.cern.ch",
            "Path": "/eos/lhcb/grid/xrootuser",
            "PluginName": "GFAL2_XROOT",
            "Port": "",
            "Protocol": "root",
            "SpaceToken": "",
            "WSUrl": "",
        },
    ]

    assert storages["ProtocolOptions"] == expectedProtocols


def test_getStorages_protocolSections():
    """The idea is to test getStorages with different combinations of
    requested protocolSections"""

    sf = StorageFactory(vo="lhcb")
    seName = "CERN-CHILD-INHERIT-FROM-BASE-WITH-TWO-SAME-PLUGINS"
    res = sf.getStorages(seName)
    assert res
    allStorages = res["Value"]
    remoteProtocolSections = allStorages["RemoteProtocolSections"] + allStorages["LocalProtocolSections"]

    res = sf.getStorages(seName, protocolSections=["AccessProtocol.1", "AccessProtocol.2"])
    assert res["OK"]
    specificStorages = res["Value"]
