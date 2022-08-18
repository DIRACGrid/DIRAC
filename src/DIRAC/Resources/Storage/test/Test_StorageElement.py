""" test StoragElement
"""
import os
import tempfile
from unittest import mock
import unittest
import itertools

from diraccfg import CFG

from DIRAC import S_OK
from DIRAC.Resources.Storage.StorageElement import StorageElementItem
from DIRAC.Resources.Storage.StorageBase import StorageBase


from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient


class fake_SRM2Plugin(StorageBase):
    """Fake SRM2 plugin.
    Only implements the two methods needed
    for transfer, so we can test that it is really this plugin
    that returned
    """

    def putFile(self, lfns, sourceSize=0):
        return S_OK({"Successful": dict.fromkeys(lfns, "srm:putFile"), "Failed": {}})

    def getTransportURL(self, path, protocols=False):
        return S_OK({"Successful": dict.fromkeys(path, "srm:getTransportURL"), "Failed": {}})


class fake_XROOTPlugin(StorageBase):
    """Fake XROOT plugin.
    Only implements the two methods needed
    for transfer, so we can test that it is really this plugin
    that returned
    """

    def putFile(self, lfns, sourceSize=0):
        return S_OK({"Successful": dict.fromkeys(lfns, "root:putFile"), "Failed": {}})

    def getTransportURL(self, path, protocols=False):
        return S_OK({"Successful": dict.fromkeys(path, "root:getTransportURL"), "Failed": {}})


class fake_GSIFTPPlugin(StorageBase):
    """Fake GSIFTP plugin.
    Only implements the two methods needed
    for transfer, so we can test that it is really this plugin
    that returned
    """

    def putFile(self, lfns, sourceSize=0):
        return S_OK({"Successful": dict.fromkeys(lfns, "gsiftp:putFile"), "Failed": {}})

    def getTransportURL(self, path, protocols=False):
        return S_OK({"Successful": dict.fromkeys(path, "gsiftp:getTransportURL"), "Failed": {}})


def mock_StorageFactory_generateStorageObject(storageName, pluginName, parameters, hideExceptions=False):
    """Generate fake storage object"""
    storageObj = StorageBase(storageName, parameters)

    if pluginName == "SRM2":
        storageObj = fake_SRM2Plugin(storageName, parameters)
        storageObj.protocolParameters["InputProtocols"] = ["file", "root", "srm"]
        storageObj.protocolParameters["OutputProtocols"] = ["file", "root", "dcap", "gsidcap", "rfio", "srm"]
    elif pluginName == "File":
        # Not needed to do anything, StorageBase should do it :)
        pass
    elif pluginName == "XROOT":
        storageObj = fake_XROOTPlugin(storageName, parameters)
        storageObj.protocolParameters["InputProtocols"] = ["file", "root"]
        storageObj.protocolParameters["OutputProtocols"] = ["root"]
    elif pluginName == "GSIFTP":
        storageObj = fake_GSIFTPPlugin(storageName, parameters)
        storageObj.protocolParameters["InputProtocols"] = ["file", "gsiftp"]
        storageObj.protocolParameters["OutputProtocols"] = ["gsiftp"]

    storageObj.pluginName = pluginName

    return S_OK(storageObj)


class TestBase(unittest.TestCase):
    """Base test class. Defines all the method to test"""

    @mock.patch(
        "DIRAC.Resources.Storage.StorageFactory.StorageFactory._StorageFactory__generateStorageObject",
        side_effect=mock_StorageFactory_generateStorageObject,
    )
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def setUp(self, _mk_generateStorage, _mk_isLocalSE, _mk_addAccountingOperation):

        # Creating test configuration file
        self.testCfgFileName = os.path.join(tempfile.gettempdir(), "test_StorageElement.cfg")
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

        with open(self.testCfgFileName, "w") as f:
            f.write(cfgContent)

        # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
        # not to conflict with other tests that might be using a local dirac.cfg
        gConfigurationData.localCFG = CFG()
        gConfigurationData.remoteCFG = CFG()
        gConfigurationData.mergedCFG = CFG()
        gConfigurationData.generateNewVersion()

        gConfig = ConfigurationClient(
            fileToLoadList=[self.testCfgFileName]
        )  # we replace the configuration by our own one.

        self.seA = StorageElementItem("StorageA")
        self.seA.vo = "lhcb"
        self.seB = StorageElementItem("StorageB")
        self.seB.vo = "lhcb"
        self.seC = StorageElementItem("StorageC")
        self.seC.vo = "lhcb"
        self.seD = StorageElementItem("StorageD")
        self.seD.vo = "lhcb"
        self.seE = StorageElementItem("StorageE")
        self.seE.vo = "lhcb"

        self.seX = StorageElementItem("StorageX")
        self.seX.vo = "lhcb"
        self.seY = StorageElementItem("StorageY")
        self.seY.vo = "lhcb"
        self.seZ = StorageElementItem("StorageZ")
        self.seZ.vo = "lhcb"

    def tearDown(self):
        try:
            os.remove(self.testCfgFileName)
        except OSError:
            pass
        # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
        # not to conflict with other tests that might be using a local dirac.cfg
        gConfigurationData.localCFG = CFG()
        gConfigurationData.remoteCFG = CFG()
        gConfigurationData.mergedCFG = CFG()
        gConfigurationData.generateNewVersion()

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_01_negociateProtocolWithOtherSE(self, mk_isLocalSE, mk_addAccounting):
        """Testing negotiation algorithm"""

        # Find common protocol between SRM2 and File
        res = self.seA.negociateProtocolWithOtherSE(self.seB)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], ["file"])

        # Find common protocol between File and SRM@
        res = self.seB.negociateProtocolWithOtherSE(self.seA)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], ["file"])

        # Find common protocol between XROOT and File
        # Nothing goes from xroot to file
        res = self.seA.negociateProtocolWithOtherSE(self.seC)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], [])

        # Find common protocol between File and XROOT
        res = self.seC.negociateProtocolWithOtherSE(self.seA)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], ["file"])

        # Find common protocol between File and File
        res = self.seA.negociateProtocolWithOtherSE(self.seA)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], ["file"])

        # Find common protocol between SRM and SRM
        res = self.seB.negociateProtocolWithOtherSE(self.seB)
        self.assertTrue(res["OK"], res)
        self.assertEqual(sorted(res["Value"]), sorted(["file", "root", "srm"]))

        # Find common protocol between SRM and XROOT
        res = self.seC.negociateProtocolWithOtherSE(self.seB)
        self.assertTrue(res["OK"], res)
        self.assertEqual(sorted(res["Value"]), sorted(["root", "file"]))

        # Find common protocol between XROOT and SRM
        res = self.seC.negociateProtocolWithOtherSE(self.seB)
        self.assertTrue(res["OK"], res)
        self.assertEqual(sorted(res["Value"]), sorted(["root", "file"]))

        # Testing restrictions
        res = self.seC.negociateProtocolWithOtherSE(self.seB, protocols=["file"])
        self.assertTrue(res["OK"], res)
        self.assertEqual(sorted(res["Value"]), ["file"])

        res = self.seC.negociateProtocolWithOtherSE(self.seB, protocols=["nonexisting"])
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], [])

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_02_followOrder(self, _mk_isLocalSE, _mk_addAccounting):
        """Testing If the order of preferred protocols is respected"""

        for permutation in itertools.permutations(["srm", "file", "root", "nonexisting"]):
            permuList = list(permutation)
            # Don't get tricked ! remove cannot be put
            # after the conversion, because it is inplace modification
            permuList.remove("nonexisting")
            res = self.seD.negociateProtocolWithOtherSE(self.seD, protocols=permutation)
            self.assertTrue(res["OK"], res)
            self.assertEqual(res["Value"], permuList)

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_03_multiProtocolThirdParty(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test case for storages with several protocols

        Here comes the fun :-)
        Suppose we have endpoints that we can read in root, but cannot write
        If we have root in the accessProtocols and thirdPartyProtocols lists
        but not in the writeProtocols, we should get a root url to read,
        and write with SRM

        We reproduce here the behavior of DataManager.replicate

        """

        thirdPartyProtocols = ["root", "srm"]

        lfn = "/lhcb/fake/lfn"
        res = self.seD.negociateProtocolWithOtherSE(self.seD, protocols=thirdPartyProtocols)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], thirdPartyProtocols)

        # Only the XROOT plugin here implements the geTransportURL
        # that returns what we want, so we know that
        # if the return is successful, it is because of the XROOT
        res = self.seD.getURL(lfn, protocol=res["Value"])
        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)

        srcUrl = res["Value"]["Successful"][lfn]
        self.assertEqual(srcUrl, "root:getTransportURL")

        # Only the SRM2 plugin here implements the putFile method
        # so if we get a success here, it means that we used the SRM plugin
        res = self.seD.replicateFile({lfn: srcUrl}, sourceSize=123, inputProtocol="root")

        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)
        self.assertEqual(res["Value"]["Successful"][lfn], "srm:putFile")

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_04_thirdPartyLocalWrite(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test case for storages with several protocols

        Here, we locally define the write protocol to be root and srm
        So we should be able to do everything with XROOT plugin

        """

        thirdPartyProtocols = ["root", "srm"]

        lfn = "/lhcb/fake/lfn"
        res = self.seE.negociateProtocolWithOtherSE(self.seE, protocols=thirdPartyProtocols)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], thirdPartyProtocols)

        res = self.seE.getURL(lfn, protocol=res["Value"])
        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)

        srcUrl = res["Value"]["Successful"][lfn]
        self.assertEqual(srcUrl, "root:getTransportURL")

        res = self.seE.replicateFile({lfn: srcUrl}, sourceSize=123, inputProtocol="root")

        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)
        self.assertEqual(res["Value"]["Successful"][lfn], "root:putFile")

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_05_thirdPartyMix(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test case for storages with several protocols

        Here, we locally define the write protocol for the destination, so it should
        all go directly through the XROOT plugin

        """

        thirdPartyProtocols = ["root", "srm"]

        lfn = "/lhcb/fake/lfn"
        res = self.seE.negociateProtocolWithOtherSE(self.seD, protocols=thirdPartyProtocols)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], thirdPartyProtocols)

        res = self.seD.getURL(lfn, protocol=res["Value"])
        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)

        srcUrl = res["Value"]["Successful"][lfn]
        self.assertEqual(srcUrl, "root:getTransportURL")

        res = self.seE.replicateFile({lfn: srcUrl}, sourceSize=123, inputProtocol="root")

        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)
        self.assertEqual(res["Value"]["Successful"][lfn], "root:putFile")

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_06_thirdPartyMixOpposite(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test case for storages with several protocols

        Here, we locally define the write protocol for the source, so it should
        get the source directly using XROOT, and perform the put using SRM

        """

        thirdPartyProtocols = ["root", "srm"]

        lfn = "/lhcb/fake/lfn"
        res = self.seD.negociateProtocolWithOtherSE(self.seE, protocols=thirdPartyProtocols)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], thirdPartyProtocols)

        res = self.seE.getURL(lfn, protocol=res["Value"])
        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)

        srcUrl = res["Value"]["Successful"][lfn]
        self.assertEqual(srcUrl, "root:getTransportURL")

        res = self.seD.replicateFile({lfn: srcUrl}, sourceSize=123, inputProtocol="root")

        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)
        self.assertEqual(res["Value"]["Successful"][lfn], "srm:putFile")

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_07_multiProtocolSrmOnly(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test case for storages with several protocols

        Here comes the fun :-)
        Suppose we have endpoints that we can read in root, but cannot write
        If we have root in the accessProtocols and thirdPartyProtocols lists
        but not in the writeProtocols, we should get a root url to read,
        and write with SRM

        We reproduce here the behavior of DataManager.replicate

        """

        thirdPartyProtocols = ["srm"]

        lfn = "/lhcb/fake/lfn"
        res = self.seD.negociateProtocolWithOtherSE(self.seD, protocols=thirdPartyProtocols)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"], thirdPartyProtocols)

        res = self.seD.getURL(lfn, protocol=res["Value"])
        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)

        srcUrl = res["Value"]["Successful"][lfn]
        self.assertEqual(srcUrl, "srm:getTransportURL")

        res = self.seD.replicateFile({lfn: srcUrl}, sourceSize=123, inputProtocol="srm")

        self.assertTrue(res["OK"], res)
        self.assertTrue(lfn in res["Value"]["Successful"], res)
        self.assertEqual(res["Value"]["Successful"][lfn], "srm:putFile")

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_08_multiProtocolFTS(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test case FTS replication between storages with several protocols

        Here comes the fun :-)
        Suppose we have endpoints that we can read in root, but cannot write
        If we have root in the accessProtocols and thirdPartyProtocols lists
        but not in the writeProtocols, we should get a root url to read,
        and write with SRM.
        And We should get the proper url for source and destination

        Storage X, Y and Z represents the situation we could now have in LHCb:
          * X is RAL Echo: you read with root, write with gsiftp
          * Y is Gridka: you have gsiftp available for read only
          * Z is CERN EOS: you can do everything with EOS

        This makes it necessary to add gsiftp as third party option to write to ECHO

        """
        thirdPartyProtocols = ["root", "gsiftp", "srm"]
        rankedProtocols = ["root", "gsiftp", "gsidcap", "dcap", "file", "srm", "rfio"]

        lfn = "/lhcb/fake/lfn"

        # RAL -> GRIDKA
        # We should read using root and write through srm
        res = self.seY.generateTransferURLsBetweenSEs(lfn, self.seX, protocols=rankedProtocols)
        self.assertTrue(res["OK"], res)
        urlPair = res["Value"]["Successful"].get(lfn)
        self.assertTupleEqual(urlPair, ("root:%s" % lfn, "srm:%s" % lfn))
        protoPair = res["Value"]["Protocols"]
        self.assertTupleEqual(protoPair, ("root", "srm"))

        # RAL -> CERN
        # We should read using root and write directly with it
        res = self.seZ.generateTransferURLsBetweenSEs(lfn, self.seX, protocols=rankedProtocols)
        self.assertTrue(res["OK"], res)
        urlPair = res["Value"]["Successful"].get(lfn)
        self.assertTupleEqual(urlPair, ("root:%s" % lfn, "root:%s" % lfn))
        protoPair = res["Value"]["Protocols"]
        self.assertTupleEqual(protoPair, ("root", "root"))

        # GRIDKA -> RAL
        # We should read using gsiftp and write directly with it
        res = self.seX.generateTransferURLsBetweenSEs(lfn, self.seY, protocols=rankedProtocols)
        self.assertTrue(res["OK"], res)
        urlPair = res["Value"]["Successful"].get(lfn)
        self.assertTupleEqual(urlPair, ("gsiftp:%s" % lfn, "gsiftp:%s" % lfn))
        protoPair = res["Value"]["Protocols"]
        self.assertTupleEqual(protoPair, ("gsiftp", "gsiftp"))

        # GRIDKA -> CERN
        # We should read using srm and write with root
        res = self.seZ.generateTransferURLsBetweenSEs(lfn, self.seY, protocols=rankedProtocols)
        self.assertTrue(res["OK"], res)
        urlPair = res["Value"]["Successful"].get(lfn)
        self.assertTupleEqual(urlPair, ("srm:%s" % lfn, "root:%s" % lfn))
        protoPair = res["Value"]["Protocols"]
        self.assertTupleEqual(protoPair, ("srm", "root"))

        # CERN -> RAL
        # We should read using srm and write with gsiftp
        res = self.seX.generateTransferURLsBetweenSEs(lfn, self.seZ, protocols=rankedProtocols)
        self.assertTrue(res["OK"], res)
        urlPair = res["Value"]["Successful"].get(lfn)
        self.assertTupleEqual(urlPair, ("srm:%s" % lfn, "gsiftp:%s" % lfn))
        protoPair = res["Value"]["Protocols"]
        self.assertTupleEqual(protoPair, ("srm", "gsiftp"))

        # CERN -> GRIDKA
        # We should read using root and write directly with srm
        res = self.seY.generateTransferURLsBetweenSEs(lfn, self.seZ, protocols=rankedProtocols)
        self.assertTrue(res["OK"], res)
        urlPair = res["Value"]["Successful"].get(lfn)
        self.assertTupleEqual(urlPair, ("root:%s" % lfn, "srm:%s" % lfn))
        protoPair = res["Value"]["Protocols"]
        self.assertTupleEqual(protoPair, ("root", "srm"))


class TestSameSE(unittest.TestCase):
    """Tests to compare two SEs together."""

    @mock.patch(
        "DIRAC.Resources.Storage.StorageFactory.StorageFactory._StorageFactory__generateStorageObject",
        side_effect=mock_StorageFactory_generateStorageObject,
    )
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def setUp(self, _mk_generateStorage, _mk_isLocalSE, _mk_addAccountingOperation):

        # Creating test configuration file
        self.testCfgFileName = os.path.join(tempfile.gettempdir(), "test_StorageElement.cfg")
        cfgContent = """
    DIRAC
    {
      Setup=TestSetup
    }
    Resources{
      StorageElements{
        DiskStorageA
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          GFAL2_SRM2
          {
            Host = srm-diskandtape.cern.ch
            SpaceToken = Disk
            Protocol = srm
            Path = /base/pathDisk
          }
        }
        # Same end point as DiskStorageA, but with a different space token
        # So they should be considered the same
        TapeStorageA
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          GFAL2_SRM2
          {
            Host = srm-diskandtape.cern.ch
            Protocol = srm
            SpaceToken = Tape
            Path = /base/pathDisk
          }
        }
        # Normally does not happen in practice, but this is the same as DiskStorageA with more plugins
        DiskStorageAWithMoreProtocol
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          GFAL2_SRM2
          {
            Host = srm-diskandtape.cern.ch
            SpaceToken = Disk
            Protocol = srm
            Path = /base/pathDisk
          }
          GFAL2_GSIFTP
          {
            Host = gsiftp-diskandtape.cern.ch
            SpaceToken = Disk
            Protocol = gsiftp
            Path = /base/pathDisk
          }
        }
        # A different storage
        StorageB
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          GFAL2_GSIFTP
          {
            Host = otherstorage.cern.ch
            SpaceToken = Disk
            Protocol = gsiftp
            Path = /base/pathDisk
          }
        }
        # The same endpoint as StorageB but with differetn base path, so not the same
        StorageBWithOtherBasePath
        {
          BackendType = local
          ReadAccess = Active
          WriteAccess = Active
          GFAL2_GSIFTP
          {
            Host = otherstorage.cern.ch
            SpaceToken = Disk
            Protocol = gsiftp
            Path = /base/otherPath
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

        with open(self.testCfgFileName, "w") as f:
            f.write(cfgContent)

        # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
        # not to conflict with other tests that might be using a local dirac.cfg
        gConfigurationData.localCFG = CFG()
        gConfigurationData.remoteCFG = CFG()
        gConfigurationData.mergedCFG = CFG()
        gConfigurationData.generateNewVersion()

        gConfig = ConfigurationClient(
            fileToLoadList=[self.testCfgFileName]
        )  # we replace the configuration by our own one.

        self.diskStorageA = StorageElementItem("DiskStorageA")
        self.diskStorageA.vo = "lhcb"
        self.tapeStorageA = StorageElementItem("TapeStorageA")
        self.tapeStorageA.vo = "lhcb"
        self.diskStorageAWithMoreProtocol = StorageElementItem("DiskStorageAWithMoreProtocol")
        self.diskStorageAWithMoreProtocol.vo = "lhcb"
        self.storageB = StorageElementItem("StorageB")
        self.storageB.vo = "lhcb"
        self.storageBWithOtherBasePath = StorageElementItem("StorageBWithOtherBasePath")
        self.storageBWithOtherBasePath.vo = "lhcb"

    def tearDown(self):
        try:
            os.remove(self.testCfgFileName)
        except OSError:
            pass
        # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
        # not to conflict with other tests that might be using a local dirac.cfg
        gConfigurationData.localCFG = CFG()
        gConfigurationData.remoteCFG = CFG()
        gConfigurationData.mergedCFG = CFG()
        gConfigurationData.generateNewVersion()

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_01_compareSEWithItself(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test case to compare SE together
        """

        for se in (
            self.diskStorageA,
            self.tapeStorageA,
            self.diskStorageAWithMoreProtocol,
            self.storageB,
            self.storageBWithOtherBasePath,
        ):

            self.assertTrue(se.isSameSE(se))

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_02_compareSEThatShouldBeTheSame(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test SEs that should be the considered same
        """

        matchingCouples = (
            (self.diskStorageA, self.diskStorageAWithMoreProtocol),
            (self.diskStorageA, self.tapeStorageA),
        )

        for se1, se2 in matchingCouples:
            self.assertTrue(se1.isSameSE(se2))

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_02_compareSEThatShouldBeDifferent(self, _mk_isLocalSE, _mk_addAccounting):
        """
        Test SEs that should be the considered same
        """

        notMatchingCouples = (
            (self.diskStorageA, self.storageB),
            (self.tapeStorageA, self.storageB),
            (self.storageB, self.storageBWithOtherBasePath),
        )

        for se1, se2 in notMatchingCouples:
            self.assertFalse(se1.isSameSE(se2))


if __name__ == "__main__":
    from DIRAC import gLogger

    gLogger.setLevel("DEBUG")
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestBase)

    unittest.TextTestRunner(verbosity=2).run(suite)
