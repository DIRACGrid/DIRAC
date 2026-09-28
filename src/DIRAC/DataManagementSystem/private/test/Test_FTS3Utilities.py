""" Test the FTS3Utilities"""
import unittest

from unittest import mock

from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC import S_OK, S_ERROR

from DIRAC.DataManagementSystem.private import FTS3Utilities
from DIRAC.DataManagementSystem.private.FTS3Utilities import (
    groupFilesByTarget,
    selectUniqueSource,
    FTS3ServerPolicy,
    getFTS3Plugin,
)
from DIRAC.DataManagementSystem.private.FTS3Plugins.DefaultFTS3Plugin import DefaultFTS3Plugin


def mock__checkSourceReplicas(ftsFiles, preferDisk=False):
    succ = {}
    failed = {}

    for ftsFile in ftsFiles:
        if hasattr(ftsFile, "fakeAttr_possibleSources"):
            succ[ftsFile.lfn] = dict.fromkeys(getattr(ftsFile, "fakeAttr_possibleSources"))
        else:
            failed[ftsFile.lfn] = "No such file or directory"

    return S_OK({"Successful": succ, "Failed": failed})


class TestFileGrouping(unittest.TestCase):
    """Testing all the grouping functions of FTS3Utilities"""

    def setUp(self):
        self.f1 = FTS3File()
        self.f1.fakeAttr_possibleSources = ["Src1", "Src2"]
        self.f1.lfn = "f1"
        self.f1.targetSE = "target1"

        self.f2 = FTS3File()
        self.f2.fakeAttr_possibleSources = ["Src2", "Src3"]
        self.f2.lfn = "f2"
        self.f2.targetSE = "target2"

        self.f3 = FTS3File()
        self.f3.fakeAttr_possibleSources = ["Src4"]
        self.f3.lfn = "f3"
        self.f3.targetSE = "target1"

        # File does not exist :-)
        self.f4 = FTS3File()
        self.f4.lfn = "f4"
        self.f4.targetSE = "target3"

        self.allFiles = [self.f1, self.f2, self.f3, self.f4]

    def test_01_groupFilesByTarget(self):
        # empty input
        self.assertTrue(groupFilesByTarget([])["Value"] == {})

        res = groupFilesByTarget(self.allFiles)

        self.assertTrue(res["OK"])

        groups = res["Value"]

        self.assertTrue(self.f1 in groups["target1"])
        self.assertTrue(self.f2 in groups["target2"])
        self.assertTrue(self.f3 in groups["target1"])
        self.assertTrue(self.f4 in groups["target3"])

    @mock.patch(
        "DIRAC.DataManagementSystem.private.FTS3Utilities._checkSourceReplicas", side_effect=mock__checkSourceReplicas
    )
    def test_04_selectUniqueSource(self, _mk_checkSourceReplicas):
        """Suppose they all go to the same target"""

        fts3Plugin = DefaultFTS3Plugin()
        res = selectUniqueSource(self.allFiles, fts3Plugin)

        self.assertTrue(res["OK"])

        uniqueSources, _failedFiles = res["Value"]

        # There should be only f1,f2 and f3
        allReturnedFiles = []
        existingFiles = [self.f1, self.f2, self.f3]
        for _srcSe, ftsFiles in uniqueSources.items():
            allReturnedFiles.extend(ftsFiles)

        # No files should be duplicated and all files should be there, except the non existing one
        self.assertEqual(len(existingFiles), len(allReturnedFiles))
        self.assertEqual(set(existingFiles), set(allReturnedFiles))

        filesInSrc1 = uniqueSources.get("Src1", [])
        filesInSrc2 = uniqueSources.get("Src2", [])
        filesInSrc3 = uniqueSources.get("Src3", [])

        filesInSrc4 = uniqueSources.get("Src4", [])
        # f1
        self.assertTrue(self.f1 in filesInSrc1 + filesInSrc2)
        self.assertTrue(self.f2 in filesInSrc2 + filesInSrc3)
        self.assertTrue(self.f3 in filesInSrc4)


def mock__failoverServerPolicy(_attempt):
    return "server_0"


def mock__randomServerPolicy(_attempt):
    return "server_0"


def mock__sequenceServerPolicy(_attempt):
    return "server_0"


def mock__OKFTSServerStatus(ftsServer):
    return S_OK(ftsServer)


def mock__ErrorFTSServerStatus(ftsServer):
    return S_ERROR(ftsServer)


class TestFTS3ServerPolicy(unittest.TestCase):
    """Testing FTS3 ServerPolicy selection"""

    def setUp(self):
        self.fakeServerDict = {
            "server_0": "server0.cern.ch",
            "server_1": "server1.cern.ch",
            "server_2": "server2.cern.ch",
        }

    @mock.patch(
        "DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._getFTSServerStatus",
        side_effect=mock__OKFTSServerStatus,
    )
    @mock.patch(
        "DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._sequenceServerPolicy",
        side_effect=mock__sequenceServerPolicy,
    )
    @mock.patch(
        "DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._randomServerPolicy",
        side_effect=mock__randomServerPolicy,
    )
    @mock.patch(
        "DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._failoverServerPolicy",
        side_effect=mock__failoverServerPolicy,
    )
    def testCorrectServerPolicyIsUsed(self, mockFailoverFunc, mockRandomFunc, mockSequenceFunc, mockFTSServerStatus):
        "Test correct server policy method is called"

        obj = FTS3ServerPolicy(self.fakeServerDict, "Sequence")
        obj.chooseFTS3Server()
        self.assertTrue(mockSequenceFunc.called)

        obj = FTS3ServerPolicy(self.fakeServerDict, "Random")
        obj.chooseFTS3Server()
        self.assertTrue(mockRandomFunc.called)

        obj = FTS3ServerPolicy(self.fakeServerDict, "Failover")
        obj.chooseFTS3Server()
        self.assertTrue(mockFailoverFunc.called)

        # random policy should be selected for an invalid policy
        obj = FTS3ServerPolicy(self.fakeServerDict, "InvalidPolicy")
        obj.chooseFTS3Server()
        self.assertTrue(mockRandomFunc.called)

    @mock.patch(
        "DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._getFTSServerStatus",
        side_effect=mock__ErrorFTSServerStatus,
    )
    def testFailoverServerPolicy(self, mockFTSServerStatus):
        """Test if the failover server policy returns server at a given position"""

        obj = FTS3ServerPolicy(self.fakeServerDict, "Failover")
        for i in range(len(self.fakeServerDict)):
            self.assertEqual("server_%d" % i, obj._failoverServerPolicy(i))

    @mock.patch(
        "DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._getFTSServerStatus",
        side_effect=mock__ErrorFTSServerStatus,
    )
    def testSequenceServerPolicy(self, mockFTSServerStatus):
        """Test if the sequence server policy selects the servers Sequentially"""

        obj = FTS3ServerPolicy(self.fakeServerDict, "Sequence")

        for i in range(len(self.fakeServerDict)):
            self.assertEqual("server_%d" % i, obj._sequenceServerPolicy(i))

        self.assertEqual("server_0", obj._sequenceServerPolicy(i))

    @mock.patch(
        "DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._getFTSServerStatus",
        side_effect=mock__ErrorFTSServerStatus,
    )
    def testRandomServerPolicy(self, mockFTSServerStatus):
        """Test if the random server policy does not selects the same server multiple times"""

        obj = FTS3ServerPolicy(self.fakeServerDict, "Random")
        serverSet = set()

        for i in range(len(self.fakeServerDict)):
            serverSet.add(obj._randomServerPolicy(i))

        self.assertEqual(len(serverSet), len(self.fakeServerDict))


class FakeFTS3Plugin:
    """Stands for a plugin class loaded by the ObjectLoader"""

    def __init__(self, vo=None):
        self.vo = vo


class TestGetFTS3Plugin(unittest.TestCase):
    """Testing that the FTS3Plugin is shared per VO"""

    def setUp(self):
        FTS3Utilities._fts3PluginCache.clear()
        self.csVersion = "v1"
        self.pluginName = "Fake"

        patchers = [
            mock.patch(
                "DIRAC.DataManagementSystem.private.FTS3Utilities.gConfigurationData.getVersion",
                side_effect=lambda: self.csVersion,
            ),
            mock.patch(
                "DIRAC.DataManagementSystem.private.FTS3Utilities.opHelper",
                side_effect=lambda vo=None: mock.MagicMock(getValue=mock.MagicMock(return_value=self.pluginName)),
            ),
            mock.patch(
                "DIRAC.DataManagementSystem.private.FTS3Utilities.ObjectLoader.loadObject",
                return_value=S_OK(FakeFTS3Plugin),
            ),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self):
        FTS3Utilities._fts3PluginCache.clear()

    def testSameInstancePerVO(self):
        """The same VO gets the same instance, a different VO a different one"""
        pluginA = getFTS3Plugin(vo="voA")
        self.assertIs(pluginA, getFTS3Plugin(vo="voA"))
        self.assertEqual(pluginA.vo, "voA")

        pluginB = getFTS3Plugin(vo="voB")
        self.assertIsNot(pluginA, pluginB)
        self.assertEqual(pluginB.vo, "voB")

        self.assertIsNot(pluginA, getFTS3Plugin())

    def testNewInstanceOnCSRefresh(self):
        """A new instance is created when the CS version changes"""
        plugin = getFTS3Plugin(vo="voA")
        self.csVersion = "v2"
        newPlugin = getFTS3Plugin(vo="voA")
        self.assertIsNot(plugin, newPlugin)
        self.assertIs(newPlugin, getFTS3Plugin(vo="voA"))

    def testNewInstanceOnPluginChange(self):
        """A new instance is created when the configured plugin changes"""
        plugin = getFTS3Plugin(vo="voA")
        self.pluginName = "OtherFake"
        self.assertIsNot(plugin, getFTS3Plugin(vo="voA"))

    def testLoadFailure(self):
        """An exception is raised if the plugin cannot be loaded, and nothing is cached"""
        with mock.patch(
            "DIRAC.DataManagementSystem.private.FTS3Utilities.ObjectLoader.loadObject",
            return_value=S_ERROR("No such plugin"),
        ):
            with self.assertRaises(Exception):
                getFTS3Plugin(vo="voA")
        self.assertFalse(FTS3Utilities._fts3PluginCache)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestFileGrouping)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestFTS3ServerPolicy))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestGetFTS3Plugin))
    unittest.TextTestRunner(verbosity=2).run(suite)
