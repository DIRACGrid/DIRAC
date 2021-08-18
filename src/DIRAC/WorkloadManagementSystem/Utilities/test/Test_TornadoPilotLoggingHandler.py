import unittest
from unittest.mock import patch
import os
import json
import tempfile
import DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler
from DIRAC.WorkloadManagementSystem.Client.PilotLoggingPlugins.FileCacheLoggingPlugin import FileCacheLoggingPlugin
from DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler import TornadoPilotLoggingHandler


class TornadoPilotLoggingHandlerTestCase(unittest.TestCase):
    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler.os, "makedirs")
    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler.os, "getcwd")
    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler.os.path, "exists")
    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler, "getServiceOption")
    def test_initializeHandlerBasic(self, mockOpt, mockExists, mockGetcwd, mockMakedirs):
        """
        Test the initialiser with a PilotLoggingPlugin. os.* calls used are mocked.

        :param mockExists:
        :type mockExists:
        :param mockGetcwd:
        :type mockGetcwd:
        :param mockMakedirs:
        :type mockMakedirs:
        :return:
        :rtype:
        """
        mockExists.return_value = False  # will create a file
        mockGetcwd.return_value = "/tornado/document/root"
        mockOpt.return_value = "FileCacheLoggingPlugin"
        TornadoPilotLoggingHandler.initializeHandler(
            {"csPaths": "noexistent"}
        )  # should return FileCacheLoggingPlugin plugin
        mockMakedirs.assert_called_with(os.path.join("/tornado/document/root", "pilotlogs"))
        self.assertEqual(TornadoPilotLoggingHandler.loggingPlugin.__class__.__name__, "FileCacheLoggingPlugin")

        mockMakedirs.reset_mock()
        mockExists.return_value = True
        mockGetcwd.return_value = "/tornado/document/root"
        TornadoPilotLoggingHandler.initializeHandler(
            {"csPaths": "noexistent"}
        )  # should return the default, basic plugin
        mockMakedirs.assert_not_called()
        self.assertEqual(TornadoPilotLoggingHandler.loggingPlugin.__class__.__name__, "FileCacheLoggingPlugin")

    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler, "getServiceOption")
    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler.os, "makedirs")
    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler.os, "getcwd")
    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler.os.path, "exists")
    def test_initializeHandlerFileCache(self, mockExists, mockGetcwd, mockMakedirs, mockgetServiceOption):
        """
        Test loading the (real) file cache plugin.

        :return:
        :rtype:
        """

        mockExists.return_value = False  # will create a file
        mockGetcwd.return_value = "/tornado/document/root"
        mockgetServiceOption.return_value = "FileCacheLoggingPlugin"
        TornadoPilotLoggingHandler.initializeHandler(
            {"csPaths": "noexistent"}
        )  # should return the default, basic plugin
        mockMakedirs.assert_called_with(os.path.join("/tornado/document/root", "pilotlogs"))
        self.assertEqual(TornadoPilotLoggingHandler.loggingPlugin.__class__.__name__, "FileCacheLoggingPlugin")
        self.assertEqual(2, mockMakedirs.call_count)  # twice: once in the handler, once in the plugin

    @patch.object(DIRAC.WorkloadManagementSystem.Client.PilotLoggingPlugins.FileCacheLoggingPlugin.os, "makedirs")
    @patch.object(DIRAC.WorkloadManagementSystem.Client.PilotLoggingPlugins.FileCacheLoggingPlugin.os, "getcwd")
    @patch.object(DIRAC.WorkloadManagementSystem.Client.PilotLoggingPlugins.FileCacheLoggingPlugin.os.path, "exists")
    def test_FileCachePlugin(self, mockExists, mockGetcwd, mockMakedirs):
        """
        Test fileCachePlugin
        """
        mockExists.return_value = False  # will create a file
        mockGetcwd.return_value = "/tornado/document/root"
        plugin = FileCacheLoggingPlugin()
        mockGetcwd.assert_called_once()
        mockExists.assert_called_once_with(os.path.join(mockGetcwd.return_value, "pilotlogs"))
        mockMakedirs.assert_called_once_with(os.path.join(mockGetcwd.return_value, "pilotlogs"))

        # sendMessage()
        messsageText = (
            "2022-02-23 13:48:35.123456 UTC DEBUG [PilotParams] JSON file loaded: pilot.json\n"
            + "2022-02-23 13:48:36.123456 UTC DEBUG [PilotParams] JSON file analysed: pilot.json"
        )
        messageJSON = json.dumps(messsageText)
        vo = "anyVO"
        pilotUUID = "78f39a90-2073-11ec-98d7-b496913c0cf4"

        # use a temporary dir, not the one above. Plugin will create the file to write into.
        with tempfile.TemporaryDirectory(suffix="pilottests") as d:
            plugin.meta["LogPath"] = d
            res = plugin.sendMessage(messageJSON, pilotUUID, vo)
            self.assertTrue(res["OK"])
            with open(os.path.join(d, vo, pilotUUID)) as pilotLog:
                content = pilotLog.read()
                self.assertEqual(content, messsageText)

        # failures ?
        with tempfile.TemporaryDirectory(suffix="pilottests") as d:
            plugin.meta["LogPath"] = d
            os.chmod(d, 0o0000)
            res = plugin.sendMessage(messageJSON, pilotUUID, vo)
            self.assertFalse(res["OK"])

        pilotUUID = "whatever"
        res = plugin.sendMessage(messageJSON, pilotUUID, vo)
        self.assertFalse(res["OK"])

    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler.os.path, "exists")
    @patch.object(DIRAC.WorkloadManagementSystem.Client.PilotLoggingPlugins.FileCacheLoggingPlugin.os, "getcwd")
    def test_getMeta(self, mockGetcwd, mockExists):
        mockExists.return_value = True  # will not create a file
        mockGetcwd.return_value = "/tornado/document/root"  # so we have a path defined
        plugin = FileCacheLoggingPlugin()
        res = plugin.getMeta()
        self.assertTrue(res["OK"])
        plugin.meta = {}
        res = plugin.getMeta()
        self.assertFalse(res["OK"])

    @patch.object(DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler.os.path, "exists")
    @patch.object(DIRAC.WorkloadManagementSystem.Client.PilotLoggingPlugins.FileCacheLoggingPlugin.os, "getcwd")
    def test_finaliseLogs(self, mockGetcwd, mockExists):
        mockExists.return_value = True  # will not create a file
        mockGetcwd.return_value = "/tornado/document/root"  # so we have a path defined (will overwrite it below)
        plugin = FileCacheLoggingPlugin()
        vo = "anyVO"

        with tempfile.TemporaryDirectory(suffix="pilottests") as d:
            plugin.meta["LogPath"] = d
            payload = '{"retCode": 0}'
            logfile = "78f39a90-2073-11ec-98d7-b496913c0cf4"  # == pilotUUID
            os.mkdir(os.path.join(d, vo))  # vo specific directory, normally created by sending the first message
            # will fail here...
            res = plugin.finaliseLogs(payload, logfile, vo)
            self.assertFalse(res["OK"])
            # create a file ..
            with open(os.path.join(d, vo, logfile), "w") as f:
                f.write("Create a dummy logfile")
            res = plugin.finaliseLogs(payload, logfile, vo)
            self.assertTrue(res["OK"])

            logfile = "invalid!"
            res = plugin.finaliseLogs(payload, logfile, vo)
            self.assertFalse(res["OK"])


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TornadoPilotLoggingHandlerTestCase)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
