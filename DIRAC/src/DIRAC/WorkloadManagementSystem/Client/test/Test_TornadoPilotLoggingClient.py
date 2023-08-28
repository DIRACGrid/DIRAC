import unittest
from unittest.mock import patch
from unittest.mock import MagicMock

import DIRAC.WorkloadManagementSystem.Client.TornadoPilotLoggingClient as tplc


class TestTornadoPilotLoggingClient(unittest.TestCase):
    def test_client(self):
        client = tplc.TornadoPilotLoggingClient("test.server", useCertificates=True)
        self.assertEqual(client.serverURL, "test.server")
        client = tplc.TornadoPilotLoggingClient(useCertificates=True)
        self.assertEqual(client.serverURL, "WorkloadManagement/TornadoPilotLogging")


if __name__ == "__main__":
    unittest.main()
