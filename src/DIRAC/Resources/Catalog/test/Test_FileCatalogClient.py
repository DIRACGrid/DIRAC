import unittest
from unittest.mock import Mock, patch

from DIRAC import S_OK
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient


class TestFileCatalogClient(unittest.TestCase):
    def setUp(self):
        self.client = FileCatalogClient()

    def test_get_file_user_metadata_bulk(self):
        rpc_result = S_OK(
            {
                "Successful": {"/a/file": {"key1": "value1"}},
                "Failed": {"/a/missing": "No such file"},
            }
        )
        rpc_client = Mock()
        rpc_client.getFileUserMetadataBulk.return_value = rpc_result

        with patch.object(self.client, "_getRPC", return_value=rpc_client) as get_rpc:
            result = self.client.getFileUserMetadataBulk(["/a/file", "/a/missing"])

        self.assertEqual(result, rpc_result)
        get_rpc.assert_called_once_with(timeout=120)
        rpc_client.getFileUserMetadataBulk.assert_called_once_with({"/a/file": True, "/a/missing": True})

    def test_get_file_user_metadata_bulk_is_registered_as_read_method(self):
        self.assertIn("getFileUserMetadataBulk", self.client.getInterfaceMethods()[0])


if __name__ == "__main__":
    unittest.main()
