import unittest
from unittest.mock import patch

from DIRAC import S_ERROR, S_OK
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient


class TestFileCatalogClient(unittest.TestCase):
    def setUp(self):
        self.client = FileCatalogClient()

    def test_get_file_user_metadata_bulk(self):
        def get_file_user_metadata(lfn, timeout=120):
            if lfn.endswith("missing"):
                return S_ERROR("No such file")
            return S_OK({"key1": "value1"})

        with patch.object(self.client, "getFileUserMetadata", side_effect=get_file_user_metadata):
            result = self.client.getFileUserMetadataBulk(["/a/file", "/a/missing"])

        self.assertEqual(
            result,
            S_OK(
                {
                    "Successful": {"/a/file": {"key1": "value1"}},
                    "Failed": {"/a/missing": "No such file"},
                }
            ),
        )

    def test_get_file_user_metadata_bulk_is_registered_as_read_method(self):
        self.assertIn("getFileUserMetadataBulk", self.client.getInterfaceMethods()[0])


if __name__ == "__main__":
    unittest.main()
