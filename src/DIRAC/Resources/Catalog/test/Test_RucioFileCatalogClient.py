import unittest
from unittest.mock import MagicMock, patch
from DIRAC.Resources.Catalog.RucioFileCatalogClient import RucioFileCatalogClient


class TestRucioFileCatalogClient(unittest.TestCase):
    def setUp(self):
        self.patcher = patch.object(RucioFileCatalogClient, "client", new_callable=MagicMock)
        self.client = RucioFileCatalogClient()
        self.client.scopes = ["test_scope"]
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_transform_DIRAC_operator_to_Rucio(self):
        DIRAC_dict = {"key1": "value1", "key2": {">": 10}, "key3": {"=": 10}}
        expected_output = {"key1": "value1", "key2.gt": 10, "key3": 10}
        result = self.client._RucioFileCatalogClient__transform_DIRAC_operator_to_Rucio(DIRAC_dict)
        self.assertEqual(result, expected_output)

    def test_transform_dict_with_in_operateur_2steps(self):
        DIRAC_dict_with_in_operator_list = [
            {
                "particle": {"in": ["proton", "electron"]},
                "site": {"in": ["LaPalma", "paranal"]},
                "configuration_id": {"=": 14},
            }
        ]
        expected_intermediate_output = [
            {"particle": "proton", "site": {"in": ["LaPalma", "paranal"]}, "configuration_id": {"=": 14}},
            {"particle": "electron", "site": {"in": ["LaPalma", "paranal"]}, "configuration_id": {"=": 14}},
        ]
        expected_final_output = [
            {"particle": "proton", "site": "LaPalma", "configuration_id": {"=": 14}},
            {"particle": "proton", "site": "paranal", "configuration_id": {"=": 14}},
            {"particle": "electron", "site": "LaPalma", "configuration_id": {"=": 14}},
            {"particle": "electron", "site": "paranal", "configuration_id": {"=": 14}},
        ]
        result_intermediate, _ = self.client._RucioFileCatalogClient__transform_dict_with_in_operateur(
            DIRAC_dict_with_in_operator_list
        )
        self.assertEqual(result_intermediate, expected_intermediate_output)
        result_final, _ = self.client._RucioFileCatalogClient__transform_dict_with_in_operateur(result_intermediate)
        self.assertEqual(result_final, expected_final_output)

    def test_transform_DIRAC_operator_to_Rucio_simple_key_value(self):
        input_dict = {"key1": "value1", "key2": "value2"}
        expected_output = {"key1": "value1", "key2": "value2"}
        result = self.client._RucioFileCatalogClient__transform_DIRAC_operator_to_Rucio(input_dict)
        self.assertEqual(result, expected_output)

    def test_transform_DIRAC_operator_to_Rucio_nested_dict_with_operators_gl(self):
        input_dict = {
            "start": {">=": 10},
            "end": {">": 5},
            "pointingZ": {">=": 0.1},
            "organization": "ViaCorp",
            "data_levels": "DL3",
        }
        expected_output = {
            "start.gte": 10,
            "end.gt": 5,
            "pointingZ.gte": 0.1,
            "organization": "ViaCorp",
            "data_levels": "DL3",
        }
        result = self.client._RucioFileCatalogClient__transform_DIRAC_operator_to_Rucio(input_dict)
        self.assertEqual(result, expected_output)

    def test_transform_DIRAC_operator_to_Rucio_nested_dict_with_operators_equals(self):
        input_dict = {"start": {"=": 10}, "pointingZ": {"=": 0.1}, "organization": "ViaCorp", "data_levels": "DL3"}
        expected_output = {"start": 10, "pointingZ": 0.1, "organization": "ViaCorp", "data_levels": "DL3"}
        result = self.client._RucioFileCatalogClient__transform_DIRAC_operator_to_Rucio(input_dict)
        assert result == expected_output

    def test_transform_DIRAC_operator_to_Rucio_mixed_dict(self):
        input_dict = {"key1": "value1", "key2": {">": 10}, "key3": {"=": 10}}
        expected_output = {"key1": "value1", "key2.gt": 10, "key3": 10}
        result = self.client._RucioFileCatalogClient__transform_DIRAC_operator_to_Rucio(input_dict)
        assert result == expected_output

    def test_transform_DIRAC_operator_to_Rucio_in_operator(self):
        input_dict = [
            {
                "analysis_prog": {"in": ["ctapipe-merge", "ctapipe-process", "ctapipe-apply-models"]},
                "key1": "value1",
                "key3": {"=": 10},
                "key4": {"<": 5},
            }
        ]
        expected_intermediate = [
            {"key1": "value1", "key3": 10, "key4.lt": 5, "analysis_prog": "ctapipe-merge"},
            {"key1": "value1", "key3": 10, "key4.lt": 5, "analysis_prog": "ctapipe-process"},
            {"key1": "value1", "key3": 10, "key4.lt": 5, "analysis_prog": "ctapipe-apply-models"},
        ]
        result_interm = self.client._RucioFileCatalogClient__transform_DIRAC_filter_dict_to_Rucio_filter_dict(
            input_dict
        )
        assert result_interm == expected_intermediate

    def test_transform_DIRAC_operator_to_Rucio_2timesin_operator(self):
        input_dict = [{"particle": {"in": ["proton", "electron"]}, "site": {"in": ["LaPalma", "paranal"]}}]
        expected = [
            {"particle": "proton", "site": "LaPalma"},
            {"particle": "proton", "site": "paranal"},
            {"particle": "electron", "site": "LaPalma"},
            {"particle": "electron", "site": "paranal"},
        ]
        result = self.client._RucioFileCatalogClient__transform_DIRAC_filter_dict_to_Rucio_filter_dict(input_dict)
        assert result == expected

    def test_2timesin_mix_operator(self):
        input_dict = [
            {
                "particle": {"in": ["proton", "electron"]},
                "site": {"in": ["LaPalma", "paranal"]},
                "configuration_id": {"=": 14},
            }
        ]
        expected = [
            {"particle": "proton", "site": "LaPalma", "configuration_id": 14},
            {"particle": "proton", "site": "paranal", "configuration_id": 14},
            {"particle": "electron", "site": "LaPalma", "configuration_id": 14},
            {"particle": "electron", "site": "paranal", "configuration_id": 14},
        ]
        result = self.client._RucioFileCatalogClient__transform_DIRAC_filter_dict_to_Rucio_filter_dict(input_dict)
        assert result == expected

        input_dict = [
            {
                "particle": {"in": ["proton", "electron"]},
                "configuration_id": {"=": 14},
                "site": {"in": ["LaPalma", "paranal"]},
            }
        ]
        expected = [
            {"particle": "proton", "configuration_id": 14, "site": "LaPalma"},
            {"particle": "proton", "configuration_id": 14, "site": "paranal"},
            {"particle": "electron", "configuration_id": 14, "site": "LaPalma"},
            {"particle": "electron", "configuration_id": 14, "site": "paranal"},
        ]
        result = self.client._RucioFileCatalogClient__transform_DIRAC_filter_dict_to_Rucio_filter_dict(input_dict)
        assert result == expected

    def test_transform_DIRAC_filter_dict_to_Rucio_filter_dict(self):
        DIRAC_filter_dict_list = [
            {
                "particle": {"in": ["proton", "electron"]},
                "configuration_id": {"=": 14},
                "site": {"in": ["LaPalma", "paranal"]},
            }
        ]
        expected_output = [
            {"particle": "proton", "configuration_id": 14, "site": "LaPalma"},
            {"particle": "proton", "configuration_id": 14, "site": "paranal"},
            {"particle": "electron", "configuration_id": 14, "site": "LaPalma"},
            {"particle": "electron", "configuration_id": 14, "site": "paranal"},
        ]
        result = self.client._RucioFileCatalogClient__transform_DIRAC_filter_dict_to_Rucio_filter_dict(
            DIRAC_filter_dict_list
        )
        self.assertEqual(result, expected_output)

    def test_findFilesByMetadata(self):
        self.client.client.list_dids.return_value = ["did1", "did2"]
        metadataFilterDict = {"key1": "value1"}
        result = self.client.findFilesByMetadata(metadataFilterDict)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ["did1", "did2"])

    def test_findFilesByMetadata_with_error(self):
        self.client.client.list_dids.side_effect = Exception("Test error")
        metadataFilterDict = {"key1": "value1"}
        result = self.client.findFilesByMetadata(metadataFilterDict)
        self.assertFalse(result["OK"])
        self.assertIn("Test error", result["Message"])


if __name__ == "__main__":
    unittest.main()
