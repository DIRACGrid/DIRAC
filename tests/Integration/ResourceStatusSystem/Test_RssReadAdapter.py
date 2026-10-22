"""Tests for the RSS Read Adapter.

This test verifies that the RSS read adapter can be integrated with the
ResourceStatus system and produces output in the correct format.
"""

# pylint: disable=wrong-import-position, missing-docstring
import pytest
from unittest.mock import MagicMock, patch

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.RssReadAdapter import get_resource_rss_status, get_site_status

# Set up logging
gLogger.setLevel("DEBUG")


class TestRssReadAdapter:
    """Tests for the RSS Read Adapter."""

    @patch("DIRAC.ResourceStatusSystem.Client.RssReadAdapter.DiracXClient")
    def test_rss_status_format(self, mock_diracx_client):
        """Test that adapter output is compatible with ResourceStatus cache format."""
        # Mock the diracx client and its response
        mock_client = MagicMock()
        mock_diracx_client.return_value.__enter__.return_value = mock_client

        # Mock response from diracx RSS API
        mock_response = {
            "CERN-EOS": {
                "read": {"allowed": True},
                "write": {"allowed": True, "warnings": "Degraded"},
                "check": {"allowed": False, "reason": "Banned"},
                "remove": {"allowed": True},
            },
            "CERN-CASTOR": {
                "read": {"allowed": True},
                "write": {"allowed": False, "reason": "Error"},
                "check": {"allowed": True},
                "remove": {"allowed": True},
            },
        }

        # Convert mock response to proper diracx models
        from diracx.core.models.rss import StorageElementStatus, AllowedStatus, BannedStatus

        proper_response = {}
        for se_name, status_dict in mock_response.items():
            proper_response[se_name] = StorageElementStatus(
                read=AllowedStatus(allowed=True)
                if status_dict["read"]["allowed"]
                else BannedStatus(allowed=False, reason="Unknown"),
                write=AllowedStatus(allowed=True, warnings=status_dict["write"].get("warnings"))
                if status_dict["write"]["allowed"]
                else BannedStatus(allowed=False, reason=status_dict["write"].get("reason", "Unknown")),
                check=BannedStatus(allowed=False, reason=status_dict["check"].get("reason", "Unknown")),
                remove=AllowedStatus(allowed=True)
                if status_dict["remove"]["allowed"]
                else BannedStatus(allowed=False, reason="Unknown"),
            )

        mock_client.rss.get_storage_status.return_value = proper_response

        # Call the adapter function
        result = get_resource_rss_status(mock_client)

        # Verify the output can be converted to the format expected by ResourceStatus
        # The ResourceStatus cache expects: {(name, elementType, statusType, vo): status}
        legacy_cache = {}
        for entry in result:
            name, element_type, status_type, status, vo = entry
            cache_key = (name, element_type, status_type, vo)
            legacy_cache[cache_key] = status

        # Verify we have the expected number of entries
        assert len(legacy_cache) >= 8, f"Expected at least 8 cache entries, got {len(legacy_cache)}"

        # Verify the cache format matches what ResourceStatus expects
        for cache_key, cache_value in legacy_cache.items():
            # Verify cache key format: (name, elementType, statusType, vo)
            assert isinstance(cache_key, tuple), f"Cache key should be tuple, got {type(cache_key)}"
            assert len(cache_key) == 4, f"Cache key should have 4 elements, got {len(cache_key)}"

            name, element_type, status_type, vo = cache_key
            assert element_type == "StorageElement", f"Expected 'StorageElement', got {element_type}"
            assert status_type in [
                "ReadAccess",
                "WriteAccess",
                "CheckAccess",
                "RemoveAccess",
            ], f"Invalid status type: {status_type}"
            assert vo is None, f"VO should be None, got {vo}"

            # Verify cache value format: status string
            assert isinstance(cache_value, str), f"Cache value should be string, got {type(cache_value)}"
            assert cache_value in ["Active", "Banned"], f"Status should be 'Active' or 'Banned', got {cache_value}"

        # Verify specific status mappings
        cern_eos_write = [(k, v) for k, v in legacy_cache.items() if k[0] == "CERN-EOS" and k[2] == "WriteAccess"]
        assert len(cern_eos_write) == 1, "Should have exactly one WriteAccess entry for CERN-EOS"
        assert cern_eos_write[0][1] == "Active", "Degraded should be mapped to Active"

        cern_eos_check = [(k, v) for k, v in legacy_cache.items() if k[0] == "CERN-EOS" and k[2] == "CheckAccess"]
        assert len(cern_eos_check) == 1, "Should have exactly one CheckAccess entry for CERN-EOS"
        assert cern_eos_check[0][1] == "Banned", "Banned status should be preserved"

    @patch("DIRAC.ResourceStatusSystem.Client.RssReadAdapter.DiracXClient")
    def test_computing_element(self, mock_diracx_client):
        """Test computing element with ResourceStatus format."""
        # Mock the diracx client and its response
        mock_client = MagicMock()
        mock_diracx_client.return_value.__enter__.return_value = mock_client

        # Mock response from diracx RSS API
        from diracx.core.models.rss import ComputeElementStatus, AllowedStatus, BannedStatus

        mock_response = {
            "CE1.example.com": ComputeElementStatus(all=AllowedStatus(allowed=True)),
            "CE2.example.com": ComputeElementStatus(all=BannedStatus(allowed=False, reason="Probing")),
        }
        mock_client.rss.get_compute_status.return_value = mock_response

        # Call the adapter function
        result = get_resource_rss_status(mock_client)

        # Convert to legacy cache format
        legacy_cache = {}
        for entry in result:
            name, element_type, status_type, status, vo = entry
            cache_key = (name, element_type, status_type, vo)
            legacy_cache[cache_key] = status

        # Verify format
        assert len(legacy_cache) >= 2, f"Expected at least 2 cache entries, got {len(legacy_cache)}"

        for cache_key, cache_value in legacy_cache.items():
            assert isinstance(cache_key, tuple) and len(cache_key) == 4
            assert cache_key[1] == "ComputeElement"
            assert cache_key[2] == "all"
            assert cache_value in ["Active", "Banned"]

        # Verify Probing is mapped to Banned
        ce2_entry = [(k, v) for k, v in legacy_cache.items() if k[0] == "CE2.example.com"][0]
        assert ce2_entry[1] == "Banned", "Probing should be mapped to Banned"

    @patch("DIRAC.ResourceStatusSystem.Client.RssReadAdapter.DiracXClient")
    def test_fts(self, mock_diracx_client):
        """Test FTS with ResourceStatus format."""
        # Mock the diracx client and its response
        mock_client = MagicMock()
        mock_diracx_client.return_value.__enter__.return_value = mock_client

        # Mock response from diracx RSS API
        from diracx.core.models.rss import FTSStatus, AllowedStatus, BannedStatus

        mock_response = {
            "fts1.cern.ch": FTSStatus(all=AllowedStatus(allowed=True)),
            "fts2.cern.ch": FTSStatus(all=BannedStatus(allowed=False, reason="Error")),
        }
        mock_client.rss.get_fts_status.return_value = mock_response

        # Call the adapter function
        result = get_resource_rss_status(mock_client)

        # Convert to legacy cache format
        legacy_cache = {}
        for entry in result:
            name, element_type, status_type, status, vo = entry
            cache_key = (name, element_type, status_type, vo)
            legacy_cache[cache_key] = status

        # Verify format
        assert len(legacy_cache) >= 2, f"Expected at least 2 cache entries, got {len(legacy_cache)}"

        for cache_key, cache_value in legacy_cache.items():
            assert isinstance(cache_key, tuple) and len(cache_key) == 4
            assert cache_key[1] == "FTS"
            assert cache_key[2] == "all"
            assert cache_value in ["Active", "Banned"]

    @patch("DIRAC.ResourceStatusSystem.Client.RssReadAdapter.DiracXClient")
    def test_site(self, mock_diracx_client):
        """Test site with ResourceStatus format."""
        # Mock the diracx client and its response
        mock_client = MagicMock()
        mock_diracx_client.return_value.__enter__.return_value = mock_client

        # Mock response from diracx RSS API
        from diracx.core.models.rss import SiteStatus, AllowedStatus, BannedStatus

        mock_response = {
            "CERN": SiteStatus(all=AllowedStatus(allowed=True)),
            "DESY": SiteStatus(all=BannedStatus(allowed=False)),
        }
        mock_client.rss.get_site_status.return_value = mock_response

        # Call the adapter function
        result = get_site_status(mock_client)

        # Site status uses a different format: list of (name, status) tuples
        assert isinstance(result, list), "Result should be a list"
        assert len(result) == 2, f"Expected 2 entries, got {len(result)}"

        for entry in result:
            assert isinstance(entry, tuple) and len(entry) == 2
            name, status = entry
            assert status in ["Active", "Banned"]

        # Verify specific entries
        cern_entry = [e for e in result if e[0] == "CERN"][0]
        assert cern_entry[1] == "Active"

        desy_entry = [e for e in result if e[0] == "DESY"][0]
        assert desy_entry[1] == "Banned"
