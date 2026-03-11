"""Tests for CSHelpers module"""

from unittest.mock import patch, MagicMock

from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities.CSHelpers import getSiteElements


@patch("DIRAC.ResourceStatusSystem.Utilities.CSHelpers.getCESiteMapping")
@patch("DIRAC.ResourceStatusSystem.Utilities.CSHelpers.DMSHelpers")
def test_getSiteElements_returns_ses_and_ces(mock_dms_helpers, mock_get_ce_site_mapping):
    """Test that getSiteElements returns both StorageElements and ComputingElements"""
    mock_dms_instance = MagicMock()
    mock_dms_instance.getSiteSEMapping.return_value = S_OK(({}, {"LCG.CERN.cern": ["CERN-SE1", "CERN-SE2"]}, {}))
    mock_dms_helpers.return_value = mock_dms_instance

    mock_get_ce_site_mapping.return_value = S_OK(
        {"ce1.cern.ch": "LCG.CERN.cern", "ce2.cern.ch": "LCG.CERN.cern", "ce3.other.ch": "LCG.Other.cern"}
    )

    result = getSiteElements("LCG.CERN.cern")
    assert result["OK"]
    assert sorted(result["Value"]) == sorted(["CERN-SE1", "CERN-SE2", "ce1.cern.ch", "ce2.cern.ch"])


@patch("DIRAC.ResourceStatusSystem.Utilities.CSHelpers.getCESiteMapping")
@patch("DIRAC.ResourceStatusSystem.Utilities.CSHelpers.DMSHelpers")
def test_getSiteElements_site_with_only_ces(mock_dms_helpers, mock_get_ce_site_mapping):
    """Test that a site with only CEs (no SEs) still returns elements"""
    mock_dms_instance = MagicMock()
    mock_dms_instance.getSiteSEMapping.return_value = S_OK(({}, {}, {}))
    mock_dms_helpers.return_value = mock_dms_instance

    mock_get_ce_site_mapping.return_value = S_OK({"ce1.krakow.pl": "LCG.Krakow.pl"})

    result = getSiteElements("LCG.Krakow.pl")
    assert result["OK"]
    assert result["Value"] == ["ce1.krakow.pl"]


@patch("DIRAC.ResourceStatusSystem.Utilities.CSHelpers.getCESiteMapping")
@patch("DIRAC.ResourceStatusSystem.Utilities.CSHelpers.DMSHelpers")
def test_getSiteElements_ce_mapping_fails(mock_dms_helpers, mock_get_ce_site_mapping):
    """Test that if CE mapping fails, error is propagated"""
    mock_dms_instance = MagicMock()
    mock_dms_instance.getSiteSEMapping.return_value = S_OK(({}, {"LCG.CERN.cern": ["CERN-SE1"]}, {}))
    mock_dms_helpers.return_value = mock_dms_instance

    mock_get_ce_site_mapping.return_value = S_ERROR("Failed to get CE mapping")

    result = getSiteElements("LCG.CERN.cern")
    assert not result["OK"]


@patch("DIRAC.ResourceStatusSystem.Utilities.CSHelpers.getCESiteMapping")
@patch("DIRAC.ResourceStatusSystem.Utilities.CSHelpers.DMSHelpers")
def test_getSiteElements_se_mapping_fails(mock_dms_helpers, mock_get_ce_site_mapping):
    """Test that if SE mapping fails, error is propagated"""
    mock_dms_instance = MagicMock()
    mock_dms_instance.getSiteSEMapping.return_value = S_ERROR("Failed to get SE mapping")
    mock_dms_helpers.return_value = mock_dms_instance

    result = getSiteElements("LCG.CERN.cern")
    assert not result["OK"]
    mock_get_ce_site_mapping.assert_not_called()
