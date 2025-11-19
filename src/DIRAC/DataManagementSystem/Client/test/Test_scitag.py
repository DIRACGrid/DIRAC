from unittest.mock import Mock, patch

import pytest

from DIRAC.DataManagementSystem.Client.FTS3Job import get_scitag


class TestGetScitag:
    def test_valid_vo_and_activity(self):
        """Test get_scitag with valid VO and activity."""
        result = get_scitag("atlas", "Analysis Input")
        expected = 2 << 6 | 17  # atlas expId=2, analysis activityId=17
        assert result == expected

    def test_valid_vo_no_activity(self):
        """Test get_scitag with valid VO but no specific activity (should use default)."""
        result = get_scitag("cms")
        expected = 3 << 6 | 1  # cms expId=200, default activityId=1
        assert result == expected

    def test_invalid_vo(self):
        """Test get_scitag with invalid VO (should use default vo_id=1)."""
        result = get_scitag("nonexistent")
        expected = 1 << 6 | 1  # default vo_id=1, default activity_id=1
        assert result == expected

    def test_valid_vo_invalid_activity(self):
        """Test get_scitag with valid VO but invalid activity."""
        result = get_scitag("atlas", "nonexistent_activity")
        expected = 2 << 6 | 1  # atlas expId=2, default activity_id=1
        assert result == expected

    def test_case_insensitive_vo(self):
        """Test that VO matching is case insensitive."""
        result = get_scitag("ATLAS", "Data Brokering")
        expected = 2 << 6 | 3  # atlas expId=2, production activityId=3
        assert result == expected


@pytest.mark.parametrize(
    "vo,activity,expected_vo_id,expected_activity_id",
    [
        ("atlas", "Analysis Output", 2, 18),
        ("atlas", "Debug", 2, 9),
        ("cms", "Cache", 3, 3),
        ("cms", "default", 3, 1),
        ("nonexistent", "any", 1, 1),  # defaults
        ("atlas", "nonexistent", 2, 1),  # valid vo, invalid activity
    ],
)
def test_parametrized_scenarios(vo, activity, expected_vo_id, expected_activity_id):
    """Parametrized test for various VO and activity combinations."""
    result = get_scitag(vo, activity)
    expected = expected_vo_id << 6 | expected_activity_id
    assert result == expected


@pytest.mark.parametrize(
    "vo,expected_result",
    [
        ("atlas", 2 << 6 | 1),  # Should use default activity
        ("cms", 3 << 6 | 1),  # Should use default activity
        ("unknown", 1 << 6 | 1),  # Should use all defaults
    ],
)
def test_no_activity_parameter(vo, expected_result):
    """Test behavior when no activity parameter is provided."""
    result = get_scitag(vo)
    assert result == expected_result
