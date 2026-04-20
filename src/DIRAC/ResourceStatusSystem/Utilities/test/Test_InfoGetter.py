"""Tests for InfoGetter module

Tests cover:
- ``getPoliciesThatApply``: CS matching, policyType enforcement, arg defaults,
  per-policy arg overrides (key normalisation + type casting), and
  command-args sections (no policyType) being skipped.
- ``postProcessingPolicyList``: single policy, no duplicates, specificity
  disambiguation when multiple policies of the same type apply.
"""

from unittest.mock import patch

import pytest

from DIRAC import S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import getPoliciesThatApply, postProcessingPolicyList

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

# Minimal CS-style policies tree reused across tests.
# Values are lists (as returned by getCSTree).
_BASE_POLICIES = {
    # Command-args defaults section — no policyType, must be skipped.
    "FreeDiskSpace": {
        "Unit": ["TB"],
        "Banned_threshold": ["0.1"],
        "Degraded_threshold": ["5"],
    },
    # Matches all Resources of type StorageElement with WriteAccess.
    "SEWriteAccessFreeDiskSpace": {
        "policyType": ["FreeDiskSpace"],
        "matchParams": {
            "element": ["Resource"],
            "elementType": ["StorageElement"],
            "statusType": ["WriteAccess"],
        },
    },
    # Matches only SE1 — overrides Unit and Banned_threshold.
    "SpecificFreeDiskSpace": {
        "policyType": ["FreeDiskSpace"],
        "Unit": ["GB"],
        "Banned_threshold": ["15"],
        "matchParams": {"name": ["SE1"]},
    },
    # Matches all Sites.
    "AlwaysBannedForSite": {
        "policyType": ["AlwaysBanned"],
        "matchParams": {"element": ["Site"]},
    },
}

_GET_POLICIES = "DIRAC.ResourceStatusSystem.Utilities.RssConfiguration.getPolicies"

_SE1_WRITEACCESS = {
    "element": "Resource",
    "name": "SE1",
    "elementType": "StorageElement",
    "statusType": "WriteAccess",
    "status": "Active",
    "reason": None,
    "tokenOwner": None,
    "active": "Active",
}

_SE2_WRITEACCESS = {
    "element": "Resource",
    "name": "SE2",
    "elementType": "StorageElement",
    "statusType": "WriteAccess",
    "status": "Active",
    "reason": None,
    "tokenOwner": None,
    "active": "Active",
}

_SITE1 = {
    "element": "Site",
    "name": "Site1",
    "elementType": None,
    "statusType": "ReadAccess",
    "status": "Active",
    "reason": None,
    "tokenOwner": None,
    "active": "Active",
}

# ---------------------------------------------------------------------------
# getPoliciesThatApply — basic CS behaviour
# ---------------------------------------------------------------------------


@patch(_GET_POLICIES, return_value=S_OK({}))
def test_no_policies_in_cs(_mock):
    """Empty CS → empty result."""
    result = getPoliciesThatApply(_SE1_WRITEACCESS)
    assert result["OK"]
    assert result["Value"] == []


@patch(_GET_POLICIES, return_value=S_ERROR("CS unavailable"))
def test_cs_error_is_propagated(_mock):
    """CS failure → S_ERROR propagated."""
    result = getPoliciesThatApply(_SE1_WRITEACCESS)
    assert not result["OK"]


@pytest.mark.parametrize(
    "decisionParams, expected_names, unexpected_names",
    [
        pytest.param(
            _SE1_WRITEACCESS,
            [],
            ["FreeDiskSpace"],
            id="command-args-section-skipped",
        ),
        pytest.param(
            _SE1_WRITEACCESS,
            [],
            ["AlwaysBannedForSite"],
            id="site-policy-does-not-match-resource",
        ),
        pytest.param(
            _SITE1,
            ["AlwaysBannedForSite"],
            [],
            id="site-policy-matches-site",
        ),
    ],
)
@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_policy_matching(_mock, decisionParams, expected_names, unexpected_names):
    """Policies are included or excluded based on element matching and policyType presence."""
    result = getPoliciesThatApply(decisionParams)
    assert result["OK"]
    names = [p["name"] for p in result["Value"]]
    for name in expected_names:
        assert name in names
    for name in unexpected_names:
        assert name not in names


def test_unknown_policytype_in_policiesmeta_is_skipped():
    """A CS policy whose policyType has no entry in POLICIESMETA is silently skipped."""
    policies = {
        "GhostPolicy": {
            "policyType": ["NonExistentPolicyType"],
            "matchParams": {"element": ["Resource"]},
        }
    }
    with patch(_GET_POLICIES, return_value=S_OK(policies)):
        result = getPoliciesThatApply(_SE1_WRITEACCESS)
    assert result["OK"]
    assert result["Value"] == []


# ---------------------------------------------------------------------------
# getPoliciesThatApply — arg defaults and per-policy overrides
# ---------------------------------------------------------------------------


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_se2_gets_default_args(_mock):
    """SE2 matches only SEWriteAccessFreeDiskSpace → gets POLICIESMETA default args."""
    result = getPoliciesThatApply(_SE2_WRITEACCESS)
    assert result["OK"]
    assert len(result["Value"]) == 1
    args = result["Value"][0]["args"]
    assert result["Value"][0]["name"] == "SEWriteAccessFreeDiskSpace"
    assert args["unit"] == "TB"
    assert args["Banned_threshold"] == 0.1
    assert args["Degraded_threshold"] == 5


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_se1_specific_policy_wins_with_overridden_args(_mock):
    """SE1 WriteAccess: SpecificFreeDiskSpace wins; overridden args applied, default kept."""
    result = getPoliciesThatApply(_SE1_WRITEACCESS)
    assert result["OK"]
    assert len(result["Value"]) == 1
    policy = result["Value"][0]
    assert policy["name"] == "SpecificFreeDiskSpace"
    assert policy["args"]["unit"] == "GB"  # overridden, key-normalised from "Unit"
    assert policy["args"]["Banned_threshold"] == 15.0  # overridden, cast from str to float
    assert policy["args"]["Degraded_threshold"] == 5  # not overridden → POLICIESMETA default


@pytest.mark.parametrize(
    "expected_key, unexpected_key, expected_value",
    [
        pytest.param("unit", "Unit", "GB", id="Unit-normalised-to-unit"),
    ],
)
@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_arg_override_key_normalisation(_mock, expected_key, unexpected_key, expected_value):
    """CS key 'Unit' (capital) must override POLICIESMETA key 'unit' (lowercase), not add a duplicate."""
    result = getPoliciesThatApply(_SE1_WRITEACCESS)
    assert result["OK"]
    args = result["Value"][0]["args"]
    assert unexpected_key not in args
    assert args[expected_key] == expected_value


@pytest.mark.parametrize(
    "arg_key, expected_type",
    [
        pytest.param("Banned_threshold", float, id="Banned_threshold-cast-to-float"),
        pytest.param("Degraded_threshold", int, id="Degraded_threshold-remains-int"),
    ],
)
@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_arg_override_type_casting(_mock, arg_key, expected_type):
    """CS values are strings; they must be cast to the type of the POLICIESMETA default."""
    result = getPoliciesThatApply(_SE1_WRITEACCESS)
    assert result["OK"]
    assert isinstance(result["Value"][0]["args"][arg_key], expected_type)


# ---------------------------------------------------------------------------
# postProcessingPolicyList
# ---------------------------------------------------------------------------


def _entry(name, policyType, configParams=None, matchParams=None):
    """Build a 4-tuple as produced by getPoliciesThatApply's inner loop."""
    return (name, policyType, configParams or {}, matchParams or {})


_GENERAL = _entry(
    "SEWriteAccessFreeDiskSpace",
    "FreeDiskSpace",
    matchParams={"element": ["Resource"], "elementType": ["StorageElement"], "statusType": ["WriteAccess"]},
)
_SPECIFIC = _entry(
    "SpecificFreeDiskSpace",
    "FreeDiskSpace",
    configParams={"unit": "GB", "Banned_threshold": 15.0},
    matchParams={"name": ["SE1"]},
)
_NARROW = _entry(
    "NarrowPolicy",
    "FreeDiskSpace",
    matchParams={"element": ["Resource"], "elementType": ["StorageElement"], "statusType": ["WriteAccess"]},
)
_BROAD = _entry(
    "BroadPolicy",
    "FreeDiskSpace",
    matchParams={"element": ["Resource"]},
)
_DOWNTIME = _entry("SiteDowntime", "Downtime", matchParams={"element": ["Site"]})


@pytest.mark.parametrize(
    "entries, expected_count, expected_winner",
    [
        pytest.param(
            [_entry("A", "FreeDiskSpace", matchParams={"element": ["Resource"]})],
            1,
            "A",
            id="single-policy-unchanged",
        ),
        pytest.param(
            [_GENERAL, _DOWNTIME],
            2,
            None,  # both kept — no winner check needed
            id="different-types-both-kept",
        ),
        pytest.param(
            [_GENERAL, _SPECIFIC],
            1,
            "SpecificFreeDiskSpace",
            id="name-match-beats-broader-match",
        ),
        pytest.param(
            [_SPECIFIC, _GENERAL],  # reversed order
            1,
            "SpecificFreeDiskSpace",
            id="name-match-beats-broader-match-reversed",
        ),
        pytest.param(
            [_BROAD, _NARROW],
            1,
            "NarrowPolicy",
            id="more-matchparams-beats-fewer",
        ),
        pytest.param(
            [_NARROW, _BROAD],  # reversed order
            1,
            "NarrowPolicy",
            id="more-matchparams-beats-fewer-reversed",
        ),
    ],
)
def test_postprocessing(entries, expected_count, expected_winner):
    """postProcessingPolicyList keeps the correct number of policies and the right winner."""
    result = postProcessingPolicyList(entries)
    assert len(result) == expected_count
    if expected_winner is not None:
        assert result[0][0] == expected_winner
