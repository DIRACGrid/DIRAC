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
# Helpers
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


def _se1_writeaccess():
    """decisionParams for SE1 WriteAccess."""
    return {
        "element": "Resource",
        "name": "SE1",
        "elementType": "StorageElement",
        "statusType": "WriteAccess",
        "status": "Active",
        "reason": None,
        "tokenOwner": None,
        "active": "Active",
    }


def _se2_writeaccess():
    """decisionParams for SE2 WriteAccess (not SE1 — no SpecificFreeDiskSpace)."""
    return {
        "element": "Resource",
        "name": "SE2",
        "elementType": "StorageElement",
        "statusType": "WriteAccess",
        "status": "Active",
        "reason": None,
        "tokenOwner": None,
        "active": "Active",
    }


def _site1():
    """decisionParams for a generic Site."""
    return {
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
# getPoliciesThatApply
# ---------------------------------------------------------------------------


@patch(_GET_POLICIES, return_value=S_OK({}))
def test_no_policies_in_cs(_mock):
    """Empty CS → empty result."""
    result = getPoliciesThatApply(_se1_writeaccess())
    assert result["OK"]
    assert result["Value"] == []


@patch(_GET_POLICIES, return_value=S_ERROR("CS unavailable"))
def test_cs_error_is_propagated(_mock):
    """CS failure → S_ERROR propagated."""
    result = getPoliciesThatApply(_se1_writeaccess())
    assert not result["OK"]


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_command_args_section_is_skipped(_mock):
    """A CS entry without policyType (e.g. FreeDiskSpace defaults) is not a policy."""
    result = getPoliciesThatApply(_se1_writeaccess())
    assert result["OK"]
    names = [p["name"] for p in result["Value"]]
    assert "FreeDiskSpace" not in names


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_unmatched_policy_not_included(_mock):
    """AlwaysBannedForSite must not apply to a Resource element."""
    result = getPoliciesThatApply(_se1_writeaccess())
    assert result["OK"]
    names = [p["name"] for p in result["Value"]]
    assert "AlwaysBannedForSite" not in names


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_site_policy_applies_to_site(_mock):
    """AlwaysBannedForSite applies to a Site element."""
    result = getPoliciesThatApply(_site1())
    assert result["OK"]
    names = [p["name"] for p in result["Value"]]
    assert "AlwaysBannedForSite" in names


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_unknown_policytype_in_policiesmeta_is_skipped(_mock):
    """A CS policy whose policyType has no entry in POLICIESMETA is silently skipped."""
    policies = {
        "GhostPolicy": {
            "policyType": ["NonExistentPolicyType"],
            "matchParams": {"element": ["Resource"]},
        }
    }
    with patch(_GET_POLICIES, return_value=S_OK(policies)):
        result = getPoliciesThatApply(_se1_writeaccess())
    assert result["OK"]
    assert result["Value"] == []


# ---------------------------------------------------------------------------
# Arg defaults and per-policy overrides
# ---------------------------------------------------------------------------


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_se2_gets_default_args(_mock):
    """SE2 matches only SEWriteAccessFreeDiskSpace → gets POLICIESMETA default args."""
    result = getPoliciesThatApply(_se2_writeaccess())
    assert result["OK"]
    assert len(result["Value"]) == 1
    policy = result["Value"][0]
    assert policy["name"] == "SEWriteAccessFreeDiskSpace"
    assert policy["args"]["unit"] == "TB"
    assert policy["args"]["Banned_threshold"] == 0.1
    assert policy["args"]["Degraded_threshold"] == 5


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_se1_gets_specific_policy_with_overridden_args(_mock):
    """SE1 WriteAccess: SpecificFreeDiskSpace wins; Unit and Banned_threshold overridden."""
    result = getPoliciesThatApply(_se1_writeaccess())
    assert result["OK"]
    assert len(result["Value"]) == 1
    policy = result["Value"][0]
    assert policy["name"] == "SpecificFreeDiskSpace"
    assert policy["args"]["unit"] == "GB"  # overridden, key-normalised from "Unit"
    assert policy["args"]["Banned_threshold"] == 15.0  # overridden, cast from str "15" to float
    assert policy["args"]["Degraded_threshold"] == 5  # NOT overridden → falls back to default


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_arg_override_key_normalisation(_mock):
    """CS key 'Unit' (capital) must override POLICIESMETA key 'unit' (lowercase)."""
    result = getPoliciesThatApply(_se1_writeaccess())
    assert result["OK"]
    args = result["Value"][0]["args"]
    # 'Unit' from CS must land in 'unit', not create a separate 'Unit' key
    assert "Unit" not in args
    assert args["unit"] == "GB"


@patch(_GET_POLICIES, return_value=S_OK(_BASE_POLICIES))
def test_arg_override_type_casting(_mock):
    """CS values are strings; they must be cast to the type of the POLICIESMETA default."""
    result = getPoliciesThatApply(_se1_writeaccess())
    assert result["OK"]
    args = result["Value"][0]["args"]
    assert isinstance(args["Banned_threshold"], float)
    assert isinstance(args["Degraded_threshold"], int)


# ---------------------------------------------------------------------------
# postProcessingPolicyList
# ---------------------------------------------------------------------------


# Helper to build the 4-tuples expected by postProcessingPolicyList.
def _entry(name, policyType, configParams=None, matchParams=None):
    return (name, policyType, configParams or {}, matchParams or {})


def test_postprocessing_single_policy_unchanged():
    """A single policy is returned as-is."""
    entries = [_entry("A", "FreeDiskSpace", matchParams={"element": ["Resource"]})]
    assert postProcessingPolicyList(entries) == entries


def test_postprocessing_different_types_both_kept():
    """Policies of different types are all kept."""
    entries = [
        _entry("A", "FreeDiskSpace", matchParams={"element": ["Resource"]}),
        _entry("B", "Downtime", matchParams={"element": ["Resource"]}),
    ]
    result = postProcessingPolicyList(entries)
    assert len(result) == 2


def test_postprocessing_name_match_beats_broader_match():
    """When two FreeDiskSpace policies match, the one with 'name' in matchParams wins."""
    general = _entry(
        "SEWriteAccessFreeDiskSpace",
        "FreeDiskSpace",
        matchParams={"element": ["Resource"], "elementType": ["StorageElement"], "statusType": ["WriteAccess"]},
    )
    specific = _entry(
        "SpecificFreeDiskSpace",
        "FreeDiskSpace",
        configParams={"unit": "GB", "Banned_threshold": 15.0},
        matchParams={"name": ["SE1"]},
    )
    result = postProcessingPolicyList([general, specific])
    assert len(result) == 1
    assert result[0][0] == "SpecificFreeDiskSpace"


def test_postprocessing_more_matchparams_beats_fewer():
    """Without a name-match, the policy with more matchParams keys wins."""
    narrow = _entry(
        "NarrowPolicy",
        "FreeDiskSpace",
        matchParams={"element": ["Resource"], "elementType": ["StorageElement"], "statusType": ["WriteAccess"]},
    )
    broad = _entry(
        "BroadPolicy",
        "FreeDiskSpace",
        matchParams={"element": ["Resource"]},
    )
    result = postProcessingPolicyList([broad, narrow])
    assert len(result) == 1
    assert result[0][0] == "NarrowPolicy"


def test_postprocessing_order_independent():
    """Winner is the same regardless of the order policies appear in the input list."""
    general = _entry(
        "SEWriteAccessFreeDiskSpace",
        "FreeDiskSpace",
        matchParams={"element": ["Resource"], "elementType": ["StorageElement"], "statusType": ["WriteAccess"]},
    )
    specific = _entry(
        "SpecificFreeDiskSpace",
        "FreeDiskSpace",
        matchParams={"name": ["SE1"]},
    )
    assert postProcessingPolicyList([general, specific])[0][0] == "SpecificFreeDiskSpace"
    assert postProcessingPolicyList([specific, general])[0][0] == "SpecificFreeDiskSpace"
