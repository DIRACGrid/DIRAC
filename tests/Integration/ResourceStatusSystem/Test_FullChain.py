""" Starting from the PDP object, it makes the full chain of policies

    It requires

    ResourceStatus
    {
      Policies
      {
        AlwaysActiveForResource
        {
          matchParams
          {
            element = Resource
          }
          policyType = AlwaysActive
        }
        AlwaysBannedForSE1SE2
        {
          matchParams
          {
            name = SE1,SE2
          }
          policyType = AlwaysBanned
        }
        AlwaysBannedForSite
        {
          matchParams
          {
            element = Site
          }
          policyType = AlwaysBanned
        }
        AlwaysBannedForSite2
        {
          matchParams
          {
            element = Site
            domain = test
          }
          policyType = AlwaysBanned
        }
      }
      PolicyActions
      {
        LogStatusAction
        {
        }
        LogPolicyResultAction
        {
        }
      }
    }

"""
# pylint: disable=missing-docstring,wrong-import-position


import pytest
import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP

gLogger.setLevel("DEBUG")
pdp = PDP()


def test_takeDecision_noDecisionParams():
    # Arrange
    pdp.setup()

    # Act
    result = pdp.takeDecision()

    # Assert
    assert result["OK"] is True, result["Message"]


@pytest.mark.parametrize(
    "decisionParams, status",
    [
        (
            {
                "element": "Site",
                "name": "Site1",
                "elementType": None,
                "statusType": "ReadAccess",
                "status": "Active",
                "reason": None,
                "tokenOwner": None,
            },
            "Banned",
        ),
        (
            {
                "element": "Site",
                "name": "Site2",
                "elementType": "CE",
                "statusType": "ReadAccess",
                "status": "Active",
                "domain": "test",
                "reason": None,
                "tokenOwner": None,
            },
            "Banned",
        ),
        (
            {
                "element": "Resource",
                "name": "mySE",
                "elementType": "StorageElement",
                "statusType": "ReadAccess",
                "status": "Active",
                "reason": None,
                "tokenOwner": None,
            },
            "Active",
        ),
        (
            {
                "element": "Resource",
                "name": "SE1",
                "elementType": "StorageElement",
                "statusType": "ReadAccess",
                "status": "Active",
                "reason": None,
                "tokenOwner": None,
            },
            "Banned",
        ),
    ],
)
def test_takeDecision_decisionParams(decisionParams, status):
    # Arrange
    pdp.setup(decisionParams)

    # Act
    res = pdp.takeDecision()

    # Assert
    assert res["OK"] is True, res["Message"]
    assert res["Value"]["policyCombinedResult"]["Status"] == status
