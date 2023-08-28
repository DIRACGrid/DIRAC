""" Test IdProvider Factory"""
import pytest

from DIRAC.Resources.IdProvider.CheckInIdProvider import CheckInIdProvider
from DIRAC.Resources.IdProvider.IAMIdProvider import IAMIdProvider
from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider
from DIRAC.Resources.IdProvider.tests.IdProviderTestUtilities import setupConfig


config = """
Registry
{
  Groups
  {
    dirac_admin
    {
      Users = alice, bob
      Properties = NormalUser
      IdPRole = wlcg.groups:/dirac/admin
    }
    dirac_pilot
    {
      Users = pilot
      Properties = GenericPilot,LimitedDelegation
      IdPRole = eduperson_entitlement?value=urn:mace:egi.eu:group:checkin-integration:role=member#aai.egi.eu
    }
    dirac_prod
    {
      Users = alice
      Properties = NormalUser,ProductionManagement
      VO = dirac
    }
    dirac_user
    {
      Users = alice, bob, charlie
      Properties = NormalUser
      VO = otherdirac
      IdPRole = wlcg.groups:/dirac/usertest
    }
    test_user
    {
      Users = alice, bob, charlie
      Properties = NormalUser
      VO = anotherVO
    }
    testUser
    {
      Users = alice, bob, charlie
      Properties = NormalUser
      VO = anotherVO
    }
  }
}
"""


@pytest.mark.parametrize(
    "idProviderType, group, expectedValue",
    [
        # OAuth2
        # IdPRole is defined, no VO parameter: IdPRole is used
        (OAuth2IdProvider, "dirac_admin", ["wlcg.groups:/dirac/admin"]),
        # IdPRole is defined, no VO parameter: IdPRole is used
        (
            OAuth2IdProvider,
            "dirac_pilot",
            ["eduperson_entitlement?value=urn:mace:egi.eu:group:checkin-integration:role=member#aai.egi.eu"],
        ),
        # No IdProle, VO parameter is defined: VO + last part of group name is used
        (OAuth2IdProvider, "dirac_prod", []),
        # Both VO and IdPRole are defined: IdPRole is preferred
        (OAuth2IdProvider, "dirac_user", ["wlcg.groups:/dirac/usertest"]),
        # No IdPRole, VO is defined but different from the group name: VO + last part of group name is used
        (OAuth2IdProvider, "test_user", []),
        # No IdPRole, VO is defined but group name is not formed as <group>_<subgroup>
        (OAuth2IdProvider, "testUser", []),
        # Group does not exists
        (OAuth2IdProvider, "doesnot_exist", []),
        # IAM
        # IdPRole is defined, no VO parameter: IdPRole is used
        (IAMIdProvider, "dirac_admin", ["wlcg.groups:/dirac/admin"]),
        # IdPRole is defined, no VO parameter: IdPRole is used
        (
            IAMIdProvider,
            "dirac_pilot",
            ["eduperson_entitlement?value=urn:mace:egi.eu:group:checkin-integration:role=member#aai.egi.eu"],
        ),
        # No IdProle, VO parameter is defined: VO + last part of group name is used
        (IAMIdProvider, "dirac_prod", ["wlcg.groups:/dirac/prod"]),
        # Both VO and IdPRole are defined: IdPRole is preferred
        (IAMIdProvider, "dirac_user", ["wlcg.groups:/dirac/usertest"]),
        # No IdPRole, VO is defined but different from the group name: VO + last part of group name is used
        (IAMIdProvider, "test_user", ["wlcg.groups:/anotherVO/user"]),
        # No IdPRole, VO is defined but group name is not formed as <group>_<subgroup>
        (IAMIdProvider, "testUser", []),
        # Group does not exists
        (IAMIdProvider, "doesnot_exist", []),
        # CheckIn
        # IdPRole is defined, no VO parameter: IdPRole is used
        (CheckInIdProvider, "dirac_admin", ["wlcg.groups:/dirac/admin"]),
        # IdPRole is defined, no VO parameter: IdPRole is used
        (
            CheckInIdProvider,
            "dirac_pilot",
            ["eduperson_entitlement?value=urn:mace:egi.eu:group:checkin-integration:role=member#aai.egi.eu"],
        ),
        # No IdProle, VO parameter is defined: VO + last part of group name is used
        (
            CheckInIdProvider,
            "dirac_prod",
            ["eduperson_entitlement?value=urn:mace:egi.eu:group:dirac:role=prod#aai.egi.eu"],
        ),
        # Both VO and IdPRole are defined: IdPRole is preferred
        (CheckInIdProvider, "dirac_user", ["wlcg.groups:/dirac/usertest"]),
        # No IdPRole, VO is defined but different from the group name: VO + last part of group name is used
        (
            CheckInIdProvider,
            "test_user",
            ["eduperson_entitlement?value=urn:mace:egi.eu:group:anotherVO:role=user#aai.egi.eu"],
        ),
        # No IdPRole, VO is defined but group name is not formed as <group>_<subgroup>
        (CheckInIdProvider, "testUser", []),
        # Group does not exist
        (CheckInIdProvider, "doesnot_exist", []),
    ],
)
def test_getGroupScopes(idProviderType, group, expectedValue):
    """Test getGroupScopes"""
    setupConfig(config)
    idProvider = idProviderType()
    result = idProvider.getGroupScopes(group)
    assert result == expectedValue


@pytest.mark.parametrize(
    "idProviderType, scope, expectedValue",
    [
        # OAuth2
        # Normal cases
        (OAuth2IdProvider, "wlcg.groups:/dirac/admin", ["dirac_admin"]),
        (
            OAuth2IdProvider,
            "eduperson_entitlement?value=urn:mace:egi.eu:group:checkin-integration:role=member#aai.egi.eu",
            ["dirac_pilot"],
        ),
        # Scope does not exist
        (OAuth2IdProvider, "wlcg.groups:/dirac/prod", []),
    ],
)
def test_getScopeGroups(idProviderType, scope, expectedValue):
    """Test getScopeGroups"""
    setupConfig(config)
    idProvider = idProviderType()
    result = idProvider.getScopeGroups(scope)
    assert result == expectedValue
