import json
from pathlib import Path

import pytest

iam_data = json.loads((Path(__file__).parent / "iam_data.json").read_text())
groups_by_name = {g["displayName"]: g for g in iam_data["groups"]}


@pytest.mark.parametrize(
    ("group_name", "role_name"),
    [
        ("ctao.dpps.test/user", "/ctao.dpps.test/Role=user"),
        ("ctao.dpps.test/dpps/user", "/ctao.dpps.test/dpps/Role=user"),
        ("ctao.dpps.test/dpps/pipelines/user", "/ctao.dpps.test/dpps/pipelines/Role=user"),
        ("ctao.dpps.test/dpps/pipelines/manager", "/ctao.dpps.test/dpps/pipelines/Role=manager"),
    ],
)
def test_group_name_to_role_name(group_name, role_name):
    from DIRAC.Core.Security.IAMService import IAMService

    assert IAMService._group_name_to_role_string(group_name) == role_name


@pytest.mark.parametrize(
    ("group", "expected"),
    [
        (groups_by_name["ctao.dpps.test"], False),
        (groups_by_name["ctao.dpps.test/dpps"], False),
        (groups_by_name["ctao.dpps.test/dpps/user"], True),
        (groups_by_name["ctao.dpps.test/dpps/pipelines"], False),
        (groups_by_name["ctao.dpps.test/dpps/pipelines/user"], True),
    ],
)
def test_is_voms_roles(group, expected):
    from DIRAC.Core.Security.IAMService import IAMService

    assert IAMService._is_voms_role(group) == expected


def test_users_to_cs():
    from DIRAC.ConfigurationSystem.Client.Config import gConfig
    from DIRAC.Core.Security.IAMService import IAMService

    vo = "ctao.dpps.test"
    gConfig.setOptionValue("/Resources/IdProviders/dummy/issuer", "https://iam.test.example")
    gConfig.setOptionValue(f"/Registry/VO/{vo}/IdProvider", "dummy")

    iam_service = IAMService(access_token="dummy", vo="ctao.dpps.test")
    # the class uses caching, this prevents it from actually trying to contact an IAM
    iam_service.iam_users_raw = iam_data["users"]
    iam_service.iam_groups_raw = iam_data["groups"]

    result = iam_service.getUsers()
    assert result["OK"]
    value = result["Value"]
    assert len(value["Errors"]) == 6
    assert sum(1 for error in value["Errors"] if "User must have at least one voms role" in error) == 3
    assert sum(1 for error in value["Errors"] if "KeyError('certificates')" in error) == 3
    assert len(value["Users"]) == 1

    dpps_user = value["Users"]["/CN=DPPS User"]
    assert set(dpps_user["Roles"]) == {
        "/ctao.dpps.test/dpps/Role=user",
        "/ctao.dpps.test/dpps/pipelines/Role=user",
        "/ctao.dpps.test/dpps/archive/Role=user",
        "/ctao.dpps.test/dpps/dataquality/Role=user",
    }
