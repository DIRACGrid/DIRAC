import math
from typing import Any

import pytest
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TQ_MIN_SHARE, calculate_priority


@pytest.mark.parametrize("allow_bg_tqs", [True, False])
@pytest.mark.parametrize("share", [0.5, 1.0, 2.0])
def test_calculate_priority_empty_entry(share: float, allow_bg_tqs: bool) -> None:
    """test of the calculate_priority function"""
    # Arrange
    tq_dict: dict[int, float] = {}
    all_tqs_data: dict[int, dict[str, Any]] = {}

    # Act
    result = calculate_priority(tq_dict, all_tqs_data, share, allow_bg_tqs)

    # Assert
    assert isinstance(result, dict)
    assert len(result.keys()) == 0


@pytest.mark.parametrize("allow_bg_tqs", [True, False])
@pytest.mark.parametrize("share", [0.5, 1.0, 2.0])
def test_calculate_priority_different_priority_same_number_of_jobs(share: float, allow_bg_tqs: bool) -> None:
    """test of the calculate_priority function"""
    # Arrange
    tq_dict: dict[int, float] = {
        1: 3.0,
        2: 2.0,
        3: 0.3,
    }
    all_tqs_data: dict[int, dict[str, Any]] = {
        1: {
            "Priority": 3.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 50000,
        },
        2: {
            "Priority": 2.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 50000,
        },
        3: {
            "Priority": 1.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 50000,
        },
    }

    # Act
    result = calculate_priority(tq_dict, all_tqs_data, share, allow_bg_tqs)

    # Assert
    assert isinstance(result, dict)
    assert len(result.keys()) == 1
    key, value = result.popitem()
    assert key == pytest.approx(share)
    assert value == [1, 2, 3]
    assert all(prio >= TQ_MIN_SHARE for prio in result.keys())


@pytest.mark.parametrize("allow_bg_tqs", [True, False])
@pytest.mark.parametrize("share", [0.5, 1.0, 2.0])
def test_calculate_priority_same_cpu_time(share: float, allow_bg_tqs: bool) -> None:
    """test of the calculate_priority function"""
    # Arrange

    # NOTE: the priority value from the tq_dict is not used in the calculation
    # because all task queues end up in the same "priority group" let's say
    tq_dict: dict[int, float] = {
        1: 3.0,
        2: 2.0,
        3: 0.3,
    }
    all_tqs_data: dict[int, dict[str, Any]] = {
        1: {
            "Priority": 1.0,
            "Jobs": 100,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 50000,
        },
        2: {
            "Priority": 2.0,
            "Jobs": 14,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 50000,
        },
        3: {
            "Priority": 1.0,
            "Jobs": 154,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 50000,
        },
    }

    # Act
    result = calculate_priority(tq_dict, all_tqs_data, share, allow_bg_tqs)

    # Assert
    # All the tqs are supporsed to be regrouped in the same priority group
    # even though they have different priority values (same cpu time)
    assert isinstance(result, dict)
    assert len(result.keys()) == 1
    priority = set(result.keys()).pop()
    assert priority == pytest.approx(share)
    assert result[priority] == [1, 2, 3]
    assert all(prio >= TQ_MIN_SHARE for prio in result.keys())


@pytest.mark.parametrize("allow_bg_tqs", [True, False])
@pytest.mark.parametrize("share", [0.5, 1.0, 2.0])
def test_calculate_priority_different_cpu_time(share: float, allow_bg_tqs: bool) -> None:
    """test of the calculate_priority function"""
    # Arrange
    tq_dict: dict[int, float] = {
        1: 1.0,
        2: 1.0,
        3: 1.0,
    }
    all_tqs_data: dict[int, dict[str, Any]] = {
        1: {
            "Priority": 1.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 150000,
        },
        2: {
            "Priority": 1.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 100000,
        },
        3: {
            "Priority": 1.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 50000,
        },
    }

    # Act
    result = calculate_priority(tq_dict, all_tqs_data, share, allow_bg_tqs)

    # Assert
    assert isinstance(result, dict)
    assert len(result.keys()) == 1
    priority = set(result.keys()).pop()
    assert priority == pytest.approx(share / 3)  # different group category
    assert result[priority] == [1, 2, 3]


@pytest.mark.parametrize("allow_bg_tqs", [True, False])
@pytest.mark.parametrize("share", [0.5, 1.0, 2.0])
def test_calculate_priority_different_priority_different_number_of_jobs_different_cpu_time(
    share: float, allow_bg_tqs: bool
) -> None:
    """test of the calculate_priority function"""
    # Arrange
    tq_dict: dict[int, float] = {
        1: 5.0,
        2: 3.0,
        3: 2.0,
    }
    all_tqs_data: dict[int, dict[str, Any]] = {
        1: {
            "Priority": 1.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 150000,
        },
        2: {
            "Priority": 1.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 100000,
        },
        3: {
            "Priority": 1.0,
            "Jobs": 1,
            "Owner": "userName",
            "OwnerGroup": "myGroup",
            "CPUTime": 50000,
        },
    }

    # Act
    result = calculate_priority(tq_dict, all_tqs_data, share, allow_bg_tqs)

    # Assert
    assert isinstance(result, dict)
    assert sum(result.keys()) == pytest.approx(share)
    assert len(result.keys()) == 3

    for priority in result.keys():
        # assert that each key is in the following list at maximum epsilon distance
        delta = math.inf
        for expected_priority in [share * 0.5, share * 0.3, share * 0.2]:
            delta = min(delta, abs(priority - expected_priority))
        assert delta < 1e-6
        assert len(result[priority]) == 1
