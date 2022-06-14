# pylint: disable=missing-docstring, invalid-name

from unittest.mock import MagicMock
import pytest

from DIRAC import gLogger

from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus

# sut
from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import JobStatusUtility

gLogger.setLevel("DEBUG")


@pytest.mark.parametrize(
    "statusDict_in, "
    + "jobDB_getJobAttributes_rv, jobDB_setJobAttributes_rv, jobLoggingDB_getWMSTimeStamps_rv, force, "
    + "resExpected, resExpected_value",
    [
        ({}, {"OK": False}, {"OK": False}, {"OK": False}, False, False, None),
        ({}, {"OK": True, "Value": None}, {"OK": False}, {"OK": False}, False, False, None),
        ({}, {"OK": True, "Value": {"Status": JobStatus.WAITING}}, {"OK": False}, {"OK": False}, False, False, None),
        (
            {},
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": False},
            {"OK": True, "Value": {}},
            False,
            False,
            None,
        ),
        (
            {"2002-01-01 00:00:00": {"Status": JobStatus.MATCHED}},
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": False},
            {
                "OK": True,
                "Value": {
                    JobStatus.RECEIVED: "1000000001.001",
                    JobStatus.CHECKING: "1000000002.002",
                    JobStatus.WAITING: "1000000003.003",
                    "LastTime": "2001-09-09 03:46:43",
                },
            },
            False,
            False,
            None,
        ),
        (
            {"2002-01-01 00:00:00": {"Status": JobStatus.MATCHED}},
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": True},
            {
                "OK": True,
                "Value": {
                    JobStatus.RECEIVED: "1000000001.001",
                    JobStatus.CHECKING: "1000000002.002",
                    JobStatus.WAITING: "1000000003.003",
                    "LastTime": "2001-09-09 03:46:43",
                },
            },
            False,
            True,
            (["Status"], [JobStatus.MATCHED]),
        ),
        (
            {
                "2002-01-01 00:00:00": {"Status": JobStatus.MATCHED},
                "2003-01-01 00:00:00": {"MinorStatus": "some_minor_status"},
                "2004-01-01 00:00:00": {"Status": JobStatus.RUNNING, "ApplicationStatus": "some_app_status"},
                "2005-01-01 00:00:00": {"MinorStatus": JobMinorStatus.APPLICATION},
            },
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": True},
            {
                "OK": True,
                "Value": {
                    JobStatus.RECEIVED: "1000000001.001",
                    JobStatus.CHECKING: "1000000002.002",
                    JobStatus.WAITING: "1000000003.003",
                    "LastTime": "2001-09-09 03:46:43",
                },
            },
            False,
            True,
            (
                ["Status", "MinorStatus", "ApplicationStatus"],
                [JobStatus.RUNNING, JobMinorStatus.APPLICATION, "some_app_status"],
            ),
        ),
        (
            {
                "2002-01-01 00:00:00": {"Status": JobStatus.MATCHED},
                "2003-01-01 00:00:00": {"Status": JobStatus.MATCHED, "MinorStatus": "some_minor_status"},
                "2004-01-01 00:00:00": {"Status": JobStatus.RUNNING, "ApplicationStatus": "some_app_status"},
                "2005-01-01 00:00:00": {"Status": JobStatus.RUNNING, "MinorStatus": JobMinorStatus.APPLICATION},
            },
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": True},
            {
                "OK": True,
                "Value": {
                    JobStatus.RECEIVED: "1000000001.001",
                    JobStatus.CHECKING: "1000000002.002",
                    JobStatus.WAITING: "1000000003.003",
                    "LastTime": "2001-09-09 03:46:43",
                },
            },
            False,
            True,
            (
                ["Status", "MinorStatus", "ApplicationStatus"],
                [JobStatus.RUNNING, JobMinorStatus.APPLICATION, "some_app_status"],
            ),
        ),
        (
            {
                "2002-01-01 00:00:00": {"Status": JobStatus.DONE},  # try inserting a "wrong" one
            },
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": True},
            {
                "OK": True,
                "Value": {
                    JobStatus.RECEIVED: "1000000001.001",
                    JobStatus.CHECKING: "1000000002.002",
                    JobStatus.WAITING: "1000000003.003",
                    "LastTime": "2001-09-09 03:46:43",
                },
            },
            False,
            True,
            (["Status"], [JobStatus.WAITING]),
        ),
        (
            {
                "2002-01-01 00:00:00": {"Status": JobStatus.RUNNING},  # this would trigger a wrong update
                "2003-01-01 00:00:00": {"Status": JobStatus.MATCHED},
            },
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": True},
            {
                "OK": True,
                "Value": {
                    JobStatus.RECEIVED: "1000000001.001",
                    JobStatus.CHECKING: "1000000002.002",
                    JobStatus.WAITING: "1000000003.003",
                    "LastTime": "2001-09-09 03:46:43",
                },
            },
            False,
            True,
            (["Status"], [JobStatus.MATCHED]),
        ),
        (
            {},
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": True},
            {"OK": True, "Value": {}},
            True,
            False,
            None,
        ),
        (
            {"2002-01-01 00:00:00": {"Status": JobStatus.MATCHED}},
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": True},
            {
                "OK": True,
                "Value": {
                    JobStatus.RECEIVED: "1000000001.001",
                    JobStatus.CHECKING: "1000000002.002",
                    JobStatus.WAITING: "1000000003.003",
                    "LastTime": "2001-09-09 03:46:43",
                },
            },
            True,
            True,
            (["Status"], [JobStatus.MATCHED]),
        ),
        (
            {"2002-01-01 00:00:00": {"Status": JobStatus.DONE}},
            {"OK": True, "Value": {"Status": JobStatus.WAITING}},
            {"OK": True},
            {
                "OK": True,
                "Value": {
                    JobStatus.RECEIVED: "1000000001.001",
                    JobStatus.CHECKING: "1000000002.002",
                    JobStatus.WAITING: "1000000003.003",
                    "LastTime": "2001-09-09 03:46:43",
                },
            },
            True,
            True,
            (["Status"], [JobStatus.DONE]),
        ),
    ],
)
def test__setJobStatusBulk(
    statusDict_in,
    jobDB_getJobAttributes_rv,
    jobDB_setJobAttributes_rv,
    jobLoggingDB_getWMSTimeStamps_rv,
    force,
    resExpected,
    resExpected_value,
):
    # Arrange
    jobDB_mock = MagicMock()
    jobDB_mock.getJobAttributes.return_value = jobDB_getJobAttributes_rv
    jobDB_mock.setJobAttributes.return_value = jobDB_setJobAttributes_rv

    jobLoggingDB_mock = MagicMock()
    jobLoggingDB_mock.getWMSTimeStamps.return_value = jobLoggingDB_getWMSTimeStamps_rv

    jsu = JobStatusUtility(jobDB_mock, jobLoggingDB_mock)

    # Act
    res = jsu.setJobStatusBulk(1, statusDict_in, force)

    # Assert
    assert res["OK"] is resExpected
    if res["OK"]:
        assert res["Value"] == resExpected_value
