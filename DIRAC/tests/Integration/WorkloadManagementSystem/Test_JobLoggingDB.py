""" This test only need the JobLoggingDB to be present
"""
# pylint: disable=wrong-import-position, missing-docstring

import datetime

import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB


@pytest.fixture(name="jobLoggingDB")
def fixtureJobLoggingDB():
    yield JobLoggingDB()


def test_JobStatus(jobLoggingDB: JobLoggingDB):
    result = jobLoggingDB.addLoggingRecord(
        1,
        status="testing",
        minorStatus="date=datetime.datetime.utcnow()",
        date=datetime.datetime.utcnow(),
        source="Unittest",
    )
    assert result["OK"] is True, result["Message"]

    date = "2006-04-25 14:20:17"
    result = jobLoggingDB.addLoggingRecord(
        1, status="testing", minorStatus="2006-04-25 14:20:17", date=date, source="Unittest"
    )
    assert result["OK"] is True, result["Message"]

    result = jobLoggingDB.addLoggingRecord(1, status="testing", minorStatus="No date 1", source="Unittest")
    assert result["OK"] is True, result["Message"]

    result = jobLoggingDB.addLoggingRecord(1, status="testing", minorStatus="No date 2", source="Unittest")
    assert result["OK"] is True, result["Message"]

    result = jobLoggingDB.getJobLoggingInfo(1)
    assert result["OK"] is True, result["Message"]

    result = jobLoggingDB.getWMSTimeStamps(1)
    assert result["OK"] is True, result["Message"]

    jobLoggingDB.deleteJob(1)
