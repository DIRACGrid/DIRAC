""" tests for the JobDB module """

# pylint: disable=protected-access, invalid-name

from unittest.mock import MagicMock, patch

import pytest

from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


@pytest.fixture(name="jobDB")
def fixturejobDB():
    """Fixture for the JobDB class"""
    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB.__init__", return_value=None):
        jobDB = JobDB()

    jobDB.log = MagicMock()
    jobDB.logger = MagicMock()
    jobDB._connected = True

    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.getVOForGroup", MagicMock(return_value="vo")):
        yield jobDB


def test_getInputData(jobDB: JobDB):
    """Test the getInputData method from JobDB"""
    # Arrange
    jobDB._escapeString = MagicMock(return_value=S_OK())
    jobDB._query = MagicMock(return_value=S_OK([(1234, "/vo/user/lfn1"), (1234, "LFN:/vo/user/lfn2")]))

    # Act
    res = jobDB.getInputData(1234)

    # Assert
    assert res["OK"], res["Message"]
    assert res["Value"] == ["/vo/user/lfn1", "/vo/user/lfn2"]
