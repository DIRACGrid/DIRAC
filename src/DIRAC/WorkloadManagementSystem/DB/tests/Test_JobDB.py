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


def test_getInputData_bulk_with_duplicates(jobDB: JobDB):
    """Test getInputData with a list of jobIDs containing duplicates"""
    # Arrange
    jobDB._query = MagicMock(return_value=S_OK([(1234, "/vo/user/lfn1"), (5678, "LFN:/vo/user/lfn2")]))

    # Act
    res = jobDB.getInputData([1234, 1234, 5678])

    # Assert
    assert res["OK"], res["Message"]
    assert res["Value"] == {1234: ["/vo/user/lfn1"], 5678: ["/vo/user/lfn2"]}
    # The number of SQL placeholders must match the number of bound arguments
    assert jobDB._query.call_args.args[0].count("%s") == len(jobDB._query.call_args.kwargs["args"])
