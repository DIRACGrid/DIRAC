""" tests for the JobDB module """

# pylint: disable=protected-access, missing-docstring

from mock import MagicMock, patch
import pytest

from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


@pytest.fixture(name="jobDB")
def fixtureJobDB():
    def __init__(self):
        self.log = MagicMock()
        self.logger = MagicMock()
        self._connected = True

    with patch.object(JobDB, "__init__", __init__):
        with patch.object(JobDB, "_escapeString", MagicMock(return_value=S_OK())):
            yield JobDB()


def test_getInputData(jobDB):
    with patch.object(jobDB, "_query", MagicMock(return_value=S_OK((("/vo/user/lfn1",), ("LFN:/vo/user/lfn2",))))):
        result = jobDB.getInputData(1234)
        assert result["OK"] is True
        assert result["Value"] == ["/vo/user/lfn1", "/vo/user/lfn2"]
