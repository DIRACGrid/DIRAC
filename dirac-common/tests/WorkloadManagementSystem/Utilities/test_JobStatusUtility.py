"""Test the JobStatusUtility stateless functions."""

import unittest
from datetime import datetime
from unittest.mock import MagicMock

from DIRACCommon.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRACCommon.WorkloadManagementSystem.Client.JobStatus import WAITING, MATCHED, RUNNING, DONE, FAILED
from DIRACCommon.WorkloadManagementSystem.Utilities.JobStatusUtility import getStartAndEndTime, getNewStatus


class TestJobStatusUtility(unittest.TestCase):
    """Test cases for JobStatusUtility functions"""

    def test_getStartAndEndTime_no_running_status(self):
        """Test getStartAndEndTime when job never reaches running state"""
        startTime = None
        endTime = None
        updateTimes = ["2023-01-01 10:00:00", "2023-01-01 11:00:00"]
        timeStamps = [(1672563600.0, WAITING), (1672567200.0, MATCHED)]
        statusDict = {"2023-01-01 10:00:00": {"Status": WAITING}, "2023-01-01 11:00:00": {"Status": MATCHED}}

        newStartTime, newEndTime = getStartAndEndTime(startTime, endTime, updateTimes, timeStamps, statusDict)

        self.assertIsNone(newStartTime)
        self.assertIsNone(newEndTime)

    def test_getStartAndEndTime_with_running_and_done(self):
        """Test getStartAndEndTime when job runs and completes"""
        startTime = None
        endTime = None
        updateTimes = [
            "2023-01-01 10:00:00",  # WAITING
            "2023-01-01 11:00:00",  # MATCHED
            "2023-01-01 12:00:00",  # RUNNING
            "2023-01-01 13:00:00",  # DONE
        ]
        timeStamps = [(1672563600.0, WAITING), (1672567200.0, MATCHED), (1672570800.0, RUNNING), (1672574400.0, DONE)]
        statusDict = {
            "2023-01-01 10:00:00": {"Status": WAITING},
            "2023-01-01 11:00:00": {"Status": MATCHED},
            "2023-01-01 12:00:00": {"Status": RUNNING},
            "2023-01-01 13:00:00": {"Status": DONE},
        }

        newStartTime, newEndTime = getStartAndEndTime(startTime, endTime, updateTimes, timeStamps, statusDict)

        self.assertEqual(newStartTime, "2023-01-01 12:00:00")  # When it started running
        self.assertEqual(newEndTime, "2023-01-01 13:00:00")  # When it finished

    def test_getStartAndEndTime_existing_start_time(self):
        """Test getStartAndEndTime when startTime already exists"""
        startTime = "2023-01-01 09:00:00"  # Already set
        endTime = None
        updateTimes = ["2023-01-01 12:00:00", "2023-01-01 13:00:00"]
        timeStamps = [(1672570800.0, RUNNING), (1672574400.0, DONE)]
        statusDict = {"2023-01-01 12:00:00": {"Status": RUNNING}, "2023-01-01 13:00:00": {"Status": DONE}}

        newStartTime, newEndTime = getStartAndEndTime(startTime, endTime, updateTimes, timeStamps, statusDict)

        self.assertEqual(newStartTime, "2023-01-01 09:00:00")  # Should keep existing
        self.assertEqual(newEndTime, "2023-01-01 13:00:00")  # Should set end time

    def test_getNewStatus_simple_progression(self):
        """Test getNewStatus with simple status progression"""
        jobID = 123
        updateTimes = [datetime.fromisoformat("2023-01-01 10:00:00")]
        lastTime = datetime.fromisoformat("2023-01-01 09:00:00")
        statusDict = {
            datetime.fromisoformat("2023-01-01 10:00:00"): {
                "Status": MATCHED,
                "MinorStatus": "JobAgent",
                "ApplicationStatus": "Starting",
            }
        }
        currentStatus = WAITING
        force = False

        # Mock logger
        log = MagicMock()
        log.debug = MagicMock()
        log.error = MagicMock()

        result = getNewStatus(jobID, updateTimes, lastTime, statusDict, currentStatus, force, log)

        self.assertTrue(result["OK"])
        status, minor, application = result["Value"]
        self.assertEqual(status, MATCHED)
        self.assertEqual(minor, "JobAgent")
        self.assertEqual(application, "Starting")

    def test_getNewStatus_no_updates_after_last_time(self):
        """Test getNewStatus when no updates after lastTime"""
        jobID = 123
        updateTimes = [datetime.fromisoformat("2023-01-01 08:00:00")]  # Before lastTime
        lastTime = datetime.fromisoformat("2023-01-01 09:00:00")
        statusDict = {datetime.fromisoformat("2023-01-01 08:00:00"): {"Status": WAITING}}
        currentStatus = WAITING
        force = False
        log = MagicMock()

        result = getNewStatus(jobID, updateTimes, lastTime, statusDict, currentStatus, force, log)

        self.assertTrue(result["OK"])
        status, minor, application = result["Value"]
        self.assertEqual(status, "")  # No status change
        self.assertEqual(minor, "")
        self.assertEqual(application, "")

    def test_getNewStatus_multiple_updates(self):
        """Test getNewStatus with multiple status updates"""
        jobID = 123
        updateTimes = [
            datetime.fromisoformat("2023-01-01 10:00:00"),
            datetime.fromisoformat("2023-01-01 11:00:00"),
            datetime.fromisoformat("2023-01-01 12:00:00"),
        ]
        lastTime = datetime.fromisoformat("2023-01-01 09:00:00")
        statusDict = {
            datetime.fromisoformat("2023-01-01 10:00:00"): {"Status": MATCHED, "MinorStatus": "Pilot Agent"},
            datetime.fromisoformat("2023-01-01 11:00:00"): {"Status": RUNNING, "ApplicationStatus": "Running"},
            datetime.fromisoformat("2023-01-01 12:00:00"): {
                "Status": DONE,
                "MinorStatus": "Execution Complete",
                "ApplicationStatus": "Success",
            },
        }
        currentStatus = WAITING
        force = False
        log = MagicMock()

        result = getNewStatus(jobID, updateTimes, lastTime, statusDict, currentStatus, force, log)

        self.assertTrue(result["OK"])
        status, minor, application = result["Value"]
        self.assertEqual(status, DONE)  # Final status
        self.assertEqual(minor, "Execution Complete")  # Final minor status
        self.assertEqual(application, "Success")  # Final application status

    def test_getNewStatus_force_mode(self):
        """Test getNewStatus with force=True bypasses state machine"""
        jobID = 123
        updateTimes = [datetime.fromisoformat("2023-01-01 10:00:00")]
        lastTime = datetime.fromisoformat("2023-01-01 09:00:00")
        statusDict = {
            datetime.fromisoformat("2023-01-01 10:00:00"): {
                "Status": DONE,  # Direct jump to DONE (would normally be rejected)
                "MinorStatus": "Forced",
            }
        }
        currentStatus = WAITING
        force = True  # Force mode
        log = MagicMock()

        result = getNewStatus(jobID, updateTimes, lastTime, statusDict, currentStatus, force, log)

        self.assertTrue(result["OK"])
        status, minor, application = result["Value"]
        self.assertEqual(status, DONE)
        self.assertEqual(minor, "Forced")


if __name__ == "__main__":
    unittest.main()
