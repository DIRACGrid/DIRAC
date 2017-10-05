"""Test the Transformationinfo"""

import unittest
from collections import OrderedDict

from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR
import DIRAC

from DIRAC.TransformationSystem.Utilities.JobInfo import JobInfo
from DIRAC.TransformationSystem.Utilities.TransformationInfo import TransformationInfo

from ILCDIRAC.Tests.Utilities.GeneralUtils import MatchStringWith

__RCSID__ = "$Id$"

#pylint: disable=W0212


class TestTI(unittest.TestCase):
  """Test the TransformationInfo class"""

  def setUp(self):

    tMock = Mock(name="transMock", spec=DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient)
    tMock.setFileStatusForTransformation = Mock(name="setFileStat")
    fcMock = Mock(name="fcMock", spec=DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient)
    jmMock = Mock(name="jobMonMock", spec=DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient.JobMonitoringClient)

    self.tri = TransformationInfo(transformationID=1234,
                                  transName="TestTrans",
                                  transType="MCGeneration",
                                  enabled=False,
                                  tClient=tMock,
                                  fcClient=fcMock,
                                  jobMon=jmMock,
                                  )

    self.taskDicts = [dict(TaskID=123,
                           LFN="lfn123",
                           Status="Assigned",
                           FileID=987001,
                           ErrorCount=9,
                           ),
                      dict(TaskID=124,
                           LFN="lfn124",
                           Status="Processed",
                           FileID=987002,
                           ErrorCount=8,
                           ),
                      ]
    self.tri.log = Mock(name="LogMock")

  def tearDown( self ):
    pass

  def test_init(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo init..........................."""
    self.assertIsInstance(self.tri, TransformationInfo)
    self.assertFalse(self.tri.enabled)

  def test_checkTasksStatus( self ):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo checkTasksStatus..............."""
    ## error getting files
    self.tri.tClient.getTransformationFiles.return_value = S_ERROR("nope")
    with self.assertRaisesRegexp(RuntimeError, "Failed to get transformation tasks: nope"):
      self.tri.checkTasksStatus()

    ## success getting files
    self.tri.tClient.getTransformationFiles.return_value = S_OK(self.taskDicts)
    retDict = self.tri.checkTasksStatus()
    self.assertEqual(len(retDict), 2)
    self.assertIn(123, retDict)
    self.assertIn(124, retDict)
    self.assertIn("FileID", retDict[124])

  def test_setJob_Status(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo setJob functions..............."""
    job = Mock(spec=JobInfo)
    job.jobID = 5678
    self.tri.enabled = False
    self.tri._TransformationInfo__setTaskStatus = Mock()
    self.tri._TransformationInfo__updateJobStatus = Mock()

    ## setJobDone
    self.tri.setJobDone(job)
    self.tri._TransformationInfo__setTaskStatus.assert_not_called()
    self.tri._TransformationInfo__updateJobStatus.assert_not_called()

    self.tri.enabled = True
    job.status = "Done"
    self.tri.setJobDone(job)
    self.tri._TransformationInfo__setTaskStatus.assert_called_once_with(job, "Done")
    self.tri._TransformationInfo__updateJobStatus.assert_not_called()

    self.tri.enabled = True
    job.status = "Failed"
    self.tri._TransformationInfo__setTaskStatus.reset_mock()
    self.tri.setJobDone(job)
    self.tri._TransformationInfo__setTaskStatus.assert_called_once_with(job, "Done")
    self.tri._TransformationInfo__updateJobStatus.assert_called_once_with(job.jobID, 'Done', "Job forced to Done")

    ## setJobFailed
    self.tri.enabled = False
    self.tri._TransformationInfo__setTaskStatus.reset_mock()
    self.tri._TransformationInfo__updateJobStatus.reset_mock()
    self.tri.setJobFailed(job)
    self.tri._TransformationInfo__setTaskStatus.assert_not_called()
    self.tri._TransformationInfo__updateJobStatus.assert_not_called()

    self.tri.enabled = True
    job.status = "Failed"
    self.tri._TransformationInfo__setTaskStatus.reset_mock()
    self.tri._TransformationInfo__updateJobStatus.reset_mock()
    self.tri.setJobFailed(job)
    self.tri._TransformationInfo__setTaskStatus.assert_called_once_with(job, "Failed")
    self.tri._TransformationInfo__updateJobStatus.assert_not_called()

    self.tri.enabled = True
    job.status = "Done"
    self.tri._TransformationInfo__setTaskStatus.reset_mock()
    self.tri._TransformationInfo__updateJobStatus.reset_mock()
    self.tri.setJobFailed(job)
    self.tri._TransformationInfo__setTaskStatus.assert_called_once_with(job, "Failed")
    self.tri._TransformationInfo__updateJobStatus.assert_called_once_with(job.jobID, "Failed", "Job forced to Failed")

  def test_setInputStatusFuncs(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo setInput functions............."""
    self.tri._TransformationInfo__setInputStatus = Mock()
    self.tri.enabled = False
    job = Mock(spec=JobInfo)

    self.tri._TransformationInfo__setInputStatus.reset_mock()
    self.tri.setInputUnused(job)
    self.tri._TransformationInfo__setInputStatus.assert_called_once_with(job, "Unused")

    self.tri._TransformationInfo__setInputStatus.reset_mock()
    self.tri.setInputProcessed(job)
    self.tri._TransformationInfo__setInputStatus.assert_called_once_with(job, "Processed")

    self.tri._TransformationInfo__setInputStatus.reset_mock()
    self.tri.setInputDeleted(job)
    self.tri._TransformationInfo__setInputStatus.assert_called_once_with(job, "Deleted")

    self.tri._TransformationInfo__setInputStatus.reset_mock()
    self.tri.setInputMaxReset(job)
    self.tri._TransformationInfo__setInputStatus.assert_called_once_with(job, "MaxReset")

  def test_setInputStatus(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo setInputStatus................."""
    job = Mock(spec=JobInfo)
    job.inputFile = "dummylfn"
    status = "Unused"

    self.tri.enabled = False
    self.tri._TransformationInfo__setInputStatus(job, status)

    self.tri.enabled = True
    self.tri.tClient.setFileStatusForTransformation.return_value = S_ERROR("Failed to set")
    with self.assertRaisesRegexp(RuntimeError, "Failed updating file status"):
      self.tri._TransformationInfo__setInputStatus(job, status)

    self.tri.enabled = True
    self.tri.tClient.setFileStatusForTransformation.return_value = S_OK("All Good")
    self.tri._TransformationInfo__setInputStatus(job, status)

  def test_setTaskStatus(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo setTaskStatus.................."""
    job = Mock(spec=JobInfo)
    job.taskID = 1234
    self.tri.tClient.setTaskStatus = Mock(return_value=S_OK("Done"))
    self.tri._TransformationInfo__setTaskStatus(job, "Processed")
    self.tri.tClient.setTaskStatus.assert_called_once_with(self.tri.transName, 1234, "Processed")

    self.tri.tClient.setTaskStatus = Mock(return_value=S_ERROR("NotDone"))
    with self.assertRaisesRegexp(RuntimeError, "Failed updating task status: NotDone"):
      self.tri._TransformationInfo__setTaskStatus(job, "Processed")
    self.tri.tClient.setTaskStatus.assert_called_once_with(self.tri.transName, 1234, "Processed")

  def test_updateJobStatus(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo updateJobStatus................"""
    dbMock = Mock()
    dbMock.setJobAttribute.return_value = S_OK()
    dbMock.getJobAttributes.return_value = S_OK(dict(MinorStatus="Statussed"))

    logMock = Mock()
    logMock.addLoggingRecord.return_value = S_OK("added record")

    jobDBMock = Mock()
    jobDBMock.return_value = dbMock
    logDBMock = Mock()
    logDBMock.return_value = logMock
#    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB", new=Mock() ):
    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
      #      with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB", new=Mock() ):
      with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
        self.tri.enabled = False
        res = self.tri._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], "DisabledMode")

    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
      with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
        self.tri.enabled = True
        res = self.tri._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], "added record")

    dbMock.setJobAttribute.return_value = S_ERROR("Error setting job status")
    dbMock.getJobAttributes.return_value = S_OK(dict(MinorStatus="Statussed"))
    jobDBMock.return_value = dbMock
    logDBMock.return_value = logMock
    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock ):
      with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock ):
        self.tri.enabled = True
        with self.assertRaisesRegexp( RuntimeError, "Failed to update job status" ):
          self.tri._TransformationInfo__updateJobStatus( 1234, "Failed", minorstatus=None )

    dbMock.setJobAttribute.return_value = S_OK("SetTheStatus")
    dbMock.getJobAttributes.return_value = S_ERROR("Failed to get status")
    jobDBMock.return_value = dbMock
    logDBMock.return_value = logMock
    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
      with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
        self.tri.enabled = True
        with self.assertRaisesRegexp(RuntimeError, "Failed to get Minorstatus"):
          self.tri._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)

    dbMock.setJobAttribute.return_value = S_OK("SetTheStatus")
    dbMock.getJobAttributes.return_value = S_OK(dict(MinorStatus="Statussed"))
    logMock.addLoggingRecord.reset_mock()
    logMock.addLoggingRecord.return_value = S_ERROR("Did not add record")
    jobDBMock.return_value = dbMock
    logDBMock.return_value = logMock
    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
      with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
        self.tri.enabled = True
        res = self.tri._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)
        self.assertFalse(res['OK'])
        self.assertEqual(res['Message'], "Did not add record")
        logMock.addLoggingRecord.assert_called_once_with(
            1234, status="Failed", minor="Statussed", source='DataRecoveryAgent')

    dbMock.setJobAttribute.return_value = S_OK("SetTheStatus")
    dbMock.getJobAttributes.return_value = S_OK(dict(MinorStatus="Statussed"))
    logMock.addLoggingRecord.reset_mock()
    logMock.addLoggingRecord.return_value = S_OK("added record")
    jobDBMock.return_value = dbMock
    logDBMock.return_value = logMock
    with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
      with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
        self.tri.enabled = True
        res = self.tri._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus="minorstatus")
        self.assertTrue(res['OK'])
        self.assertEqual(res['Value'], "added record")
        logMock.addLoggingRecord.assert_called_once_with(
            1234, status="Failed", minor="minorstatus", source='DataRecoveryAgent')

  def test_findAllDescendants(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo findAllDescendents............."""
    self.tri.fcClient.getFileDescendents = Mock(return_value=S_OK({"Successful": {"lfn1": ["lfnD1", "lfnD2"],
                                                                                  "lfnD1": ["lfnDD1", "lfnDD2"]
                                                                                  }
                                                                   })
                                                )
    descList = self.tri._TransformationInfo__findAllDescendants(lfnList=[])
    self.assertEqual(descList, ["lfnDD1", "lfnDD2", "lfnD1", "lfnD2"])

    self.tri.fcClient.getFileDescendents = Mock(return_value=S_ERROR("Cannot get descendants"))
    descList = self.tri._TransformationInfo__findAllDescendants(lfnList=[])
    self.assertEqual(descList, [])

  def test_cleanOutputs(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo cleanOutputs..................."""
    descList = ["lfnDD1", "lfnDD2", "lfnD1", "lfnD2"]

    jobInfo = Mock(spec=JobInfo)
    jobInfo.outputFiles = ["lfn1", "lfn2"]
    jobInfo.outputFileStatus = ["Exists", "Missing"]

    self.tri.enabled = False
    self.tri._TransformationInfo__findAllDescendants = Mock(return_value=descList)

    self.tri.cleanOutputs(jobInfo)
    self.tri.log.notice.assert_any_call(MatchStringWith("Would have removed these files"))
    self.tri.log.notice.assert_any_call(MatchStringWith("lfn1"))
    for _name, args, _kwargs in self.tri.log.notice.mock_calls:
      self.assertNotIn("lfn2", str(args))

    remMock = Mock(name="remmock")
    remMock.removeFile.return_value = S_ERROR("arg")

    self.tri.enabled = True
    self.tri._TransformationInfo__findAllDescendants = Mock(return_value=descList)
    with patch("DIRAC.TransformationSystem.Utilities.TransformationInfo.DataManager",
               return_value=remMock,
               autospec=True):
      with self.assertRaisesRegexp(RuntimeError, "Failed to remove LFNs: arg"):
        self.tri.cleanOutputs(jobInfo)

    remMock = Mock(name="remmock")
    remMock.removeFile.return_value = S_OK({"Successful": {"lfn1": "OK", "lfn2": "OK"},
                                            "Failed": {"lfnD2": "SomeReason",
                                                       "lfnD3": "SomeReason",
                                                       "lfnDD2": "SomeOtherReason"}})

    self.tri.enabled = True
    with patch("DIRAC.TransformationSystem.Utilities.TransformationInfo.DataManager",
               autospec=True,
               return_value=remMock):
      self.tri.cleanOutputs(jobInfo)
      self.tri.log.notice.assert_any_call(MatchStringWith("Successfully removed 2 files"))

    ### nothing to remove
    jobInfo = Mock(spec=JobInfo)
    jobInfo.outputFiles = []
    self.tri._TransformationInfo__findAllDescendants = Mock(return_value=descList)
    self.tri.cleanOutputs(jobInfo)
    self.tri._TransformationInfo__findAllDescendants.assert_not_called()

    ### nothing to remove
    jobInfo = Mock(spec=JobInfo)
    jobInfo.outputFiles = ["lfn1", "lfn2"]
    jobInfo.outputFileStatus = ["Missing", "Missing"]
    self.tri._TransformationInfo__findAllDescendants = Mock(return_value=[])
    self.tri.cleanOutputs(jobInfo)
    self.tri._TransformationInfo__findAllDescendants.assert_called_once_with(jobInfo.outputFiles)

  def test_getJobs(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo getJobs........................"""
    self.tri.jobMon.getJobs = Mock()
    self.tri.jobMon.getJobs.side_effect = (S_OK([123, 456, 789]), S_OK([1123, 1456, 1789]))

    ##All OK, just Done
    jobs, ndone, nfailed = self.tri.getJobs(statusList=["Done"])
    attrDict = dict(Status=["Done"], JobGroup="00001234")
    self.tri.jobMon.getJobs.assert_called_once_with(attrDict)
    self.assertEqual(ndone, 3)
    self.assertEqual(nfailed, 0)
    self.assertIsInstance(jobs, OrderedDict)

    ##All OK, just Done
    self.tri.jobMon.getJobs.reset_mock()
    self.tri.jobMon.getJobs.side_effect = (S_OK([123, 456, 789]), S_OK([1123, 1456, 1789]))
    jobs, ndone, nfailed = self.tri.getJobs(statusList=["Failed"])
    attrDict = dict(Status=["Failed"], JobGroup="00001234")
    self.tri.jobMon.getJobs.assert_called_once_with(attrDict)
    self.assertEqual(ndone, 0)
    self.assertEqual(nfailed, 3)
    self.assertIsInstance(jobs, OrderedDict)

    ##All OK, None
    self.tri.jobMon.getJobs.reset_mock()
    self.tri.jobMon.getJobs.side_effect = (S_OK([123, 456, 789]), S_OK([1123, 56, 89]))
    jobs, ndone, nfailed = self.tri.getJobs(statusList=None)
    attrDict = dict(Status=["Done"], JobGroup="00001234")
    attrDict2 = dict(Status=["Failed"], JobGroup="00001234")
    self.tri.jobMon.getJobs.assert_any_call(attrDict)
    self.tri.jobMon.getJobs.assert_any_call(attrDict2)
    self.assertEqual(ndone, 3)
    self.assertEqual(nfailed, 3)
    self.assertIsInstance(jobs, OrderedDict)
    self.assertEqual([56, 89, 123, 456, 789, 1123], jobs.keys())

    ##All ERROR
    self.tri.jobMon.getJobs = Mock()
    self.tri.jobMon.getJobs.side_effect = (S_ERROR("Not Done"), S_ERROR("Not Failed"))
    with self.assertRaisesRegexp(RuntimeError, "Failed to get jobs"):
      jobs, ndone, nfailed = self.tri.getJobs(statusList=None)
    attrDict = dict(Status=["Done"], JobGroup="00001234")
    self.tri.jobMon.getJobs.assert_called_once_with(attrDict)


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestTI)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
