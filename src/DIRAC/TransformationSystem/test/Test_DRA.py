"""Test the DataRecoveryAgent"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from collections import defaultdict
from mock import MagicMock as Mock, patch, ANY

from parameterized import parameterized, param

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.TransformationSystem.Agent.DataRecoveryAgent import DataRecoveryAgent
from DIRAC.TransformationSystem.Utilities.JobInfo import TaskInfoException

from DIRAC.tests.Utilities.utils import MatchStringWith

__RCSID__ = "$Id$"

MODULE_NAME = 'DIRAC.TransformationSystem.Agent.DataRecoveryAgent'


class TestDRA(unittest.TestCase):
  """Test the DataRecoveryAgent"""
  dra = None

  @patch("DIRAC.Core.Base.AgentModule.PathFinder", new=Mock())
  @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock())
  @patch("%s.ReqClient" % MODULE_NAME, new=Mock())
  def setUp(self):
    self.dra = DataRecoveryAgent(agentName="ILCTransformationSystem/DataRecoveryAgent", loadName="TestDRA")
    self.dra.transNoInput = ['MCGeneration']
    self.dra.transWithInput = ['MCSimulation', 'MCReconstruction']
    self.dra.transformationTypes = ['MCGeneration', 'MCSimulation', 'MCReconstruction']

    self.dra.reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    self.dra.tClient = Mock(
        name="transMock",
        spec=DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient)
    self.dra.fcClient = Mock(name="fcMock", spec=DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient)
    self.dra.jobMon = Mock(
        name="jobMonMock",
        spec=DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient.JobMonitoringClient)
    self.dra.printEveryNJobs = 10
    self.dra.log = Mock(name="LogMock")
    self.dra.addressTo = 'myself'
    self.dra.addressFrom = 'me'

  def tearDown(self):
    pass

  def getTestMock(self, nameID=0, jobID=1234567):
    """create a JobInfo object with mocks"""
    from DIRAC.TransformationSystem.Utilities.JobInfo import JobInfo
    testJob = Mock(name="jobInfoMock_%s" % nameID, spec=JobInfo)
    testJob.jobID = jobID
    testJob.tType = "testType"
    testJob.otherTasks = []
    testJob.errorCounts = []
    testJob.status = "Done"
    testJob.transFileStatus = ['Assigned', 'Assigned']
    testJob.inputFileStatus = ['Exists', 'Exists']
    testJob.outputFiles = ["/my/stupid/file.lfn", "/my/stupid/file2.lfn"]
    testJob.outputFileStatus = ["Exists", "Exists"]
    testJob.inputFiles = ['inputfile.lfn', 'inputfile2.lfn']
    testJob.pendingRequest = False
    testJob.getTaskInfo = Mock()
    return testJob

  @patch("DIRAC.Core.Base.AgentModule.PathFinder", new=Mock())
  @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock())
  @patch("%s.ReqClient" % MODULE_NAME, new=Mock())
  def test_init(self):
    """test for DataRecoveryAgent initialisation...................................................."""
    res = DataRecoveryAgent(agentName="ILCTransformationSystem/DataRecoveryAgent", loadName="TestDRA")
    self.assertIsInstance(res, DataRecoveryAgent)

  def test_beginExecution(self):
    """test for DataRecoveryAgent beginExecution...................................................."""
    theOps = Mock(name='OpsInstance')
    theOps.getValue.side_effect = [['MCGeneration'], ['MCReconstruction', 'Merge']]
    with patch('DIRAC.TransformationSystem.Agent.DataRecoveryAgent.Operations', return_value=theOps):
      res = self.dra.beginExecution()
    assert isinstance(self.dra.transformationTypes, list)
    assert set(['MCGeneration', 'MCReconstruction', 'Merge']) == set(self.dra.transformationTypes)
    assert set(['MCGeneration']) == set(self.dra.transNoInput)
    assert set(['MCReconstruction', 'Merge']) == set(self.dra.transWithInput)
    self.assertFalse(self.dra.enabled)
    self.assertTrue(res['OK'])

  def test_getEligibleTransformations_success(self):
    """test for DataRecoveryAgent getEligibleTransformations success................................"""
    transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd",
                         AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    self.dra.tClient.getTransformations = Mock(return_value=S_OK([transInfoDict]))
    res = self.dra.getEligibleTransformations(status="Active", typeList=['TestProds'])
    self.assertTrue(res['OK'])
    self.assertIsInstance(res['Value'], dict)
    vals = res['Value']
    self.assertIn("1234", vals)
    self.assertIsInstance(vals['1234'], dict)
    self.assertEqual(transInfoDict, vals["1234"])

  def test_getEligibleTransformations_failed(self):
    """test for DataRecoveryAgent getEligibleTransformations failure................................"""
    self.dra.tClient.getTransformations = Mock(return_value=S_ERROR("No can Do"))
    res = self.dra.getEligibleTransformations(status="Active", typeList=['TestProds'])
    self.assertFalse(res['OK'])
    self.assertEqual("No can Do", res['Message'])

  def test_treatTransformation1(self):
    """test for DataRecoveryAgent treatTransformation success1.........................................."""
    getJobMock = Mock(name="getJobMOck")
    getJobMock.getJobs.return_value = (Mock(name="jobsMOck"), 50, 50)
    tinfoMock = Mock(name="infoMock", return_value=getJobMock)
    self.dra.checkAllJobs = Mock()
    # catch the printout to check path taken
    transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd",
                         AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    with patch("%s.TransformationInfo" % MODULE_NAME, new=tinfoMock):
      self.dra.treatTransformation(1234, transInfoDict)  # returns None
    # check we start with the summary right away
    for _name, args, _kwargs in self.dra.log.notice.mock_calls:
      self.assertNotIn('Getting Tasks:', str(args))

  def test_treatTransformation2(self):
    """test for DataRecoveryAgent treatTransformation success2.........................................."""
    getJobMock = Mock(name="getJobMOck")
    getJobMock.getJobs.return_value = (Mock(name="jobsMock"), 50, 50)
    tinfoMock = Mock(name="infoMock", return_value=getJobMock)
    self.dra.checkAllJobs = Mock()
    # catch the printout to check path taken
    transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="MCSimulation",
                         AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    with patch("%s.TransformationInfo" % MODULE_NAME, new=tinfoMock):
      self.dra.treatTransformation(1234, transInfoDict)  # returns None
    self.dra.log.notice.assert_any_call(MatchStringWith("Getting tasks..."))

  def test_treatTransformation3(self):
    """test for DataRecoveryAgent treatTransformation skip.............................................."""
    getJobMock = Mock(name="getJobMOck")
    getJobMock.getJobs.return_value = (Mock(name="jobsMock"), 50, 50)
    self.dra.checkAllJobs = Mock()
    self.dra.jobCache[1234] = (50, 50)
    # catch the printout to check path taken
    transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd",
                         AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')

    with patch("%s.TransformationInfo" % MODULE_NAME,
               autospec=True,
               return_value=getJobMock):
      self.dra.treatTransformation(transID=1234, transInfoDict=transInfoDict)  # returns None
    self.dra.log.notice.assert_called_with(MatchStringWith("Skipping transformation 1234"))

  def test_checkJob(self):
    """test for DataRecoveryAgent checkJob No inputFiles............................................."""

    from DIRAC.TransformationSystem.Utilities.TransformationInfo import TransformationInfo
    tInfoMock = Mock(name="tInfoMock", spec=TransformationInfo)

    from DIRAC.TransformationSystem.Utilities.JobInfo import JobInfo

    # Test First option for MCGeneration
    tInfoMock.reset_mock()
    testJob = JobInfo(jobID=1234567, status="Failed", tID=123, tType="MCGeneration")
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]

    self.dra.checkJob(testJob, tInfoMock)
    self.assertIn("setJobDone", tInfoMock.method_calls[0])
    self.assertEqual(self.dra.todo['NoInputFiles'][0]['Counter'], 1)
    self.assertEqual(self.dra.todo['NoInputFiles'][1]['Counter'], 0)

    # Test Second option for MCGeneration
    tInfoMock.reset_mock()
    testJob.status = "Done"
    testJob.outputFileStatus = ["Missing"]
    self.dra.checkJob(testJob, tInfoMock)
    self.assertIn("setJobFailed", tInfoMock.method_calls[0])
    self.assertEqual(self.dra.todo['NoInputFiles'][0]['Counter'], 1)
    self.assertEqual(self.dra.todo['NoInputFiles'][1]['Counter'], 1)

    # Test Second option for MCGeneration
    tInfoMock.reset_mock()
    testJob.status = "Done"
    testJob.outputFileStatus = ["Exists"]
    self.dra.checkJob(testJob, tInfoMock)
    self.assertEqual(tInfoMock.method_calls, [])
    self.assertEqual(self.dra.todo['NoInputFiles'][0]['Counter'], 1)
    self.assertEqual(self.dra.todo['NoInputFiles'][1]['Counter'], 1)

  @parameterized.expand([
      param(0, ['setJobDone', 'setInputProcessed'], jStat='Failed', ifStat=[
            'Exists'], ofStat=['Exists'], tFiStat=['Assigned'], others=True),
      param(1, ['setJobFailed'], ifStat=['Exists'], ofStat=['Missing'], others=True, ifProcessed=['/my/inputfile.lfn']),
      param(2, ['setJobFailed', 'cleanOutputs'], ifStat=['Exists'], others=True, ifProcessed=['/my/inputfile.lfn']),
      param(3, ['cleanOutputs', 'setJobFailed', 'setInputDeleted'], ifStat=['Missing']),
      param(4, ['cleanOutputs', 'setJobFailed'], tFiStat=['Deleted'], ifStat=['Missing']),
      param(5, ['setJobDone', 'setInputProcessed'], jStat='Failed', ifStat=['Exists'], tFiStat=['Assigned']),
      param(6, ['setJobDone'], jStat='Failed', ifStat=['Exists'], tFiStat=['Processed']),
      param(7, ['setInputProcessed'], jStat='Done', ifStat=['Exists'], tFiStat=['Assigned']),
      param(8, ['setInputMaxReset'], jStat='Failed', ifStat=['Exists'],
            ofStat=['Missing'], tFiStat=['Assigned'], errorCount=[14]),
      param(9, ['setInputUnused'], jStat='Failed', ifStat=['Exists'],
            ofStat=['Missing'], tFiStat=['Assigned'], errorCount=[2]),
      param(10, ['setInputUnused', 'setJobFailed'], jStat='Done',
            ifStat=['Exists'], ofStat=['Missing'], tFiStat=['Assigned']),
      param(11, ['cleanOutputs', 'setInputUnused'], jStat='Failed', ifStat=[
          'Exists'], ofStat=['Missing', 'Exists'], tFiStat=['Assigned']),
      param(12, ['cleanOutputs', 'setInputUnused', 'setJobFailed'], jStat='Done',
            ifStat=['Exists'], ofStat=['Missing', 'Exists'], tFiStat=['Assigned']),
      param(13, ['setJobFailed'], jStat='Done', ifStat=['Exists'], ofStat=['Missing', 'Missing'], tFiStat=['Unused']),
      param(14, [], jStat='Strange', ifStat=['Exists'], ofStat=['Exists'], tFiStat=['Processed']),
      param(-1, [], jStat='Failed', ifStat=['Exists'], ofStat=['Missing', 'Missing'],
            outFiles=['/my/stupid/file.lfn', "/my/stupid/file2.lfn"], tFiStat=['Processed'], others=True),
  ])
  def test_checkJob_others_(self, counter, infoCalls, jID=1234567, jStat='Done', others=False,
                            inFiles=['/my/inputfile.lfn'], outFiles=['/my/stupid/file.lfn'],
                            ifStat=[], ofStat=['Exists'], ifProcessed=[],
                            tFiStat=['Processed'], errorCount=[]):
    from DIRAC.TransformationSystem.Utilities.TransformationInfo import TransformationInfo
    from DIRAC.TransformationSystem.Utilities.JobInfo import JobInfo
    tInfoMock = Mock(name="tInfoMock", spec=TransformationInfo)
    testJob = JobInfo(jobID=jID, status=jStat, tID=123, tType="MCSimulation")
    testJob.outputFiles = outFiles
    testJob.outputFileStatus = ofStat
    testJob.otherTasks = others
    testJob.inputFiles = inFiles
    testJob.inputFileStatus = ifStat
    testJob.transFileStatus = tFiStat
    testJob.errorCounts = errorCount
    self.dra.inputFilesProcessed = set(ifProcessed)
    self.dra.checkJob(testJob, tInfoMock)
    gLogger.notice('Testing counter', counter)
    gLogger.notice('Expecting calls', infoCalls)
    gLogger.notice('Called', tInfoMock.method_calls)
    assert len(infoCalls) == len(tInfoMock.method_calls)
    for index, infoCall in enumerate(infoCalls):
      self.assertIn(infoCall, tInfoMock.method_calls[index])
    for count in range(15):
      gLogger.notice('Checking Counter:', count)
      if count == counter:
        self.assertEqual(self.dra.todo['InputFiles'][count]['Counter'], 1)
      else:
        self.assertEqual(self.dra.todo['InputFiles'][count]['Counter'], 0)
    if 0 <= counter <= 2:
      assert set(testJob.inputFiles).issubset(self.dra.inputFilesProcessed)

  @parameterized.expand([
      param(['cleanOutputs', 'setJobFailed']),
      param([], jID=667, jStat='Failed', ofStat=['Missing']),
      param([], jID=668, jStat='Failed', ofStat=['Missing'], inFiles=['some']),
  ])
  def test_failHard(self, infoCalls, jID=666, jStat='Done', inFiles=None, ofStat=['Exists']):
    """Test the job.failHard function."""
    from DIRAC.TransformationSystem.Utilities.TransformationInfo import TransformationInfo
    from DIRAC.TransformationSystem.Utilities.JobInfo import JobInfo
    tInfoMock = Mock(name="tInfoMock", spec=TransformationInfo)
    tInfoMock.reset_mock()
    testJob = JobInfo(jobID=666, status=jStat, tID=123, tType='MCSimulation')
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ofStat
    testJob.otherTasks = True
    testJob.inputFiles = inFiles
    testJob.inputFileExists = True
    testJob.fileStatus = 'Processed'
    self.dra.inputFilesProcessed = set()
    self.dra._DataRecoveryAgent__failJobHard(testJob, tInfoMock)  # pylint: disable=protected-access, no-member
    gLogger.notice('Expecting calls', infoCalls)
    gLogger.notice('Called', tInfoMock.method_calls)
    assert len(infoCalls) == len(tInfoMock.method_calls)
    for index, infoCall in enumerate(infoCalls):
      self.assertIn(infoCall, tInfoMock.method_calls[index])
    if jStat == 'Done':
      self.assertIn('Failing job %s' % jID, self.dra.notesToSend)
    else:
      self.assertNotIn('Failing job %s' % jID, self.dra.notesToSend)

  def test_notOnlyKeepers(self):
    """ test for __notOnlyKeepers function """

    funcToTest = self.dra._DataRecoveryAgent__notOnlyKeepers  # pylint: disable=protected-access, no-member
    self.assertTrue(funcToTest('MCGeneration'))

    self.dra.todo['InputFiles'][0]['Counter'] = 3  # keepers
    self.dra.todo['InputFiles'][3]['Counter'] = 0
    self.assertFalse(funcToTest("MCSimulation"))

    self.dra.todo['InputFiles'][0]['Counter'] = 3  # keepers
    self.dra.todo['InputFiles'][3]['Counter'] = 3
    self.assertTrue(funcToTest("MCSimulation"))

  def test_checkAllJob(self):
    """test for DataRecoveryAgent checkAllJobs ....................................................."""
    from DIRAC.TransformationSystem.Utilities.JobInfo import JobInfo

    # test with additional task dicts
    from DIRAC.TransformationSystem.Utilities.TransformationInfo import TransformationInfo
    tInfoMock = Mock(name="tInfoMock", spec=TransformationInfo)
    mockJobs = dict([(i, self.getTestMock()) for i in range(11)])
    mockJobs[2].pendingRequest = True
    mockJobs[3].getJobInformation = Mock(side_effect=(RuntimeError('ARGJob1'), None))
    mockJobs[4].getTaskInfo = Mock(side_effect=(TaskInfoException('ARG1'), None))
    taskDict = True
    lfnTaskDict = True
    self.dra.checkAllJobs(mockJobs, tInfoMock, taskDict, lfnTaskDict)
    self.dra.log.error.assert_any_call(MatchStringWith('+++++ Exception'), 'ARGJob1')
    self.dra.log.error.assert_any_call(MatchStringWith("Skip Task, due to TaskInfoException: ARG1"))
    self.dra.log.reset_mock()

    # test inputFile None
    mockJobs = dict([(i, self.getTestMock(nameID=i)) for i in range(5)])
    mockJobs[1].inputFiles = []
    mockJobs[1].getTaskInfo = Mock(side_effect=(TaskInfoException("NoInputFile"), None))
    mockJobs[1].tType = "MCSimulation"
    tInfoMock.reset_mock()
    self.dra.checkAllJobs(mockJobs, tInfoMock, taskDict, lfnTaskDict=True)
    self.dra.log.notice.assert_any_call(MatchStringWith("Failing job hard"))

  def test_checkAllJob_2(self):
    """Test where failJobHard fails (via cleanOutputs)."""
    from DIRAC.TransformationSystem.Utilities.TransformationInfo import TransformationInfo
    tInfoMock = Mock(name='tInfoMock', spec=TransformationInfo)
    mockJobs = dict([(i, self.getTestMock()) for i in range(5)])
    mockJobs[2].pendingRequest = True
    mockJobs[3].getTaskInfo = Mock(side_effect=(TaskInfoException('ARGJob3'), None))
    mockJobs[3].inputFiles = []
    mockJobs[3].tType = 'MCReconstruction'
    self.dra._DataRecoveryAgent__failJobHard = Mock(side_effect=(RuntimeError('ARGJob4'), None), name='FJH')
    self.dra.checkAllJobs(mockJobs, tInfoMock, tasksDict=True, lfnTaskDict=True)
    mockJobs[3].getTaskInfo.assert_called()
    self.dra._DataRecoveryAgent__failJobHard.assert_called()
    self.dra.log.error.assert_any_call(MatchStringWith('+++++ Exception'), 'ARGJob4')
    self.dra.log.reset_mock()

  def test_execute(self):
    """test for DataRecoveryAgent execute .........................................................."""
    self.dra.treatTransformation = Mock()

    self.dra.transformationsToIgnore = [123, 456, 789]
    self.dra.jobCache = defaultdict(lambda: (0, 0))
    self.dra.jobCache[123] = (10, 10)
    self.dra.jobCache[124] = (10, 10)
    self.dra.jobCache[125] = (10, 10)

    # Eligible fails
    self.dra.log.reset_mock()
    self.dra.getEligibleTransformations = Mock(return_value=S_ERROR("outcast"))
    res = self.dra.execute()
    self.assertFalse(res["OK"])
    self.dra.log.error.assert_any_call(ANY, MatchStringWith("outcast"))
    self.assertEqual("Failure to get transformations", res['Message'])

    d123 = dict(TransformationID=123, TransformationName='TestProd123', Type='MCGeneration',
                AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    d124 = dict(TransformationID=124, TransformationName='TestProd124', Type='MCGeneration',
                AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    d125 = dict(TransformationID=125, TransformationName='TestProd125', Type='MCGeneration',
                AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')

    # Eligible succeeds
    self.dra.log.reset_mock()
    self.dra.getEligibleTransformations = Mock(return_value=S_OK({123: d123, 124: d124, 125: d125}))
    res = self.dra.execute()
    self.assertTrue(res["OK"])
    self.dra.log.notice.assert_any_call(MatchStringWith("Will ignore the following transformations: [123, 456, 789]"))
    self.dra.log.notice.assert_any_call(MatchStringWith("Ignoring Transformation: 123"))
    self.dra.log.notice.assert_any_call(MatchStringWith("Running over Transformation: 124"))

    # Notes To Send
    self.dra.log.reset_mock()
    self.dra.getEligibleTransformations = Mock(return_value=S_OK({123: d123, 124: d124, 125: d125}))
    self.dra.notesToSend = "Da hast du deine Karte"
    sendmailMock = Mock()
    sendmailMock.sendMail.return_value = S_OK("Nice Card")
    notificationMock = Mock(return_value=sendmailMock)
    with patch("%s.NotificationClient" % MODULE_NAME, new=notificationMock):
      res = self.dra.execute()
    self.assertTrue(res["OK"])
    self.dra.log.notice.assert_any_call(MatchStringWith("Will ignore the following transformations: [123, 456, 789]"))
    self.dra.log.notice.assert_any_call(MatchStringWith("Ignoring Transformation: 123"))
    self.dra.log.notice.assert_any_call(MatchStringWith("Running over Transformation: 124"))
    self.assertNotIn(124, self.dra.jobCache)  # was popped
    self.assertIn(125, self.dra.jobCache)  # was not popped
    gLogger.notice("JobCache: %s" % self.dra.jobCache)

    # sending notes fails
    self.dra.log.reset_mock()
    self.dra.notesToSend = "Da hast du deine Karte"
    sendmailMock = Mock()
    sendmailMock.sendMail.return_value = S_ERROR("No stamp")
    notificationMock = Mock(return_value=sendmailMock)
    with patch("%s.NotificationClient" % MODULE_NAME, new=notificationMock):
      res = self.dra.execute()
    self.assertTrue(res["OK"])
    self.assertNotIn(124, self.dra.jobCache)  # was popped
    self.assertIn(125, self.dra.jobCache)  # was not popped
    self.dra.log.error.assert_any_call(MatchStringWith("Cannot send notification mail"), ANY)

    self.assertEqual("", self.dra.notesToSend)

  def test_printSummary(self):
    """test DataRecoveryAgent printSummary.........................................................."""
    self.dra.notesToSend = ""
    self.dra.printSummary()
    self.assertNotIn(" Other Tasks --> Keep                                    :     0", self.dra.notesToSend)

    self.dra.notesToSend = "Note This"
    self.dra.printSummary()

  def test_setPendingRequests_1(self):
    """Check the setPendingRequests function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in range(11))
    reqMock = Mock()
    reqMock.Status = "Done"
    reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    reqClient.readRequestsForJobs.return_value = S_OK({"Successful": {}})
    self.dra.reqClient = reqClient
    self.dra.setPendingRequests(mockJobs)
    for _index, mj in mockJobs.items():
      self.assertFalse(mj.pendingRequest)

  def test_setPendingRequests_2(self):
    """Check the setPendingRequests function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in range(11))
    reqMock = Mock()
    reqMock.RequestID = 666
    reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    reqClient.readRequestsForJobs.return_value = S_OK({"Successful": {6: reqMock}})
    reqClient.getRequestStatus.return_value = {'Value': 'Done'}
    self.dra.reqClient = reqClient
    self.dra.setPendingRequests(mockJobs)
    for _index, mj in mockJobs.items():
      self.assertFalse(mj.pendingRequest)
    reqClient.getRequestStatus.assert_called_once_with(666)

  def test_setPendingRequests_3(self):
    """Check the setPendingRequests function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in range(11))
    reqMock = Mock()
    reqMock.RequestID = 555
    reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    reqClient.readRequestsForJobs.return_value = S_OK({'Successful': {5: reqMock}})
    reqClient.getRequestStatus.return_value = {'Value': 'Pending'}
    self.dra.reqClient = reqClient
    self.dra.setPendingRequests(mockJobs)
    for index, mj in mockJobs.items():
      if index == 5:
        self.assertTrue(mj.pendingRequest)
      else:
        self.assertFalse(mj.pendingRequest)
    reqClient.getRequestStatus.assert_called_once_with(555)

  def test_setPendingRequests_Fail(self):
    """Check the setPendingRequests function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in range(11))
    reqMock = Mock()
    reqMock.Status = "Done"
    reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    reqClient.readRequestsForJobs.side_effect = (S_ERROR('Failure'), S_OK({'Successful': {}}))
    self.dra.reqClient = reqClient
    self.dra.setPendingRequests(mockJobs)
    for _index, mj in mockJobs.items():
      self.assertFalse(mj.pendingRequest)

  def test_getLFNStatus(self):
    """Check the getLFNStatus function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in range(11))
    self.dra.fcClient.exists.return_value = S_OK({'Successful':
                                                  {'/my/stupid/file.lfn': True,
                                                   '/my/stupid/file2.lfn': True}})
    lfnExistence = self.dra.getLFNStatus(mockJobs)
    self.assertEqual(lfnExistence, {'/my/stupid/file.lfn': True,
                                    '/my/stupid/file2.lfn': True})

    self.dra.fcClient.exists.side_effect = (S_ERROR('args'),
                                            S_OK({'Successful':
                                                  {'/my/stupid/file.lfn': True,
                                                   '/my/stupid/file2.lfn': True}}))
    lfnExistence = self.dra.getLFNStatus(mockJobs)
    self.assertEqual(lfnExistence, {'/my/stupid/file.lfn': True,
                                    '/my/stupid/file2.lfn': True})
