""" unit tests for Transformation Clients
"""

# pylint: disable=protected-access,missing-docstring,invalid-name

import unittest
import json
import mock

from DIRAC import gLogger, S_OK
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.TransformationSystem.Client.TaskManager import TaskBase, WorkflowTasks, RequestTasks
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.Utilities import PluginUtilities

#############################################################################


def ourgetSitesForSE(ses):
  if ses == ['pippo'] or ses == 'pippo':
    return S_OK(['Site1'])
  elif ses == ['pluto'] or ses == 'pluto':
    return S_OK(['Site2'])
  elif ses == ['pippo', 'pluto'] or ses == 'pippo,pluto':
    return S_OK(['Site1', 'Site2'])


class reqValFake_C(object):
  def validate(self, opsInput):
    for ops in opsInput:
      if not len(ops):
        return {'OK': False}
      for f in ops:
        try:
          if not f.LFN:
            return {'OK': False}
        except:
          return {'OK': False}
    return {'OK': True}


reqValFake = reqValFake_C()


class ClientsTestCase(unittest.TestCase):
  """ Base class for the clients test cases
  """

  # @mock.patch( 'DIRAC.TransformationSystem.Client.TaskManagerPlugin.getSitesForSE', side_effect = ourgetSitesForSE )
  # def setUp( self, _ ):

  def setUp(self):
    self.mockTransClient = mock.MagicMock()
    self.mockTransClient.setTaskStatusAndWmsID.return_value = {'OK': True}

    self.WMSClientMock = mock.MagicMock()
    self.jobMonitoringClient = mock.MagicMock()
    self.mockReqClient = mock.MagicMock()

    self.jobMock = mock.MagicMock()
    self.jobMock2 = mock.MagicMock()
    mockWF = mock.MagicMock()
    mockPar = mock.MagicMock()
    mockWF.findParameter.return_value = mockPar
    mockPar.getValue.return_value = 'MySite'

    self.jobMock2.workflow = mockWF
    self.jobMock2.setDestination.return_value = {'OK': True}
    self.jobMock.workflow.return_value = ''
    self.jobMock.return_value = self.jobMock2

    self.reqValidatorMock = mock.MagicMock()
    self.reqValidatorMock.validate.return_value = {'OK': True}

    self.taskBase = TaskBase(transClient=self.mockTransClient)
    self.pu = PluginUtilities(transClient=self.mockTransClient)
    self.wfTasks = WorkflowTasks(transClient=self.mockTransClient,
                                 submissionClient=self.WMSClientMock,
                                 jobMonitoringClient=self.jobMonitoringClient,
                                 outputDataModule="mock")

    self.requestTasks = RequestTasks(transClient=self.mockTransClient,
                                     requestClient=self.mockReqClient,
                                     requestValidator=reqValFake)
    self.tc = TransformationClient()
    self.transformation = Transformation()

    self.maxDiff = None

    gLogger.setLevel('DEBUG')

  def tearDown(self):
    pass

#############################################################################


class TaskBaseSuccess(ClientsTestCase):

  def test_updateDBAfterTaskSubmission(self):
    res = self.taskBase.updateDBAfterTaskSubmission({})
    self.assertEqual(res['OK'], True)

#############################################################################

class PluginUtilitiesSuccess(ClientsTestCase):

  def test_groupByReplicas(self):
    res = self.pu.groupByReplicas({'/this/is/at.1': ['SE1'],
                                   '/this/is/at.12': ['SE1', 'SE2'],
                                   '/this/is/at.2': ['SE2'],
                                   '/this/is/at_123': ['SE1', 'SE2', 'SE3'],
                                   '/this/is/at_23': ['SE2', 'SE3'],
                                   '/this/is/at_4': ['SE4']},
                                  'Flush')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [('SE1', ['/this/is/at.1']),
                                    ('SE1,SE2', ['/this/is/at.12']),
                                    ('SE1,SE2,SE3', ['/this/is/at_123']),
                                    ('SE2', ['/this/is/at.2']),
                                    ('SE2,SE3', ['/this/is/at_23']),
                                    ('SE4', ['/this/is/at_4'])])

    res = self.pu.groupByReplicas({'/this/is/at.123': ['SE1', 'SE2', 'SE3'],
                                   '/this/is/at.12': ['SE1', 'SE2'],
                                   '/this/is/at.134': ['SE1', 'SE3', 'SE4']},
                                  'Flush')
    self.assertTrue(res['OK'])
    print res['Value']
    self.assertEqual(res['Value'], [
                     ('SE1,SE2', ['/this/is/at.12']),
                     ('SE1,SE2,SE3', ['/this/is/at.123']),
                     ('SE1,SE3,SE4', ['/this/is/at.134'])])

#############################################################################


class WorkflowTasksSuccess(ClientsTestCase):

  def test_prepareTranformationTasks(self):
    taskDict = {1: {'TransformationID': 1, 'a1': 'aa1', 'b1': 'bb1', 'Site': 'MySite'},
                2: {'TransformationID': 1, 'a2': 'aa2', 'b2': 'bb2', 'InputData': ['a1', 'a2']},
                3: {'TransformationID': 2, 'a3': 'aa3', 'b3': 'bb3'}, }

    res = self.wfTasks.prepareTransformationTasks(
        '', taskDict, 'test_user', 'test_group', 'test_DN')
    self.assertTrue(res['OK'])
    self.assertEqual(res, {'OK': True,
                           'Value': {1: {'a1': 'aa1', 'TaskObject': '', 'TransformationID': 1,
                                         'b1': 'bb1', 'Site': 'ANY', 'JobType': 'User'},
                                     2: {'TaskObject': '', 'a2': 'aa2', 'TransformationID': 1,
                                         'InputData': ['a1', 'a2'], 'b2': 'bb2', 'Site': 'ANY', 'JobType': 'User'},
                                     3: {'TaskObject': '', 'a3': 'aa3', 'TransformationID': 2,
                                         'b3': 'bb3', 'Site': 'ANY', 'JobType': 'User'}
                                     }
                           }
                     )

    taskDict = {1: {'TransformationID': 1, 'a1': 'aa1', 'b1': 'bb1', 'Site': 'MySite'},
                2: {'TransformationID': 1, 'a2': 'aa2', 'b2': 'bb2', 'InputData': ['a1', 'a2']},
                3: {'TransformationID': 2, 'a3': 'aa3', 'b3': 'bb3'}, }

    res = self.wfTasks.prepareTransformationTasks('', dict(
        taskDict), 'test_user', 'test_group', 'test_DN', bulkSubmissionFlag=True)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][1],
                     {'a1': 'aa1', 'TransformationID': 1, 'b1': 'bb1', 'Site': 'MySite'})
    self.assertEqual(res['Value'][2],
                     {'a2': 'aa2', 'TransformationID': 1, 'b2': 'bb2', 'InputData': ['a1', 'a2']})
    self.assertEqual(res['Value'][3],
                     {'TransformationID': 2, 'a3': 'aa3', 'b3': 'bb3'})
    self.assertTrue('BulkJobObject' in res['Value'])

  @mock.patch('DIRAC.TransformationSystem.Client.TaskManagerPlugin.getSitesForSE', side_effect=ourgetSitesForSE)
  def test__handleDestination(self, _):

    res = self.wfTasks._handleDestination({'Site': '', 'TargetSE': ''})
    self.assertEqual(res, ['ANY'])
    res = self.wfTasks._handleDestination({'Site': 'ANY', 'TargetSE': ''})
    self.assertEqual(res, ['ANY'])
    res = self.wfTasks._handleDestination({'TargetSE': 'Unknown'})
    self.assertEqual(res, ['ANY'])
    res = self.wfTasks._handleDestination({'Site': 'Site2', 'TargetSE': ''})
    self.assertEqual(res, ['Site2'])
    res = self.wfTasks._handleDestination({'Site': 'Site1;Site2', 'TargetSE': 'pippo'})
    self.assertEqual(res, ['Site1'])
    res = self.wfTasks._handleDestination({'Site': 'Site1;Site2', 'TargetSE': 'pippo,pluto'})
    self.assertEqual(sorted(res), sorted(['Site1', 'Site2']))
    res = self.wfTasks._handleDestination({'Site': 'Site1;Site2;Site3', 'TargetSE': 'pippo,pluto'})
    self.assertEqual(sorted(res), sorted(['Site1', 'Site2']))
    res = self.wfTasks._handleDestination({'Site': 'Site2', 'TargetSE': 'pippo,pluto'})
    self.assertEqual(sorted(res), sorted(['Site2']))
    res = self.wfTasks._handleDestination({'Site': 'ANY', 'TargetSE': 'pippo,pluto'})
    self.assertEqual(sorted(res), sorted(['Site1', 'Site2']))
    res = self.wfTasks._handleDestination({'Site': 'Site1', 'TargetSE': 'pluto'})
    self.assertEqual(res, [])

#############################################################################


class RequestTasksSuccess(ClientsTestCase):

  def test_prepareTranformationTasks(self):

    # No tasks in input
    taskDict = {}
    res = self.requestTasks.prepareTransformationTasks('', taskDict, 'owner', 'ownerGroup', '/bih/boh/DN')
    self.assertTrue(res['OK'])
    self.assertEqual(len(taskDict), 0)

    # 3 tasks, 1 task not OK (in second transformation)
    taskDict = {123: {'TransformationID': 2,
                      'TargetSE': 'SE3', 'b3': 'bb3', 'InputData': ''}}
    res = self.requestTasks.prepareTransformationTasks(
        '', taskDict, 'owner', 'ownerGroup', '/bih/boh/DN')
    self.assertTrue(res['OK'])
    # We should "lose" one of the task in the preparation
    self.assertEqual(len(taskDict), 0)

    taskDict = {1: {'TransformationID': 1, 'TargetSE': 'SE1', 'b1': 'bb1', 'Site': 'MySite',
                    'InputData': ['/this/is/a1.lfn', '/this/is/a2.lfn']},
                2: {'TransformationID': 1, 'TargetSE': 'SE2', 'b2': 'bb2',
                    'InputData': "/this/is/a1.lfn;/this/is/a2.lfn"},
                3: {'TransformationID': 2, 'TargetSE': 'SE3', 'b3': 'bb3', 'InputData': ''}}

    res = self.requestTasks.prepareTransformationTasks('', taskDict, 'owner', 'ownerGroup', '/bih/boh/DN')
    self.assertTrue(res['OK'])
    # We should "lose" one of the task in the preparation
    self.assertEqual(len(taskDict), 2)
    for task in res['Value'].values():
      self.assertTrue(isinstance(task['TaskObject'], Request))
      self.assertEqual(task['TaskObject'][0].Type, 'ReplicateAndRegister')
      try:
        self.assertEqual(task['TaskObject'][0][0].LFN, '/this/is/a1.lfn')
      except IndexError:
        self.assertEqual(task['TaskObject'][0].Status, 'Waiting')
      try:
        self.assertEqual(task['TaskObject'][0][1].LFN, '/this/is/a2.lfn')
      except IndexError:
        self.assertEqual(task['TaskObject'][0].Status, 'Waiting')

    # # test another (single) OperationType
    res = self.requestTasks.prepareTransformationTasks('someType;LogUpload', taskDict, 'owner', 'ownerGroup', '/bih/boh/DN')
    self.assertTrue(res['OK'])
    # We should "lose" one of the task in the preparation
    self.assertEqual(len(taskDict), 2)
    for task in res['Value'].values():
      self.assertTrue(isinstance(task['TaskObject'], Request))
      self.assertEqual(task['TaskObject'][0].Type, 'LogUpload')

    # ## Multiple operations
    transBody = [("ReplicateAndRegister", {"SourceSE": "FOO-SRM", "TargetSE": "BAR-SRM"}),
                 ("RemoveReplica", {"TargetSE": "FOO-SRM"}),
                 ]
    jsonBody = json.dumps(transBody)

    taskDict = {1: {'TransformationID': 1, 'TargetSE': 'SE1', 'b1': 'bb1', 'Site': 'MySite',
                    'InputData': ['/this/is/a1.lfn', '/this/is/a2.lfn']},
                2: {'TransformationID': 1, 'TargetSE': 'SE2', 'b2': 'bb2',
                    'InputData': "/this/is/a1.lfn;/this/is/a2.lfn"},
                3: {'TransformationID': 2, 'TargetSE': 'SE3', 'b3': 'bb3',
                    'InputData': ''}}

    res = self.requestTasks.prepareTransformationTasks(jsonBody, taskDict, 'owner', 'ownerGroup', '/bih/boh/DN')
    self.assertTrue(res['OK'])
    # We should "lose" one of the task in the preparation
    self.assertEqual(len(taskDict), 2)
    for task in res['Value'].values():
      self.assertTrue(isinstance(task['TaskObject'], Request))
      self.assertEqual(task['TaskObject'][0].Type, 'ReplicateAndRegister')
      self.assertEqual(task['TaskObject'][1].Type, 'RemoveReplica')
      try:
        self.assertEqual(task['TaskObject'][0][0].LFN, '/this/is/a1.lfn')
        self.assertEqual(task['TaskObject'][1][0].LFN, '/this/is/a1.lfn')
      except IndexError:
        self.assertEqual(task['TaskObject'][0].Status, 'Waiting')
        self.assertEqual(task['TaskObject'][1].Status, 'Waiting')
      try:
        self.assertEqual(task['TaskObject'][0][1].LFN, '/this/is/a2.lfn')
        self.assertEqual(task['TaskObject'][1][1].LFN, '/this/is/a2.lfn')
      except IndexError:
        self.assertEqual(task['TaskObject'][0].Status, 'Waiting')
        self.assertEqual(task['TaskObject'][1].Status, 'Waiting')

      self.assertEqual(task['TaskObject'][0].SourceSE, 'FOO-SRM')
      self.assertEqual(task['TaskObject'][0].TargetSE, 'BAR-SRM')
      self.assertEqual(task['TaskObject'][1].TargetSE, 'FOO-SRM')

#############################################################################


class TransformationClientSuccess(ClientsTestCase):

  def test__applyTransformationFilesStateMachine(self):
    tsFiles = {}
    dictOfNewLFNsStatus = {}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {})

    tsFiles = {}
    dictOfNewLFNsStatus = {'foo': ['status', 2L, 1234]}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {})

    tsFiles = {'foo': ['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo': 'status'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {})

    tsFiles = {'foo': ['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo': 'statusA'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {'foo': 'statusA'})

    tsFiles = {'foo': ['status', 2L, 1234], 'bar': ['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'status'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {})

    tsFiles = {'foo': ['status', 2L, 1234], 'bar': ['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'statusA'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {'foo': 'statusA'})

    tsFiles = {'foo': ['status', 2L, 1234], 'bar': ['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'A', 'bar': 'B'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {'foo': 'A', 'bar': 'B'})

    tsFiles = {'foo': ['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo': 'A', 'bar': 'B'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {'foo': 'A'})

    tsFiles = {'foo': ['Assigned', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo': 'A', 'bar': 'B'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {'foo': 'A'})

    tsFiles = {'foo': ['Assigned', 2L, 1234], 'bar': ['Assigned', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'Assigned', 'bar': 'Processed'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {'bar': 'Processed'})

    tsFiles = {'foo': ['Processed', 2L, 1234], 'bar': ['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'Assigned', 'bar': 'Processed'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {'bar': 'Processed'})

    tsFiles = {'foo': ['Processed', 2L, 1234], 'bar': ['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'Assigned', 'bar': 'Processed'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, True)
    self.assertEqual(res, {'foo': 'Assigned', 'bar': 'Processed'})

    tsFiles = {'foo': ['MaxReset', 12L, 1234], 'bar': ['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'Unused', 'bar': 'Unused'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {})

    tsFiles = {'foo': ['MaxReset', 12L, 1234], 'bar': ['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'Unused', 'bar': 'Unused'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, True)
    self.assertEqual(res, {'foo': 'Unused', 'bar': 'Unused'})

    tsFiles = {'foo': ['Assigned', 20L, 1234], 'bar': ['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'Unused', 'bar': 'Unused'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, False)
    self.assertEqual(res, {'foo': 'MaxReset'})

    tsFiles = {'foo': ['Assigned', 20L, 1234], 'bar': ['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo': 'Unused', 'bar': 'Unused'}
    res = self.tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, True)
    self.assertEqual(res, {'foo': 'Unused', 'bar': 'Unused'})

#############################################################################


class TransformationSuccess(ClientsTestCase):

  def test_setGet(self):

    res = self.transformation.setTransformationName('TestTName')
    self.assertTrue(res['OK'])
    description = 'Test transformation description'
    res = self.transformation.setDescription(description)
    longDescription = 'Test transformation long description'
    res = self.transformation.setLongDescription(longDescription)
    self.assertTrue(res['OK'])
    res = self.transformation.setType('MCSimulation')
    self.assertTrue(res['OK'])
    res = self.transformation.setPlugin('aPlugin')
    self.assertTrue(res['OK'])

    # # Test DataOperation Body

    res = self.transformation.setBody("")
    self.assertTrue(res['OK'])
    self.assertEqual(self.transformation.paramValues["Body"], "")

    res = self.transformation.setBody("_requestType;RemoveReplica")
    self.assertTrue(res['OK'])
    self.assertEqual(self.transformation.paramValues["Body"], "_requestType;RemoveReplica")

    # #Json will turn tuples to lists and strings to unicode
    transBody = [[u"ReplicateAndRegister", {u"SourceSE": u"FOO-SRM", u"TargetSE": u"BAR-SRM"}],
                 [u"RemoveReplica", {u"TargetSE": u"FOO-SRM"}],
                 ]
    res = self.transformation.setBody(transBody)
    self.assertTrue(res['OK'])

    self.assertEqual(self.transformation.paramValues["Body"], json.dumps(transBody))

    # # This is not true if any of the keys or values are not strings, e.g., integers
    self.assertEqual(json.loads(self.transformation.paramValues["Body"]), transBody)

    with self.assertRaisesRegexp(TypeError, "Expected list"):
      self.transformation.setBody({"ReplicateAndRegister": {"foo": "bar"}})
    with self.assertRaisesRegexp(TypeError, "Expected tuple"):
      self.transformation.setBody(["ReplicateAndRegister", "RemoveReplica"])
    with self.assertRaisesRegexp(TypeError, "Expected 2-tuple"):
      self.transformation.setBody([("ReplicateAndRegister", "RemoveReplica", "LogUpload")])
    with self.assertRaisesRegexp(TypeError, "Expected string"):
      self.transformation.setBody([(123, "Parameter:Value")])
    with self.assertRaisesRegexp(TypeError, "Expected dictionary"):
      self.transformation.setBody([("ReplicateAndRegister", "parameter=foo")])
    with self.assertRaisesRegexp(TypeError, "Expected string"):
      self.transformation.setBody([("ReplicateAndRegister", {123: "foo"})])
    with self.assertRaisesRegexp(ValueError, "Unknown attribute"):
      self.transformation.setBody([("ReplicateAndRegister", {"Request": Request()})])
    with self.assertRaisesRegexp(TypeError, "Cannot encode"):
      self.transformation.setBody([("ReplicateAndRegister", {"Arguments": Request()})])

  def test_SetGetReset(self):
    """ Testing of the set, get and reset methods.

          set*()
          get*()
          setTargetSE()
          setSourceSE()
          getTargetSE()
          getSourceSE()
          reset()
        Ensures that after a reset all parameters are returned to their defaults
    """

    res = self.transformation.getParameters()
    self.assertTrue(res['OK'])
    defaultParams = res['Value'].copy()
    for parameterName, defaultValue in res['Value'].items():
      if isinstance(defaultValue, basestring):
        testValue = 'TestValue'
      else:
        testValue = 99999
      # # set*

      setterName = 'set%s' % parameterName
      self.assertTrue(hasattr(self.transformation, setterName))
      setter = getattr(self.transformation, setterName)
      self.assertTrue(callable(setter))
      res = setter(testValue)
      self.assertTrue(res['OK'])
      # # get*
      getterName = "get%s" % parameterName
      self.assertTrue(hasattr(self.transformation, getterName))
      getter = getattr(self.transformation, getterName)
      self.assertTrue(callable(getter))
      res = getter()
      self.assertTrue(res['OK'])
      self.assertTrue(res['Value'], testValue)

    res = self.transformation.reset()
    self.assertTrue(res['OK'])
    res = self.transformation.getParameters()
    self.assertTrue(res['OK'])
    for parameterName, resetValue in res['Value'].items():
      self.assertEqual(resetValue, defaultParams[parameterName])
    self.assertRaises(AttributeError, self.transformation.getTargetSE)
    self.assertRaises(AttributeError, self.transformation.getSourceSE)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TaskBaseSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(WorkflowTasksSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PluginUtilitiesSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RequestTasksSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransformationClientSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransformationSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
