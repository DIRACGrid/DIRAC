""" unit tests for Transformation Clients
"""
# pylint: disable=protected-access,missing-docstring

import unittest
import json
from unittest import mock

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.TransformationSystem.Client.TaskManager import TaskBase
from DIRAC.TransformationSystem.Client.RequestTasks import RequestTasks
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.Utilities import PluginUtilities
from DIRAC.TransformationSystem.Client.BodyPlugin.DummyBody import DummyBody


class reqValFake_C:
    def validate(self, opsInput):
        for ops in opsInput:
            if not len(ops):
                return {"OK": False}
            for f in ops:
                try:
                    if not f.LFN:
                        return {"OK": False}
                except Exception:
                    return {"OK": False}
        return {"OK": True}


reqValFake = reqValFake_C()


class ClientsTestCase(unittest.TestCase):
    """Base class for the clients test cases"""

    def setUp(self):
        self.mockTransClient = mock.MagicMock()
        self.mockTransClient.setTaskStatusAndWmsID.return_value = {"OK": True}

        self.mockReqClient = mock.MagicMock()

        self.taskBase = TaskBase(transClient=self.mockTransClient)
        self.pu = PluginUtilities(transClient=self.mockTransClient)

        self.requestTasks = RequestTasks(
            transClient=self.mockTransClient, requestClient=self.mockReqClient, requestValidator=reqValFake
        )
        self.transformation = Transformation()

        self.maxDiff = None

    def tearDown(self):
        pass


class TaskBaseSuccess(ClientsTestCase):
    def test_updateDBAfterTaskSubmission(self):
        res = self.taskBase.updateDBAfterTaskSubmission({})
        self.assertEqual(res["OK"], True)


class PluginUtilitiesSuccess(ClientsTestCase):
    def test_groupByReplicas(self):
        res = self.pu.groupByReplicas(
            {
                "/this/is/at.1": ["SE1"],
                "/this/is/at.12": ["SE1", "SE2"],
                "/this/is/at.2": ["SE2"],
                "/this/is/at_123": ["SE1", "SE2", "SE3"],
                "/this/is/at_23": ["SE2", "SE3"],
                "/this/is/at_4": ["SE4"],
            },
            "Flush",
        )
        self.assertTrue(res["OK"])
        self.assertEqual(
            res["Value"],
            [
                ("SE1", ["/this/is/at.1"]),
                ("SE1,SE2", ["/this/is/at.12"]),
                ("SE1,SE2,SE3", ["/this/is/at_123"]),
                ("SE2", ["/this/is/at.2"]),
                ("SE2,SE3", ["/this/is/at_23"]),
                ("SE4", ["/this/is/at_4"]),
            ],
        )

        res = self.pu.groupByReplicas(
            {
                "/this/is/at.123": ["SE1", "SE2", "SE3"],
                "/this/is/at.12": ["SE1", "SE2"],
                "/this/is/at.134": ["SE1", "SE3", "SE4"],
            },
            "Flush",
        )
        self.assertTrue(res["OK"])
        self.assertEqual(
            res["Value"],
            [
                ("SE1,SE2", ["/this/is/at.12"]),
                ("SE1,SE2,SE3", ["/this/is/at.123"]),
                ("SE1,SE3,SE4", ["/this/is/at.134"]),
            ],
        )


class RequestTasksSuccess(ClientsTestCase):
    def test_prepareTranformationTasks(self):

        # No tasks in input
        taskDict = {}
        res = self.requestTasks.prepareTransformationTasks("", taskDict, "owner", "ownerGroup", "/bih/boh/DN")
        self.assertTrue(res["OK"])
        self.assertEqual(len(taskDict), 0)

        # 3 tasks, 1 task not OK (in second transformation)
        taskDict = {123: {"TransformationID": 2, "TargetSE": "SE3", "b3": "bb3", "InputData": ""}}
        res = self.requestTasks.prepareTransformationTasks("", taskDict, "owner", "ownerGroup", "/bih/boh/DN")
        self.assertTrue(res["OK"])
        # We should "lose" one of the task in the preparation
        self.assertEqual(len(taskDict), 0)

        taskDict = {
            1: {
                "TransformationID": 1,
                "TargetSE": "SE1",
                "b1": "bb1",
                "Site": "MySite",
                "InputData": ["/this/is/a1.lfn", "/this/is/a2.lfn"],
            },
            2: {"TransformationID": 1, "TargetSE": "SE2", "b2": "bb2", "InputData": "/this/is/a1.lfn;/this/is/a2.lfn"},
            3: {"TransformationID": 2, "TargetSE": "SE3", "b3": "bb3", "InputData": ""},
        }

        res = self.requestTasks.prepareTransformationTasks("", taskDict, "owner", "ownerGroup", "/bih/boh/DN")
        self.assertTrue(res["OK"])
        # We should "lose" one of the task in the preparation
        self.assertEqual(len(taskDict), 2)
        for task in res["Value"].values():
            self.assertTrue(isinstance(task["TaskObject"], Request))
            self.assertEqual(task["TaskObject"][0].Type, "ReplicateAndRegister")
            try:
                self.assertEqual(task["TaskObject"][0][0].LFN, "/this/is/a1.lfn")
            except IndexError:
                self.assertEqual(task["TaskObject"][0].Status, "Waiting")
            try:
                self.assertEqual(task["TaskObject"][0][1].LFN, "/this/is/a2.lfn")
            except IndexError:
                self.assertEqual(task["TaskObject"][0].Status, "Waiting")

        # # test another (single) OperationType
        res = self.requestTasks.prepareTransformationTasks(
            "someType;LogUpload", taskDict, "owner", "ownerGroup", "/bih/boh/DN"
        )
        self.assertTrue(res["OK"])
        # We should "lose" one of the task in the preparation
        self.assertEqual(len(taskDict), 2)
        for task in res["Value"].values():
            self.assertTrue(isinstance(task["TaskObject"], Request))
            self.assertEqual(task["TaskObject"][0].Type, "LogUpload")

        # ## Multiple operations
        transBody = [
            ("ReplicateAndRegister", {"SourceSE": "FOO-SRM", "TargetSE": "BAR-SRM"}),
            ("RemoveReplica", {"TargetSE": "FOO-SRM"}),
        ]
        jsonBody = json.dumps(transBody)

        taskDict = {
            1: {
                "TransformationID": 1,
                "TargetSE": "SE1",
                "b1": "bb1",
                "Site": "MySite",
                "InputData": ["/this/is/a1.lfn", "/this/is/a2.lfn"],
            },
            2: {"TransformationID": 1, "TargetSE": "SE2", "b2": "bb2", "InputData": "/this/is/a1.lfn;/this/is/a2.lfn"},
            3: {"TransformationID": 2, "TargetSE": "SE3", "b3": "bb3", "InputData": ""},
        }

        res = self.requestTasks.prepareTransformationTasks(jsonBody, taskDict, "owner", "ownerGroup", "/bih/boh/DN")
        self.assertTrue(res["OK"])
        # We should "lose" one of the task in the preparation
        self.assertEqual(len(taskDict), 2)
        for task in res["Value"].values():
            self.assertTrue(isinstance(task["TaskObject"], Request))
            self.assertEqual(task["TaskObject"][0].Type, "ReplicateAndRegister")
            self.assertEqual(task["TaskObject"][1].Type, "RemoveReplica")
            try:
                self.assertEqual(task["TaskObject"][0][0].LFN, "/this/is/a1.lfn")
                self.assertEqual(task["TaskObject"][1][0].LFN, "/this/is/a1.lfn")
            except IndexError:
                self.assertEqual(task["TaskObject"][0].Status, "Waiting")
                self.assertEqual(task["TaskObject"][1].Status, "Waiting")
            try:
                self.assertEqual(task["TaskObject"][0][1].LFN, "/this/is/a2.lfn")
                self.assertEqual(task["TaskObject"][1][1].LFN, "/this/is/a2.lfn")
            except IndexError:
                self.assertEqual(task["TaskObject"][0].Status, "Waiting")
                self.assertEqual(task["TaskObject"][1].Status, "Waiting")

            self.assertEqual(task["TaskObject"][0].SourceSE, "FOO-SRM")
            self.assertEqual(task["TaskObject"][0].TargetSE, "BAR-SRM")
            self.assertEqual(task["TaskObject"][1].TargetSE, "FOO-SRM")


class TransformationSuccess(ClientsTestCase):
    def test_setGet(self):

        res = self.transformation.setTransformationName("TestTName")
        self.assertTrue(res["OK"])
        description = "Test transformation description"
        res = self.transformation.setDescription(description)
        longDescription = "Test transformation long description"
        res = self.transformation.setLongDescription(longDescription)
        self.assertTrue(res["OK"])
        res = self.transformation.setType("MCSimulation")
        self.assertTrue(res["OK"])
        res = self.transformation.setPlugin("aPlugin")
        self.assertTrue(res["OK"])

        # # Test DataOperation Body

        res = self.transformation.setBody("")
        self.assertTrue(res["OK"])
        self.assertEqual(self.transformation.paramValues["Body"], "")

        res = self.transformation.setBody("_requestType;RemoveReplica")
        self.assertTrue(res["OK"])
        self.assertEqual(self.transformation.paramValues["Body"], "_requestType;RemoveReplica")

        # #Json will turn tuples to lists and strings to unicode
        transBody = [
            ["ReplicateAndRegister", {"SourceSE": "FOO-SRM", "TargetSE": "BAR-SRM"}],
            ["RemoveReplica", {"TargetSE": "FOO-SRM"}],
        ]
        res = self.transformation.setBody(transBody)
        self.assertTrue(res["OK"])

        self.assertEqual(self.transformation.paramValues["Body"], json.dumps(transBody))

        # # This is not true if any of the keys or values are not strings, e.g., integers
        self.assertEqual(json.loads(self.transformation.paramValues["Body"]), transBody)

        with self.assertRaisesRegex(TypeError, "Expected list"):
            self.transformation.setBody({"ReplicateAndRegister": {"foo": "bar"}})
        with self.assertRaisesRegex(TypeError, "Expected tuple"):
            self.transformation.setBody(["ReplicateAndRegister", "RemoveReplica"])
        with self.assertRaisesRegex(TypeError, "Expected 2-tuple"):
            self.transformation.setBody([("ReplicateAndRegister", "RemoveReplica", "LogUpload")])
        with self.assertRaisesRegex(TypeError, "Expected string"):
            self.transformation.setBody([(123, "Parameter:Value")])
        with self.assertRaisesRegex(TypeError, "Expected dictionary"):
            self.transformation.setBody([("ReplicateAndRegister", "parameter=foo")])
        with self.assertRaisesRegex(TypeError, "Expected string"):
            self.transformation.setBody([("ReplicateAndRegister", {123: "foo"})])
        with self.assertRaisesRegex(ValueError, "Unknown attribute"):
            self.transformation.setBody([("ReplicateAndRegister", {"Request": Request()})])
        with self.assertRaisesRegex(TypeError, "Cannot encode"):
            self.transformation.setBody([("ReplicateAndRegister", {"Arguments": Request()})])

        # Check that all tuples are checked by passing first a valid one,
        # then a faulty one.
        # It is enough to check one case, unlike above
        with self.assertRaisesRegex(TypeError, "Expected 2-tuple"):
            self.transformation.setBody([("RemoveReplica", {"TargetSE": "FOO-SRM"}), ("One", "too long", "tuple")])

        # Test setting a body plugin as body
        complexBody = DummyBody()
        self.transformation.setBody(complexBody)

    def test_SetGetReset(self):
        """Testing of the set, get and reset methods.

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
        self.assertTrue(res["OK"])
        defaultParams = res["Value"].copy()
        for parameterName, defaultValue in res["Value"].items():
            if isinstance(defaultValue, str):
                testValue = "TestValue"
            else:
                testValue = 99999
            # # set*

            setterName = "set%s" % parameterName
            self.assertTrue(hasattr(self.transformation, setterName))
            setter = getattr(self.transformation, setterName)
            self.assertTrue(callable(setter))
            res = setter(testValue)
            self.assertTrue(res["OK"])
            # # get*
            getterName = "get%s" % parameterName
            self.assertTrue(hasattr(self.transformation, getterName))
            getter = getattr(self.transformation, getterName)
            self.assertTrue(callable(getter))
            res = getter()
            self.assertTrue(res["OK"])
            self.assertTrue(res["Value"], testValue)

        res = self.transformation.reset()
        self.assertTrue(res["OK"])
        res = self.transformation.getParameters()
        self.assertTrue(res["OK"])
        for parameterName, resetValue in res["Value"].items():
            self.assertEqual(resetValue, defaultParams[parameterName])
        self.assertRaises(AttributeError, self.transformation.getTargetSE)
        self.assertRaises(AttributeError, self.transformation.getSourceSE)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TaskBaseSuccess))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PluginUtilitiesSuccess))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RequestTasksSuccess))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransformationSuccess))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
