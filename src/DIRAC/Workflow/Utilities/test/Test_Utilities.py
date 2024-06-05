# pylint: disable=invalid-name,missing-docstring,protected-access

from importlib import import_module
import unittest

from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Workflow.Utilities.Utils import getStepDefinition, getStepCPUTimes


class UtilitiesTestCase(unittest.TestCase):
    """Base class"""

    def setUp(self):
        self.job = Job()


class UtilsSuccess(UtilitiesTestCase):
    def test__getStepDefinition(self):
        importLine = """
from DIRAC.Workflow.Modules.<MODULE> import <MODULE>
"""
        # modules
        gaudiApp = ModuleDefinition("Script")
        body = importLine.replace("<MODULE>", "Script")
        gaudiApp.setDescription(getattr(import_module("DIRAC.Workflow.Modules.Script"), "__doc__"))
        gaudiApp.setBody(body)

        genBKReport = ModuleDefinition("FailoverRequest")
        body = importLine.replace("<MODULE>", "FailoverRequest")
        genBKReport.setDescription(
            getattr(
                import_module("DIRAC.Workflow.Modules.FailoverRequest"),
                "__doc__",
            )
        )
        genBKReport.setBody(body)

        # step
        appDefn = StepDefinition("App_Step")
        appDefn.addModule(gaudiApp)
        appDefn.createModuleInstance("Script", "Script")
        appDefn.addModule(genBKReport)
        appDefn.createModuleInstance("FailoverRequest", "FailoverRequest")

        appDefn.addParameterLinked(gaudiApp.parameters)

        stepDef = getStepDefinition("App_Step", ["Script", "FailoverRequest"])

        assert str(appDefn) == str(stepDef)

        self.job._addParameter(appDefn, "name", "type", "value", "desc")
        self.job._addParameter(appDefn, "name1", "type1", "value1", "desc1")

        stepDef = getStepDefinition(
            "App_Step",
            ["Script", "FailoverRequest"],
            parametersList=[["name", "type", "value", "desc"], ["name1", "type1", "value1", "desc1"]],
        )

        assert str(appDefn) == str(stepDef)

    def test_getStepCPUTimes(self):
        execT, cpuT = getStepCPUTimes({})
        self.assertEqual(execT, 0)
        self.assertEqual(cpuT, 0)
        execT, cpuT = getStepCPUTimes({"StartTime": 0, "StartStats": (0, 0, 0, 0, 0)})
        print(execT, cpuT)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(UtilitiesTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(UtilsSuccess))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
