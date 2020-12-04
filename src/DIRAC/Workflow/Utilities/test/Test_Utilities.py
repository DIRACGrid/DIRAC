from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import unittest

from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Workflow.Utilities.Utils import getStepDefinition, getStepCPUTimes

#############################################################################


class UtilitiesTestCase(unittest.TestCase):
  """ Base class
  """

  def setUp(self):

    self.job = Job()
    pass


class UtilsSuccess(UtilitiesTestCase):

  def test__getStepDefinition(self):
    importLine = """
from DIRAC.Workflow.Modules.<MODULE> import <MODULE>
"""
    # modules
    gaudiApp = ModuleDefinition('Script')
    body = importLine.replace('<MODULE>', 'Script')
    gaudiApp.setDescription(getattr(__import__("%s.%s" % ('DIRAC.Workflow.Modules', 'Script'),
                                               globals(), locals(), ['__doc__']),
                                    "__doc__"))
    gaudiApp.setBody(body)

    genBKReport = ModuleDefinition('FailoverRequest')
    body = importLine.replace('<MODULE>', 'FailoverRequest')
    genBKReport.setDescription(getattr(__import__("%s.%s" % ('DIRAC.Workflow.Modules', 'FailoverRequest'),
                                                  globals(), locals(), ['__doc__']),
                                       "__doc__"))
    genBKReport.setBody(body)

    # step
    appDefn = StepDefinition('App_Step')
    appDefn.addModule(gaudiApp)
    appDefn.createModuleInstance('Script', 'Script')
    appDefn.addModule(genBKReport)
    appDefn.createModuleInstance('FailoverRequest', 'FailoverRequest')

    appDefn.addParameterLinked(gaudiApp.parameters)

    stepDef = getStepDefinition('App_Step', ['Script', 'FailoverRequest'])

    self.assertTrue(str(appDefn) == str(stepDef))

    self.job._addParameter(appDefn, 'name', 'type', 'value', 'desc')
    self.job._addParameter(appDefn, 'name1', 'type1', 'value1', 'desc1')

    stepDef = getStepDefinition('App_Step', ['Script', 'FailoverRequest'],
                                parametersList=[['name', 'type', 'value', 'desc'],
                                                ['name1', 'type1', 'value1', 'desc1']])

    self.assertTrue(str(appDefn) == str(stepDef))

  def test_getStepCPUTimes(self):
    execT, cpuT = getStepCPUTimes({})
    self.assertEqual(execT, 0)
    self.assertEqual(cpuT, 0)
    execT, cpuT = getStepCPUTimes({'StartTime': 0, 'StartStats': (0, 0, 0, 0, 0)})
    print(execT, cpuT)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UtilitiesTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(UtilsSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
