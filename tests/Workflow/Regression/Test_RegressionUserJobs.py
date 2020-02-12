""" This module will run some job descriptions defined with an older version of DIRAC
"""

# pylint: disable=protected-access, wrong-import-position, invalid-name, missing-docstring

import unittest
import os
import sys
import shutil

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger, rootPath

from DIRAC.tests.Utilities.utils import find_all

from DIRAC.tests.Utilities.IntegrationTest import IntegrationTest

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac


class RegressionTestCase(IntegrationTest):
  """ Base class for the Regression test cases
  """

  def setUp(self):
    super(RegressionTestCase, self).setUp()

    gLogger.setLevel('DEBUG')
    self.dirac = Dirac()

    try:
      exeScriptLoc = find_all('exe-script.py', rootPath, '/DIRAC/tests/Workflow')[0]
      helloWorldLoc = find_all('helloWorld.py', rootPath, '/DIRAC/tests/Workflow')[0]
    except IndexError:  # we are in Jenkins
      exeScriptLoc = find_all('exe-script.py', os.environ['WORKSPACE'], '/DIRAC/tests/Workflow')[0]
      helloWorldLoc = find_all('helloWorld.py', os.environ['WORKSPACE'], '/DIRAC/tests/Workflow')[0]

    shutil.copyfile(exeScriptLoc, './exe-script.py')
    shutil.copyfile(helloWorldLoc, './helloWorld.py')

    try:
      helloWorldXMLLocation = find_all('helloWorld.xml', rootPath, '/DIRAC/tests/Workflow/Regression')[0]
    except IndexError:  # we are in Jenkins
      helloWorldXMLLocation = find_all('helloWorld.xml', os.environ['WORKSPACE'], '/DIRAC/tests/Workflow/Regression')[0]

    self.j_u_hello = Job(helloWorldXMLLocation)
    self.j_u_hello.setConfigArgs('pilot.cfg')

    try:
      helloWorldXMLFewMoreLocation = find_all('helloWorld.xml', rootPath, '/DIRAC/tests/Workflow/Regression')[0]
    except IndexError:  # we are in Jenkins
      helloWorldXMLFewMoreLocation = find_all(
          'helloWorld.xml',
          os.environ['WORKSPACE'],
          '/DIRAC/tests/Workflow/Regression')[0]

    self.j_u_helloPlus = Job(helloWorldXMLFewMoreLocation)
    self.j_u_helloPlus.setConfigArgs('pilot.cfg')

  def tearDown(self):
    try:
      os.remove('exe-script.py')
      os.remove('helloWorld.py')
    except OSError:
      pass


class HelloWorldSuccess(RegressionTestCase):
  def test_Regression_User(self):
    res = self.j_u_hello.runLocal(self.dirac)
    self.assertTrue(res['OK'])


class HelloWorldPlusSuccess(RegressionTestCase):
  def test_Regression_User(self):
    res = self.j_u_helloPlus.runLocal(self.dirac)
    self.assertTrue(res['OK'])

#############################################################################
# Test Suite run
#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RegressionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldPlusSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
