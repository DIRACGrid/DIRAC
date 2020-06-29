""" Testing the API and a bit more.
    It will submit a number of test jobs locally (via runLocal), using the python unittest to assess the results.
    Can be automatized.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=protected-access, wrong-import-position, invalid-name, missing-docstring

import os
import sys
import unittest
import multiprocessing

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger, rootPath
from DIRAC.tests.Utilities.IntegrationTest import IntegrationTest
from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac


class UserJobTestCase(IntegrationTest):
  """ Base class for the UserJob test cases
  """

  def setUp(self):
    super(UserJobTestCase, self).setUp()

    self.d = Dirac()

    integration_test_dir = '/DIRAC/tests/Workflow/Integration'
    try:
      self.exeScriptLocation = find_all('exe-script.py', rootPath, integration_test_dir)[0]
      self.helloWorld = find_all("helloWorld.py", rootPath, integration_test_dir)[0]
      self.mpExe = find_all('mpTest.py', rootPath, '/DIRAC/tests/Utilities')[0]
      self.mpExeFlex = find_all('mpTest-flexible.py', rootPath, '/DIRAC/tests/Utilities')[0]
    except IndexError:  # we are in Jenkins
      self.exeScriptLocation = find_all('exe-script.py', os.environ['WORKSPACE'], integration_test_dir)[0]
      self.helloWorld = find_all("helloWorld.py", os.environ['WORKSPACE'], integration_test_dir)[0]
      self.mpExe = find_all('mpTest.py', os.environ['WORKSPACE'], '/DIRAC/tests/Utilities')[0]
      self.mpExeFlex = find_all('mpTest-flexible.py', os.environ['WORKSPACE'], '/DIRAC/tests/Utilities')[0]

    gLogger.setLevel('DEBUG')


class HelloWorldSuccess(UserJobTestCase):
  def test_execute(self):

    j = Job()

    j.setName("helloWorld-test")
    j.setExecutable(self.exeScriptLocation)
    j.setLogLevel('DEBUG')
    try:
      # This is the standard location in Jenkins
      j.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      j.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    j.setConfigArgs('pilot.cfg')
    res = j.runLocal(self.d)
    self.assertTrue(res['OK'])


class HelloWorldPlusSuccess(UserJobTestCase):
  """ Adding quite a lot of calls from the API, for pure test purpose
  """

  def test_execute(self):

    job = Job()
    job._siteSet = {'DIRAC.someSite.ch'}

    job.setName("helloWorld-test")
    job.setExecutable(self.helloWorld,
                      arguments="This is an argument",
                      logFile="aLogFileForTest.txt",
                      parameters=[('executable', 'string', '', "Executable Script"),
                                  ('arguments', 'string', '', 'Arguments for executable Script'),
                                  ('applicationLog', 'string', '', "Log file name"),
                                  ('someCustomOne', 'string', '', "boh")],
                      paramValues=[('someCustomOne', 'aCustomValue')])
    job.setBannedSites(['LCG.SiteA.com', 'DIRAC.SiteB.org'])
    job.setOwner('ownerName')
    job.setOwnerGroup('ownerGroup')
    job.setName('jobName')
    job.setJobGroup('jobGroup')
    job.setType('jobType')
    job.setDestination('DIRAC.someSite.ch')
    job.setCPUTime(12345)
    job.setLogLevel('DEBUG')
    try:
      # This is the standard location in Jenkins
      job.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      job.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    job.setConfigArgs('pilot.cfg')

    res = job.runLocal(self.d)
    self.assertTrue(res['OK'])

  def test_execute_success(self):

    job = Job()
    job._siteSet = {'DIRAC.someSite.ch'}

    job.setName("helloWorld-test")
    job.setExecutable(self.helloWorld,
                      logFile="aLogFileForTest.txt",
                      parameters=[('executable', 'string', '', "Executable Script"),
                                  ('arguments', 'string', '', 'Arguments for executable Script'),
                                  ('applicationLog', 'string', '', "Log file name"),
                                  ('someCustomOne', 'string', '', "boh")],
                      paramValues=[('someCustomOne', 'aCustomValue')])
    job.setBannedSites(['LCG.SiteA.com', 'DIRAC.SiteB.org'])
    job.setOwner('ownerName')
    job.setOwnerGroup('ownerGroup')
    job.setName('jobName')
    job.setJobGroup('jobGroup')
    job.setType('jobType')
    job.setDestination('DIRAC.someSite.ch')
    job.setCPUTime(12345)
    job.setLogLevel('DEBUG')
    try:
      # This is the standard location in Jenkins
      job.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      job.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    job.setConfigArgs('pilot.cfg')

    res = job.runLocal(self.d)
    self.assertTrue(res['OK'])


class LSSuccess(UserJobTestCase):
  def test_execute(self):
    """ just testing unix "ls"
    """

    job = Job()

    job.setName("ls-test")
    job.setExecutable("/bin/ls", '-l')
    job.setLogLevel('DEBUG')
    try:
      # This is the standard location in Jenkins
      job.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      job.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    job.setConfigArgs('pilot.cfg')
    res = job.runLocal(self.d)
    self.assertTrue(res['OK'])


class MPSuccess(UserJobTestCase):

  def test_fixed(self):
    """ this tests executes a job that requires exactly 4 processors
    """

    j = Job()

    j.setName("MP-test")
    j.setExecutable(self.mpExe)
    j.setInputSandbox(find_all('mpTest.py', rootPath, 'DIRAC/tests/Utilities')[0])
    j.setNumberOfProcessors(4)  # This requires a fixed number of processors
    j.setLogLevel('DEBUG')
    try:
      # This is the standard location in Jenkins
      j.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      j.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    j.setConfigArgs('pilot.cfg')
    res = j.runLocal(self.d)
    if multiprocessing.cpu_count() > 1:
      self.assertTrue(res['OK'])
    else:
      self.assertFalse(res['OK'])


class MPSuccessMinMax(UserJobTestCase):

  def test_min2(self):
    """ this tests executes a job that requires at least 2 processors
    """

    j = Job()

    j.setName("MP-test-min2")

    # FIXME: the number of processors should be discovered at runtime using JobParameters.getNumberOfJobProcessors()
    # here, and later
    j.setExecutable(self.mpExeFlex, arguments='2')
    j.setInputSandbox(find_all('mpTest-flexible.py', rootPath, 'DIRAC/tests/Utilities')[0])
    j.setNumberOfProcessors(minNumberOfProcessors=2)  # This requires at least 2 processors
    j.setLogLevel('DEBUG')
    try:
      # This is the standard location in Jenkins
      j.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      j.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    j.setConfigArgs('pilot.cfg')
    res = j.runLocal(self.d)
    self.assertTrue(res['OK'])

  def test_min2max4(self):
    """ this tests executes a job that requires 2 to 4 processors
    """

    j = Job()

    j.setName("MP-test-min2max4")
    j.setExecutable(self.mpExeFlex, arguments='2')
    j.setInputSandbox(find_all('mpTest-flexible.py', rootPath, 'DIRAC/tests/Utilities')[0])
    j.setNumberOfProcessors(minNumberOfProcessors=2, maxNumberOfProcessors=4)  # This requires 2 to 4 processors
    j.setLogLevel('DEBUG')
    try:
      # This is the standard location in Jenkins
      j.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      j.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    j.setConfigArgs('pilot.cfg')
    res = j.runLocal(self.d)
    self.assertTrue(res['OK'])

  def test_min1(self):
    """ this tests executes a job that requires at least 1 processor
    """

    j = Job()

    j.setName("MP-test-min1")
    j.setExecutable(self.mpExeFlex, arguments='2')
    j.setInputSandbox(find_all('mpTest-flexible.py', rootPath, 'DIRAC/tests/Utilities')[0])
    j.setNumberOfProcessors(minNumberOfProcessors=1)  # This requires 1 to infinite processors
    j.setLogLevel('DEBUG')
    try:
      # This is the standard location in Jenkins
      j.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      j.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    j.setConfigArgs('pilot.cfg')
    res = j.runLocal(self.d)
    self.assertTrue(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UserJobTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldPlusSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(LSSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MPSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MPSuccessMinMax))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
