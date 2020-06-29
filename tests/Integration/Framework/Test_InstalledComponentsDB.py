"""
Tests the ComponentMonitoring DB and Service by creating, checking,
updating and removing several instances of each table in the DB
This program assumes that the service Framework/ComponentMonitoring is running
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

import unittest
import sys
import datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient


class TestClientComponentMonitoring(unittest.TestCase):
  """
  TestCase-inheriting class with setUp and tearDown methods
  """

  def setUp(self):
    """
    Initialize the client on every test
    """
    self.client = ComponentMonitoringClient()

  def tearDown(self):
    """
    Nothing is done on termination
    """
    pass


class ComponentMonitoringClientChain(TestClientComponentMonitoring):
  """
  Contains methods for testing of separate elements
  """

  def testComponents(self):
    """
    Test the Components database operations
    """

    # Create a sample component
    result = self.client.addComponent({'System': 'Test',
                                       'Module': 'TestModule',
                                       'Type': 'TestingFeature'})

    self.assertTrue(result['OK'])

    # Check if the component exists
    result = self.client.getComponents({'System': 'Test',
                                        'Module': 'TestModule',
                                        'Type': 'TestingFeature'},
                                       False,
                                       False)

    self.assertTrue(result['OK'] and len(result['Value']) > 0)

    # Update the fields of the created component
    result = self.client.updateComponents({'System': 'Test',
                                           'Module': 'TestModule',
                                           'Type': 'TestingFeature'},
                                          {'Module': 'NewTestModule'})

    self.assertTrue(result['OK'])

    # Check if the component with the modified fields exists
    result = self.client.getComponents({'System': 'Test',
                                        'Module': 'NewTestModule',
                                        'Type': 'TestingFeature'},
                                       False,
                                       False)

    self.assertTrue(result['OK'] and len(result['Value']) > 0)

    # Remove the Component
    result = self.client.removeComponents({'System': 'Test',
                                           'Module': 'NewTestModule',
                                           'Type': 'TestingFeature'})

    self.assertTrue(result['OK'])

    # Check if the component was actually removed
    result = self.client.getComponents({'System': 'Test',
                                        'Module': 'NewTestModule',
                                        'Type': 'TestingFeature'},
                                       False,
                                       False)

    self.assertTrue(result['OK'] and len(result['Value']) <= 0)

    # Try to create an incomplete component
    result = self.client.addComponent({'System': 'Test'})

    self.assertFalse(result['OK'])

    # Multiple removal
    self.client.addComponent({'System': 'Test',
                              'Module': 'TestModule1',
                              'Type': 'TestingFeature1'})
    self.client.addComponent({'System': 'Test',
                              'Module': 'TestModule2',
                              'Type': 'TestingFeature1'})
    self.client.addComponent({'System': 'Test',
                              'Module': 'TestModule1',
                              'Type': 'TestingFeature2'})

    self.client.removeComponents({'System': 'Test', 'Module': 'TestModule1'})

    result = self.client.getComponents({'System': 'Test',
                                        'Module': 'TestModule2',
                                        'Type': 'TestingFeature1'},
                                       False,
                                       False)

    self.assertTrue(result['OK'] and len(result['Value']) >= 1)

    result = self.client.getComponents({'System': 'Test',
                                        'Module': 'TestModule1'},
                                       False,
                                       False)

    self.assertTrue(result['OK'] and len(result['Value']) <= 0)

    self.client.removeComponents({'System': 'Test',
                                  'Module': 'TestModule2',
                                  'Type': 'TestingFeature1'})

    self.assertTrue(result['OK'])

  def testHosts(self):
    """
    Tests the Hosts database operations
    """

    # Create a sample host
    result = self.client.addHost({'HostName': 'TestHost', 'CPU': 'TestCPU'})

    self.assertTrue(result['OK'])

    # Check if the host exists
    result = self.client.getHosts({'HostName': 'TestHost',
                                   'CPU': 'TestCPU'},
                                  False,
                                  False)

    self.assertTrue(result['OK'] and len(result['Value']) > 0)

    # Update the fields of the created host
    result = self.client.updateHosts({'HostName': 'TestHost',
                                      'CPU': 'TestCPU'},
                                     {'HostName': 'StillATestHost'})

    self.assertTrue(result['OK'])

    # Check if the host with the modified fields exists
    result = self.client.getHosts({'HostName': 'StillATestHost',
                                   'CPU': 'TestCPU'},
                                  False,
                                  False)

    self.assertTrue(result['OK'] and len(result['Value']) > 0)

    # Remove the Host
    result = self.client.removeHosts({'HostName': 'StillATestHost',
                                      'CPU': 'TestCPU'})

    self.assertTrue(result['OK'])

    # Check if the host was actually removed
    result = self.client.getHosts({'HostName': 'StillATestHost',
                                   'CPU': 'TestCPU'},
                                  False,
                                  False)

    self.assertTrue(result['OK'] and len(result['Value']) <= 0)

    # Try to create an incomplete host
    result = self.client.addHost({'HostName': 'TestHost'})

    self.assertFalse(result['OK'])

    # Multiple removal
    self.client.addHost({'HostName': 'TestHost', 'CPU': 'TestCPU1'})
    self.client.addHost({'HostName': 'TestHost', 'CPU': 'TestCPU2'})
    self.client.addHost({'HostName': 'TestHost', 'CPU': 'TestCPU1'})

    self.client.removeHosts({'CPU': 'TestCPU1'})

    result = self.client.getHosts({'HostName': 'TestHost',
                                   'CPU': 'TestCPU2'},
                                  False,
                                  False)

    self.assertTrue(result['OK'] and len(result['Value']) >= 1)

    result = self.client.getHosts({'HostName': 'TestHost',
                                   'CPU': 'TestCPU1'},
                                  False,
                                  False)

    self.assertTrue(result['OK'] and len(result['Value']) <= 0)

    self.client.removeHosts({'HostName': 'TestHost', 'CPU': 'TestCPU2'})

    self.assertTrue(result['OK'])

  def testInstallations(self):
    """
    Test the InstalledComponents database operations
    """

    # Create a sample installation
    result = self.client.addInstallation({'InstallationTime': datetime.datetime.now(),
                                          'UnInstallationTime': datetime.datetime.now(),
                                          'Instance': 'TestInstallA111'},
                                         {'System': 'UnexistentSystem',
                                          'Module': 'UnexistentModule',
                                          'Type': 'UnexistentType'},
                                         {'HostName': 'fictional',
                                          'CPU': 'TestCPU'},
                                         True)

    self.assertTrue(result['OK'])

    # Check if the installation exists
    result = self.client.getInstallations({'Instance': 'TestInstallA111'},
                                          {'System': 'UnexistentSystem',
                                           'Module': 'UnexistentModule',
                                           'Type': 'UnexistentType'},
                                          {'HostName': 'fictional',
                                           'CPU': 'TestCPU'},
                                          False)

    self.assertTrue(result['OK'] and len(result['Value']) > 0)

    # Update the fields of the created installation
    result = self.client.updateInstallations({'Instance': 'TestInstallA111'},
                                             {'System': 'UnexistentSystem',
                                              'Module': 'UnexistentModule',
                                              'Type': 'UnexistentType'},
                                             {'HostName': 'fictional',
                                              'CPU': 'TestCPU'},
                                             {'Instance': 'TestInstallA222'}
                                             )

    self.assertTrue(result['OK'])

    # Check if the installation with the modified fields exists
    result = self.client.getInstallations({'Instance': 'TestInstallA222'},
                                          {'System': 'UnexistentSystem',
                                           'Module': 'UnexistentModule',
                                           'Type': 'UnexistentType'},
                                          {'HostName': 'fictional',
                                           'CPU': 'TestCPU'},
                                          False)

    self.assertTrue(result['OK'] and len(result['Value']) > 0)

    # Remove the Installation
    result = self.client.removeInstallations({'Instance': 'TestInstallA222'},
                                             {'System': 'UnexistentSystem',
                                              'Module': 'UnexistentModule',
                                              'Type': 'UnexistentType'},
                                             {'HostName': 'fictional',
                                              'CPU': 'TestCPU'})

    self.assertTrue(result['OK'])

    # Check if the installation was actually removed
    result = self.client.getInstallations({'Instance': 'TestInstallA222'},
                                          {'System': 'UnexistentSystem',
                                           'Module': 'UnexistentModule',
                                              'Type': 'UnexistentType'},
                                          {'HostName': 'fictional',
                                           'CPU': 'TestCPU'},
                                          False)

    self.assertTrue(result['OK'] and len(result['Value']) <= 0)

    # Create an installation associated with nonexistent Component
    result = self.client.addInstallation(
        {'InstallationTime': datetime.datetime.now(),
         'UnInstallationTime': datetime.datetime.now(),
         'Instance': 'TestInstallA333'},
        {'System': 'UnexistentSystem',
         'Module': 'UnexistentModule22A',
         'Type': 'UnexistentType'},
        {'HostName': 'fictional',
         'CPU': 'TestCPU'},
        False)

    self.assertFalse(result['OK'])

    # Multiple removal
    self.client.addInstallation(
        {'InstallationTime': datetime.datetime.now(),
         'UnInstallationTime': datetime.datetime.now(),
         'Instance': 'MultipleRemovalInstall1'},
        {'System': 'UnexistentSystem',
         'Module': 'UnexistentModule',
         'Type': 'UnexistentType'},
        {'HostName': 'fictional',
         'CPU': 'TestCPU'},
        False)
    self.client.addInstallation(
        {'InstallationTime': datetime.datetime.now(),
         'UnInstallationTime': datetime.datetime.now(),
         'Instance': 'MultipleRemovalInstall2'},
        {'System': 'UnexistentSystem',
         'Module': 'UnexistentModule',
         'Type': 'UnexistentType'},
        {'HostName': 'fictional',
         'CPU': 'TestCPU'},
        False)
    self.client.addInstallation(
        {'InstallationTime': datetime.datetime.now(),
         'UnInstallationTime': datetime.datetime.now(),
         'Instance': 'MultipleRemovalInstall3'},
        {'System': 'UnexistentSystem',
         'Module': 'UnexistentModule2',
         'Type': 'UnexistentType'},
        {'HostName': 'fictional',
         'CPU': 'TestCPU'},
        True)

    result = self.client.getInstallations(
        {'Instance':
         ['MultipleRemovalInstall1', 'MultipleRemovalInstall3']},
        {},
        {},
        False)

    self.assertTrue(result['OK'] and len(result['Value']) == 2)

    self.client.removeInstallations({},
                                    {'Module': 'UnexistentModule'},
                                    {})

    result = self.client.getInstallations({},
                                          {'Module': 'UnexistentModule2'},
                                          {}, False)

    self.assertTrue(result['OK'] and len(result['Value']) >= 1)

    result = self.client.getInstallations({},
                                          {'Module': 'UnexistentModule'},
                                          {},
                                          False)

    self.assertTrue(result['OK'] and len(result['Value']) <= 0)

    self.client.removeInstallations({},
                                    {'Module': 'UnexistentModule2'},
                                    {})

    self.assertTrue(result['OK'])

    # Clean up what we created
    self.client.removeHosts({'HostName': 'fictional', 'CPU': 'TestCPU'})
    self.client.removeComponents({'System': 'UnexistentSystem',
                                  'Module': 'UnexistentModule',
                                  'Type': 'UnexistentType'})
    self.client.removeComponents({'System': 'UnexistentSystem',
                                  'Module': 'UnexistentModule2',
                                  'Type': 'UnexistentType'})

  def testHostLogging(self):
    """
    Tests the HostLogging database operations
    """

    # Create a sample log
    result = self.client.updateLog('TestHost', {'DIRACVersion': 'v6r15'})

    self.assertTrue(result['OK'])

    # Check that the log exists
    result = self.client.getLog('TestHost')

    self.assertTrue(result['OK'] and result['Value'][0]['DIRACVersion'] == 'v6r15')

    # Update the fields of the created log
    result = self.client.updateLog('TestHost', {'hostName': 'StillATestHost'})

    self.assertTrue(result['OK'])

    # Check if the log with the modified fields exists
    result = self.client.getLog('StillATestHost')

    self.assertTrue(result['OK'] and result['Value'][0]['DIRACVersion'] == 'v6r15')

    # Remove the log
    result = self.client.removeLogs({'hostName': 'StillATestHost'})

    self.assertTrue(result['OK'])

    # Check that the log was actually removed
    result = self.client.getLog('StillATestHost')

    self.assertFalse(result['OK'])

    # Multiple removal
    self.client.updateLog('TestHostA', {'DIRACVersion': 'v7r0'})
    self.client.updateLog('TestHostB', {'DIRACVersion': 'v7r0'})
    self.client.updateLog('TestHostC', {'DIRACVersion': 'v7r1'})

    self.client.removeLogs({'DIRACVersion': 'v7r0'})

    result = self.client.getLog('TestHostC')

    self.assertTrue(result['OK'] and len(result['Value']) >= 1)

    result = self.client.getLog('TestHostB')

    self.assertFalse(result['OK'])

    result = self.client.removeLogs({'DIRACVersion': 'v7r1'})

    self.assertTrue(result['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientComponentMonitoring)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase
                (ComponentMonitoringClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
