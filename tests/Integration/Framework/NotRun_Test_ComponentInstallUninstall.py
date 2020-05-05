"""
This program tests the correct addition and removal of components to the InstalledComponentsDB,
as well as the components
CLI functions are used to ensure the test is as similar as possible to a real user-to-cli interaction
This test assumes that there is a DIRAC master server running on the local machine
This test assumes that the Notification service is not installed
This test assumes that the FTS3DB database is not installed and doesn't exist in MySQL
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

import sys
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
from DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI import SystemAdministratorClientCLI
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN


class TestComponentInstallation(unittest.TestCase):
  """
  Contains methods for testing of separate elements
  """

  def setUp(self):
    self.host = 'localhost'
    self.notificationPort = 9154
    self.rootPwd = ''
    self.csClient = CSAPI()
    self.monitoringClient = ComponentMonitoringClient()
    self.client = SystemAdministratorClientCLI(self.host)

    self.csClient.downloadCSData()
    result = self.csClient.getCurrentCFG()
    if not result['OK']:
      raise Exception(result['Message'])
    cfg = result['Value']

    setup = cfg.getOption('DIRAC/Setup', 'dirac-JenkinsSetup')

    self.frameworkSetup = cfg.getOption('DIRAC/Setups/' + setup + '/Framework')
    self.rootPwd = cfg.getOption('Systems/Databases/Password')
    self.diracPwd = self.rootPwd

    result = getProxyInfo()
    if not result['OK']:
      raise Exception(result['Message'])
    chain = result['Value']['chain']
    result = chain.getCertInChain(-1)
    if not result['OK']:
      raise Exception(result['Message'])
    result = result['Value'].getSubjectDN()
    if not result['OK']:
      raise Exception(result['Message'])
    userDN = result['Value']
    result = getUsernameForDN(userDN)
    if not result['OK']:
      raise Exception(result['Message'])
    self.user = result['Value']
    if not self.user:
      self.user = 'unknown'

  def tearDown(self):
    pass


class ComponentInstallationChain(TestComponentInstallation):

  def testComponent(self):

    service1Present = False
    service2Present = False

    # Check whether the service is already present or not
    cfg = self.csClient.getCurrentCFG()['Value']
    if cfg.isSection('Systems/Framework/' +
                     self.frameworkSetup +
                     '/Services/Notification/') and cfg.isOption('Systems/Framework/' +
                                                                 self.frameworkSetup +
                                                                 '/URLs/Notification'):
      service1Present = True

    if not service1Present:
      # Install component
      self.client.do_install('service Framework Notification')
      self.csClient.downloadCSData()

    # Check installation in CS
    cfg = self.csClient.getCurrentCFG()['Value']
    self.assertTrue(cfg.isSection('Systems/Framework/' + self.frameworkSetup + '/Services/Notification/') and
                    cfg.isOption('Systems/Framework/' + self.frameworkSetup + '/URLs/Notification'))

    self.assertTrue(cfg.getOption('Systems/Framework/' + self.frameworkSetup + '/URLs/Notification') ==
                    'dips://' + self.host + ':' + str(self.notificationPort) + '/Framework/Notification')

    # Check installation in database
    if not service1Present:
      result = self.monitoringClient.getInstallations({'Instance': 'Notification',
                                                       'UnInstallationTime': None, 'InstalledBy': self.user},
                                                      {'System': 'Framework', 'Type': 'service',
                                                       'Module': 'Notification'},
                                                      {}, False)
    else:
      # We dont know who made the previous installation
      result = self.monitoringClient.getInstallations({'Instance': 'Notification', 'UnInstallationTime': None},
                                                      {'System': 'Framework',
                                                       'Type': 'service', 'Module': 'Notification'},
                                                      {}, False)

    self.assertTrue(result['OK'] and len(result['Value']) == 1)

    # Check whether the second service is already present or not
    cfg = self.csClient.getCurrentCFG()['Value']
    if cfg.isSection('Systems/Framework/' +
                     self.frameworkSetup +
                     '/Services/Notification2/') and cfg.isOption('Systems/Framework/' +
                                                                  self.frameworkSetup +
                                                                  '/URLs/Notification2'):
      service2Present = True

    if not service2Present:
      # Install second component
      self.client.do_install('service Framework Notification2 -m Notification')

    # Check installation in CS
    self.csClient.downloadCSData()
    cfg = self.csClient.getCurrentCFG()['Value']
    self.assertTrue(cfg.isSection('Systems/Framework/' + self.frameworkSetup + '/Services/Notification2/') and
                    cfg.isOption('Systems/Framework/' + self.frameworkSetup + '/URLs/Notification2'))

    if not service1Present:
      # Uninstall component
      self.client.do_uninstall('-f Framework Notification')

    # Check CS is intact ( there should still be at least one instance of Notification )
    self.csClient.downloadCSData()
    cfg = self.csClient.getCurrentCFG()['Value']
    self.assertTrue(cfg.isSection('Systems/Framework/' +
                                  self.frameworkSetup +
                                  '/Services/Notification/') and cfg.isSection('Systems/Framework/' +
                                                                               self.frameworkSetup +
                                                                               '/Services/Notification/') and
                    cfg.isOption('Systems/Framework/' +
                                 self.frameworkSetup +
                                 '/URLs/Notification'))

    if not service2Present:
      # Uninstall second component
      self.client.do_uninstall('-f Framework Notification2')

    if not service1Present and not service2Present:
      # Check uninstallation in CS ( only if the services were not already present )
      self.csClient.downloadCSData()
      cfg = self.csClient.getCurrentCFG()['Value']
      self.assertTrue(not cfg.isSection('Systems/Framework/' +
                                        self.frameworkSetup +
                                        '/Services/Notification/') and not cfg.isSection(
                                            'Systems/Framework/' +
                                            self.frameworkSetup +
                                            '/Services/Notification2/') and not
                      cfg.isOption('Systems/Framework/' +
                                   self.frameworkSetup +
                                   '/URLs/Notification'))

  def testDatabase(self):

    gComponentInstaller.setMySQLPasswords(self.rootPwd, self.diracPwd)

    # Install database
    self.client.do_install('db FTS3DB')

    # Check installation in CS
    self.csClient.downloadCSData()
    cfg = self.csClient.getCurrentCFG()['Value']
    self.assertTrue(cfg.isSection('Systems/DataManagement/' + self.frameworkSetup + '/Databases/FTS3DB/'))

    # Check in database
    result = self.monitoringClient.getInstallations({'Instance': 'FTS3DB',
                                                     'UnInstallationTime': None, 'InstalledBy': self.user},
                                                    {'System': 'DataManagement', 'Type': 'DB', 'Module': 'FTS3DB'},
                                                    {}, False)
    self.assertTrue(result['OK'] and len(result['Value']) == 1)

    # Uninstall database
    self.client.do_uninstall('db FTS3DB')

    # Check uninstallation in CS
    self.csClient.downloadCSData()
    cfg = self.csClient.getCurrentCFG()['Value']
    self.assertTrue(not cfg.isSection('Systems/DataManagement/' + self.frameworkSetup + '/Databases/FTS3DB/'))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestComponentInstallation)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ComponentInstallationChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
