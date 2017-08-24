''' This is a test of the chain
    ResourceManagementClient -> ResourceManagementHandler -> ResourceManagementDB
    It supposes that the DB is present, and that the service is running

    The DB is supposed to be empty when the test starts
'''

#pylint: disable=invalid-name,wrong-import-position,missing-docstring

import datetime
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

gLogger.setLevel('DEBUG')

dateEffective = datetime.datetime.now()
lastCheckTime = datetime.datetime.now()


class TestClientResourceManagementTestCase( unittest.TestCase ):

  def setUp( self ):
    self.rmClient = ResourceManagementClient()

  def tearDown( self ):
    pass


class ResourceManagementClientChain( TestClientResourceManagementTestCase ):

  def test_AccountingCache(self):
    """
    DowntimeCache table
    """

    res = self.rmClient.deleteAccountingCache('TestName12345')  # just making sure it's not there (yet)
    self.assertTrue(res['OK'])


    # TEST addOrModifyAccountingCache
    res = self.rmClient.addOrModifyAccountingCache('TestName12345', 'plotType', 'plotName', 'result',
                                                   datetime.datetime.now(), datetime.datetime.now())
    self.assertTrue(res['OK'])

    res = self.rmClient.selectAccountingCache('TestName12345')
    self.assertTrue(res['OK'])
    #check if the name that we got is equal to the previously added 'TestName12345'
    self.assertEqual(res['Value'][0][0], 'TestName12345')

    res = self.rmClient.addOrModifyAccountingCache('TestName12345', 'plotType', 'plotName', 'changedresult',
                                                   dateEffective, lastCheckTime)
    self.assertTrue(res['OK'])

    res = self.rmClient.selectAccountingCache('TestName12345')
    #check if the result has changed
    self.assertEqual(res['Value'][0][4], 'changedresult')


    # TEST deleteAccountingCache
    # ...............................................................................
    res = self.rmClient.deleteAccountingCache('TestName12345')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectAccountingCache('TestName12345')
    self.assertTrue(res['OK'])
    self.assertFalse(res['Value'])


  def test_DowntimeCache(self):
    """
    DowntimeCache table
    """

    res = self.rmClient.deleteDowntimeCache('TestName12345')  # just making sure it's not there (yet)
    self.assertTrue(res['OK'])


    # TEST addOrModifyDowntimeCache
    res = self.rmClient.addOrModifyDowntimeCache( 'TestName12345', 'element', 'name',
                                                  datetime.datetime.now(), datetime.datetime.now(),
                                                  'severity', 'description', 'link',
                                                  datetime.datetime.now(), datetime.datetime.now(),
                                                  'gOCDBServiceType')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectDowntimeCache('TestName12345')
    self.assertTrue(res['OK'])
    #check if the name that we got is equal to the previously added 'TestName12345'
    self.assertEqual(res['Value'][0][0], 'TestName12345')

    res = self.rmClient.addOrModifyDowntimeCache('TestName12345', 'element', 'name', severity = 'changedSeverity')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectDowntimeCache('TestName12345')
    #check if the result has changed
    self.assertEqual(res['Value'][0][4], 'changedSeverity')


    # TEST deleteDowntimeCache
    # ...............................................................................
    res = self.rmClient.deleteDowntimeCache('TestName12345')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectDowntimeCache('TestName12345')
    self.assertTrue(res['OK'])
    self.assertFalse(res['Value'])


  def test_GGUSTicketsCache(self):
    """
    GGUSTicketsCache table
    """

    res = self.rmClient.deleteGGUSTicketsCache('TestName12345')  # just making sure it's not there (yet)
    self.assertTrue(res['OK'])


    # TEST addOrModifyGGUSTicketsCache
    res = self.rmClient.addOrModifyGGUSTicketsCache( 'TestName12345', 'link', 0, 'tickets', datetime.datetime.now() )
    self.assertTrue(res['OK'])

    res = self.rmClient.selectGGUSTicketsCache('TestName12345')
    self.assertTrue(res['OK'])
    #check if the name that we got is equal to the previously added 'TestName12345'
    self.assertEqual(res['Value'][0][0], 'TestName12345')

    res = self.rmClient.addOrModifyGGUSTicketsCache('TestName12345', 'newLink')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectGGUSTicketsCache('TestName12345')
    #check if the result has changed
    self.assertEqual(res['Value'][0][3], 'newLink')


    # TEST deleteGGUSTicketsCache
    # ...............................................................................
    res = self.rmClient.deleteGGUSTicketsCache('TestName12345')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectGGUSTicketsCache('TestName12345')
    self.assertTrue(res['OK'])
    self.assertFalse(res['Value'])


  def test_JobCache(self):
    """
    JobCache table
    """

    res = self.rmClient.deleteJobCache('TestName12345')  # just making sure it's not there (yet)
    self.assertTrue(res['OK'])


    # TEST addOrModifyJobCache
    res = self.rmClient.addOrModifyJobCache( 'TestName12345', 'maskstatus', 50.89, 'status', datetime.datetime.now() )
    self.assertTrue(res['OK'])

    res = self.rmClient.selectJobCache('TestName12345')
    self.assertTrue(res['OK'])
    #check if the name that we got is equal to the previously added 'TestName12345'
    self.assertEqual(res['Value'][0][0], 'TestName12345')

    res = self.rmClient.addOrModifyJobCache('TestName12345', status = 'newStatus')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectJobCache('TestName12345')
    #check if the result has changed
    self.assertEqual(res['Value'][0][1], 'newStatus')


    # TEST deleteJobCache
    # ...............................................................................
    res = self.rmClient.deleteJobCache('TestName12345')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectJobCache('TestName12345')
    self.assertTrue(res['OK'])
    self.assertFalse(res['Value'])

  def test_PilotCache(self):
    """
    PilotCache table
    """

    res = self.rmClient.deletePilotCache('TestName12345')  # just making sure it's not there (yet)
    self.assertTrue(res['OK'])


    # TEST addOrModifyPilotCache
    res = self.rmClient.addOrModifyPilotCache( 'TestName12345', 'CE', 0.0, 25.5, 'status', datetime.datetime.now() )
    self.assertTrue(res['OK'])

    res = self.rmClient.selectPilotCache('TestName12345')
    self.assertTrue(res['OK'])
    #check if the name that we got is equal to the previously added 'TestName12345'
    self.assertEqual(res['Value'][0][0], 'TestName12345')

    res = self.rmClient.addOrModifyPilotCache('TestName12345', status = 'newStatus')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectPilotCache('TestName12345')
    #check if the result has changed
    self.assertEqual(res['Value'][0][2], 'newStatus')


    # TEST deletePilotCache
    # ...............................................................................
    res = self.rmClient.deletePilotCache('TestName12345')
    self.assertTrue(res['OK'])

    res = self.rmClient.selectPilotCache('TestName12345')
    self.assertTrue(res['OK'])
    self.assertFalse(res['Value'])

# FIXME: add other tables


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientResourceManagementTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ResourceManagementClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
