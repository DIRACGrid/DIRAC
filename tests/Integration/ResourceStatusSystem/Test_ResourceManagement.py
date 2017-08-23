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
    AccountingCache table
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
    print res
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

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientResourceManagementTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ResourceManagementClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
