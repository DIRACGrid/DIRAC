""" This is a test of the chain
    ResourceStatus -> ResourceStatusHandler -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running
"""

#pylint: disable=invalid-name,wrong-import-position,missing-docstring

import datetime
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

gLogger.setLevel('DEBUG')

rssClient = ResourceStatusClient()
Datetime = datetime.datetime.now()

class TestClientResourceStatusTestCase( unittest.TestCase ):

  def setUp( self ):
    self.rsClient = ResourceStatusClient()

  def tearDown( self ):
    pass


class ResourceStatusClientChain( TestClientResourceStatusTestCase ):


  def test_addAndRemove(self):

    rssClient.deleteStatusElement('Resource', 'Status', 'TestName1234')
    exit(0)
    rssClient.deleteStatusElement('Resource', 'Status', 'TestName123456789')

    # TEST insertStatusElement
    # ...............................................................................

    #add an element
    res = rssClient.insertStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                        'Active', 'elementType', 'reason', Datetime,
                                        Datetime, 'tokenOwner', Datetime)
    #check if the insert query was executed properly
    self.assertTrue(res['OK'])


    #select the previously entered element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    #check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'Active')


    # TEST addOrModifyStatusElement
    # ...............................................................................

    #modify the previously entered element
    res = rssClient.addOrModifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                             'Banned', 'elementType', 'reason', Datetime,
                                             Datetime, 'tokenOwner', Datetime)
    #check if the addOrModify query was executed properly
    self.assertTrue(res['OK'])


    #select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    #check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'Banned')


    # TEST modifyStatusElement
    # ...............................................................................

    #modify the previously entered element
    res = rssClient.modifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                        'Active', 'elementType', 'reason', Datetime,
                                        Datetime, 'tokenOwner', Datetime)
    #check if the modify query was executed properly
    self.assertTrue(res['OK'])


    #select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    #check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'Active')

    # TEST deleteStatusElement
    # ...............................................................................

    #delete the element
    res = rssClient.deleteStatusElement('Resource', 'Status', 'TestName1234')
    #check if the delete query was executed properly
    self.assertTrue(res['OK'])


    #try to select the previously deleted element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    #check if the select query was executed properly
    self.assertTrue(res['OK'])
    #check if the returned value is empty
    self.assertFalse(res['Value'])


    # TEST addIfNotThereStatusElement
    # ...............................................................................

    #delete the element
    res = rssClient.addIfNotThereStatusElement( 'Resource', 'Status', 'TestName123456789', 'statusType',
                                                'Active', 'elementType', 'reason', Datetime,
                                                Datetime, 'tokenOwner', Datetime)
    #check if the addIfNotThereStatus query was executed properly
    self.assertTrue(res['OK'])

    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
    #check if the select query was executed properly
    self.assertTrue(res['OK'])
    #check if the name that we got is equal to the previously added 'TestName1234_Test'
    self.assertEqual(res['Value'][0][0], 'Active')

    #delete it
    res = rssClient.deleteStatusElement('Resource', 'Status', 'TestName123456789')
    #check if the delete query was executed properly
    self.assertTrue(res['OK'])


    # ...............................................................................
    # The below values should be empty since they were modified

    #try to select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
    #check if the select query was executed properly
    self.assertTrue(res['OK'])
    #check if the returned value is empty
    self.assertFalse(res['Value'])

    #try to select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    #check if the select query was executed properly
    self.assertTrue(res['OK'])
    #check if the returned value is empty
    self.assertFalse(res['Value'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientResourceStatusTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ResourceStatusClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
