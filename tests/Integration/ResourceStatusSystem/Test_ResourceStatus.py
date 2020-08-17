""" This is a test of the chain
    ResourceStatus -> ResourceStatusHandler -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running
"""

# pylint: disable=invalid-name,wrong-import-position

from __future__ import print_function
import sys
import time
import datetime
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

gLogger.setLevel('DEBUG')

rssClient = ResourceStatusClient()
Datetime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)


class TestClientResourceStatusTestCase(unittest.TestCase):

  def setUp(self):
    self.rsClient = ResourceStatusClient()

  def tearDown(self):
    pass


class ResourceStatusClientChain(TestClientResourceStatusTestCase):

  def test_addAndRemove(self):

    # clean up
    rssClient.deleteStatusElement('Site', 'Status', 'TestSite1234')
    rssClient.deleteStatusElement('Site', 'History', 'TestSite1234')
    rssClient.deleteStatusElement('Site', 'Log', 'TestSite1234')
    rssClient.deleteStatusElement('Resource', 'Status', 'TestName1234')
    rssClient.deleteStatusElement('Resource', 'History', 'TestName1234')
    rssClient.deleteStatusElement('Resource', 'Log', 'TestName1234')
    rssClient.deleteStatusElement('Resource', 'Status', 'TestName123456789')
    rssClient.deleteStatusElement('Resource', 'History', 'TestName123456789')
    rssClient.deleteStatusElement('Resource', 'Log', 'TestName123456789')

    # TEST insertStatusElement
    # ...............................................................................

    # add an element
    res = rssClient.insertStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                        'Active', 'elementType', 'reason', Datetime,
                                        Datetime, 'tokenOwner', Datetime)
    # check if the insert query was executed properly
    self.assertTrue(res['OK'])

    # select the previously entered element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'TestName1234')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][0][2], 'all')
    self.assertEqual(res['Value'][0][3], 'Active')

    # try to select the previously entered element from the Log table (it should NOT be there)
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])

    # try to select the previously entered element from the Log table,
    # with a reduced list of columns
    # (it should NOT be there)
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234', meta={'columns': ['name']})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])

    # TEST insertStatusElement (now site)
    # ...............................................................................

    print("add an element (status: Active)")
    res = rssClient.insertStatusElement('Site', 'Status', 'TestSite1234', 'statusType',
                                        'Active', 'elementType', 'reason', Datetime,
                                        Datetime, 'tokenOwner', Datetime)
    # check if the insert query was executed properly
    self.assertTrue(res['OK'])

    # select the previously entered element
    res = rssClient.selectStatusElement('Site', 'Status', 'TestSite1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'TestSite1234')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][0][3], 'Active')
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res['Value'][0][7], res['Value'][0][4]))

    # try to select the previously entered element from the Log table (it should NOT be there)
    res = rssClient.selectStatusElement('Site', 'Log', 'TestSite1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])

    # try to select the previously entered element from the Log table,
    # with a reduced list of columns
    # (it should NOT be there)
    res = rssClient.selectStatusElement('Site', 'Log', 'TestName1234',
                                        meta={'columns': ['name']})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])

    # TEST addOrModifyStatusElement (this time for modifying)
    # ...............................................................................

    print("modify the previously entered element (making it Banned)")
    res = rssClient.addOrModifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                             'Banned', 'elementType', 'reason')
    # check if the addOrModify query was executed properly
    self.assertTrue(res['OK'])

    # select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'TestName1234')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][0][3], 'Banned')
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res['Value'][0][7], res['Value'][0][4]))

    # try to select the previously entered element from the Log table (now it should be there)
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][1], 'TestName1234')
    self.assertEqual(res['Value'][0][2], 'statusType')
    self.assertEqual(res['Value'][0][4], 'Banned')

    # try to select the previously entered element from the Log table
    # with a reduced list of columns
    # (now it should be there)
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234',
                                        meta={'columns': ['name']})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'TestName1234')

    # try to select the previously entered element from the Log table
    # with a reduced list of columns
    # (now it should be there)
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234',
                                        meta={'columns': ['statustype', 'status']})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'statusType')
    self.assertEqual(res['Value'][0][1], 'Banned')

    # TEST modifyStatusElement
    # ...............................................................................

    print("modify again the previously entered element, putting it back to active")
    res = rssClient.modifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                        'Active', 'elementType', 'reason')
    # check if the modify query was executed properly
    self.assertTrue(res['OK'])

    # select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'TestName1234')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][0][3], 'Active')
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res['Value'][0][7], res['Value'][0][4]))

    # try to select the previously entered element from the Log table (now it should be there)
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][1], 'TestName1234')
    self.assertEqual(res['Value'][0][2], 'statusType')
    self.assertEqual(res['Value'][0][4], 'Banned')
    self.assertEqual(res['Value'][1][4], 'Active')  # this is the last one

    print("modifing once more the previously entered element")
    res = rssClient.modifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                        'Probing', 'elementType', 'reason')
    # check if the modify query was executed properly
    self.assertTrue(res['OK'])

    # select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'TestName1234')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][0][3], 'Probing')
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res['Value'][0][7], res['Value'][0][4]))

    # try to select the previously entered element from the Log table (now it should be there)
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][1], 'TestName1234')
    self.assertEqual(res['Value'][0][2], 'statusType')
    self.assertEqual(res['Value'][0][4], 'Banned')
    self.assertEqual(res['Value'][1][4], 'Active')
    self.assertEqual(res['Value'][2][4], 'Probing')  # this is the last one

    # try to select the previously entered element from the Log table (now it should be there)
    # with a reduced list of columns
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234',
                                        meta={'columns': ['status', 'StatusType']})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'Banned')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][1][0], 'Active')
    self.assertEqual(res['Value'][2][0], 'Probing')  # this is the last one

    time.sleep(3)  # just for seeing a difference between lastCheckTime and DateEffective
    print("modifing once more the previously entered element, but this time we only modify the reason")
    res = rssClient.modifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                        'Probing', 'elementType', 'a new reason')
    # check if the modify query was executed properly
    self.assertTrue(res['OK'])

    # select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'TestName1234')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][0][3], 'Probing')
    self.assertEqual(res['Value'][0][4], 'a new reason')
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res['Value'][0][8], res['Value'][0][5]))
    self.assertNotEqual(res['Value'][0][8], res['Value'][0][5])

    # try to select the previously entered element from the Log table (now it should be there)
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][1], 'TestName1234')
    self.assertEqual(res['Value'][0][2], 'statusType')
    self.assertEqual(res['Value'][0][4], 'Banned')
    self.assertEqual(res['Value'][1][4], 'Active')
    self.assertEqual(res['Value'][2][4], 'Probing')
    self.assertEqual(res['Value'][3][4], 'Probing')  # this is the last one

    # try to select the previously entered element from the Log table (now it should be there)
    # Using also Meta
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234',
                                        meta={'columns': ['Status', 'StatusType'],
                                              'newer': ['DateEffective', Datetime]})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'Banned')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][1][0], 'Active')
    self.assertEqual(res['Value'][1][1], 'statusType')
    self.assertEqual(res['Value'][2][0], 'Probing')
    self.assertEqual(res['Value'][2][1], 'statusType')
    self.assertEqual(res['Value'][3][0], 'Probing')  # this is the last one
    self.assertEqual(res['Value'][3][1], 'statusType')

    # try to select the previously entered element from the Log table (now it should be there)
    # Using also Meta
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234',
                                        meta={'columns': ['Status', 'StatusType'],
                                              'older': ['DateEffective', Datetime]})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])

    # try to select the previously entered element from the Log table (now it should be there)
    # Using Meta with order
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234',
                                        meta={'columns': ['Status', 'StatusType'],
                                              'newer': ['DateEffective', Datetime],
                                              'order': ['status', 'DESC']})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'Probing')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][1][0], 'Probing')
    self.assertEqual(res['Value'][1][1], 'statusType')
    self.assertEqual(res['Value'][2][0], 'Banned')
    self.assertEqual(res['Value'][2][1], 'statusType')
    self.assertEqual(res['Value'][3][0], 'Active')  # this is the last one (in this order)
    self.assertEqual(res['Value'][3][1], 'statusType')

    # try to select the previously entered element from the Log table (now it should be there)
    # Using Meta with limit
    res = rssClient.selectStatusElement('Resource', 'Log', 'TestName1234',
                                        meta={'columns': ['Status', 'StatusType'],
                                              'newer': ['DateEffective', Datetime],
                                              'order': 'status',
                                              'limit': 1})
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0][0], 'Active')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(len(res['Value']), 1)

    # TEST deleteStatusElement
    # ...............................................................................

    # delete the element
    res = rssClient.deleteStatusElement('Resource', 'Status', 'TestName1234')
    # check if the delete query was executed properly
    self.assertTrue(res['OK'])

    res = rssClient.deleteStatusElement('Site', 'Status', 'TestSite1234')
    # check if the delete query was executed properly
    self.assertTrue(res['OK'])

    res = rssClient.deleteStatusElement('Site', 'Log', 'TestSite1234')
    # check if the delete query was executed properly
    self.assertTrue(res['OK'])

    # try to select the previously deleted element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    # check if the returned value is empty
    self.assertFalse(res['Value'])

    # TEST addIfNotThereStatusElement
    # ...............................................................................

    # add the element
    res = rssClient.addIfNotThereStatusElement('Resource', 'Status', 'TestName123456789', 'statusType',
                                               'Active', 'elementType', 'reason', Datetime,
                                               Datetime, 'tokenOwner', Datetime)
    # check if the addIfNotThereStatus query was executed properly
    self.assertTrue(res['OK'])

    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    # check if the name that we got is equal to the previously added 'TestName123456789'
    self.assertEqual(res['Value'][0][0], 'TestName123456789')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][0][3], 'Active')

    # try to re-add the same element but with different value
    res = rssClient.addIfNotThereStatusElement('Resource', 'Status', 'TestName123456789', 'statusType',
                                               'Banned', 'elementType', 'another reason', Datetime,
                                               Datetime, 'tokenOwner', Datetime)
    # check if the addIfNotThereStatus query was executed properly
    self.assertTrue(res['OK'])
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    # check if the name that we got is equal to the previously added 'TestName123456789'
    self.assertEqual(res['Value'][0][0], 'TestName123456789')
    self.assertEqual(res['Value'][0][1], 'statusType')
    self.assertEqual(res['Value'][0][3], 'Active')  # NOT Banned

    # delete it
    res = rssClient.deleteStatusElement('Resource', 'Status', 'TestName123456789')
    # check if the delete query was executed properly
    self.assertTrue(res['OK'])

    # ...............................................................................
    # The below values should be empty since they were modified

    # try to select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    # check if the returned value is empty
    self.assertFalse(res['Value'])

    # try to select the previously modified element
    res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
    # check if the select query was executed properly
    self.assertTrue(res['OK'])
    # check if the returned value is empty
    self.assertFalse(res['Value'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientResourceStatusTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
