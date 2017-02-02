""" This is a test of the chain
    ResourceStatus -> ResourceStatusHandler -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running

    this is pytest!
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import datetime
from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

gLogger.setLevel('DEBUG')

rssClient = ResourceStatusClient()
Datetime = datetime.datetime.now()

def test_addAndRemove():

  rssClient.deleteStatusElement('Resource', 'Status', 'TestName1234')
  rssClient.deleteStatusElement('Resource', 'Status', 'TestName123456789')

  # TEST insertStatusElement
  # ...............................................................................

  #add an element
  res = rssClient.insertStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                   'Active', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the insert query was executed properly
  assert res['OK'] == True


  #select the previously entered element
  res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  assert res['Value'][0][0] == 'Active'


  # TEST addOrModifyStatusElement
  # ...............................................................................

  #modify the previously entered element
  res = rssClient.addOrModifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                   'Banned', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the addOrModify query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  assert res['Value'][0][0] == 'Banned'


  # TEST modifyStatusElement
  # ...............................................................................

  #modify the previously entered element
  res = rssClient.modifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                   'Active', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the modify query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  assert res['Value'][0][0] == 'Active'


  # TEST updateStatusElement
  # ...............................................................................

  #update the previously entered element
  res = rssClient.updateStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                   'Banned', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the updateStatusElement query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  assert res['Value'][0][0] == 'Banned'


  # TEST deleteStatusElement
  # ...............................................................................

  #delete the element
  res = rssClient.deleteStatusElement('Resource', 'Status', 'TestName1234')
  #check if the delete query was executed properly
  assert res['OK'] == True


  #try to select the previously deleted element
  res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']


  # TEST addIfNotThereStatusElement
  # ...............................................................................

  #delete the element
  res = rssClient.addIfNotThereStatusElement('Resource', 'Status', 'TestName123456789', 'statusType',
                                   'Active', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the addIfNotThereStatus query was executed properly
  assert res['OK'] == True

  res = rssClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234_Test'
  assert res['Value'][0][0] == 'Active'

  #delete it
  res = rssClient.deleteStatusElement('Resource', 'Status', 'TestName123456789')
  #check if the delete query was executed properly
  assert res['OK'] == True


  # ...............................................................................
  # The below values should be empty since they were modified

  #try to select the previously modified element
  res = rssClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']

  #try to select the previously modified element
  res = rssClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']
