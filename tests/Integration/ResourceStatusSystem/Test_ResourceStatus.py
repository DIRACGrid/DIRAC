""" This is a test of the chain
    ResourceStatus -> ResourceStatusHandler -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running

    this is pytest!
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

gLogger.setLevel('DEBUG')

rssClient = ResourceStatus()

def test_addAndRemove():

  rsClient.deleteStatusElement('Resource', 'Status', 'TestName1234')
  rsClient.deleteStatusElement('Resource', 'Status', 'TestName123456789')

  # TEST insertStatusElement
  # ...............................................................................

  #add an element
  res = rsClient.insertStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                   'Active', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the insert query was executed properly
  assert res['OK'] == True


  #select the previously entered element
  res = rsClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  assert res['Value'][0][0] == 'Active'


  # TEST addOrModifyStatusElement
  # ...............................................................................

  #modify the previously entered element
  res = rsClient.addOrModifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                   'Banned', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the addOrModify query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  assert res['Value'][0][0] == 'Banned'


  # TEST modifyStatusElement
  # ...............................................................................

  #modify the previously entered element
  res = rsClient.modifyStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                   'Active', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the modify query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  assert res['Value'][0][0] == 'Active'


  # TEST updateStatusElement
  # ...............................................................................

  #update the previously entered element
  res = rsClient.updateStatusElement('Resource', 'Status', 'TestName1234', 'statusType',
                                   'Banned', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the updateStatusElement query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  assert res['Value'][0][0] == 'Banned'


  # TEST deleteStatusElement
  # ...............................................................................

  #delete the element
  res = rsClient.deleteStatusElement('Resource', 'Status', 'TestName1234')
  #check if the delete query was executed properly
  assert res['OK'] == True


  #try to select the previously deleted element
  res = rsClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']


  # TEST addIfNotThereStatusElement
  # ...............................................................................

  #delete the element
  res = rsClient.addIfNotThereStatusElement('Resource', 'Status', 'TestName123456789', 'statusType',
                                   'Active', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the addIfNotThereStatus query was executed properly
  assert res['OK'] == True

  res = rsClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234_Test'
  assert res['Value'][0][0] == 'Active'

  #delete it
  res = rsClient.deleteStatusElement('Resource', 'Status', 'TestName123456789')
  #check if the delete query was executed properly
  assert res['OK'] == True


  # ...............................................................................
  # The below values should be empty since they were modified

  #try to select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Status', 'TestName123456789')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']

  #try to select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Status', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']

  rssClient.rssCache.refreshCache()
  result = rssClient.getElementStatus("test_element2", "ComputingElement")
  assert result['OK'] == True
  assert result['Value']['test_element2']['all'] == 'Banned'