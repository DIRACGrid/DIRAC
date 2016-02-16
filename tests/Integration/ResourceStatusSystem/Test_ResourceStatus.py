""" This is a test of the chain
    ResourceStatusClient -> ResourceStatusHandler -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running

    this is pytest!
"""

import datetime
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

gLogger.setLevel('DEBUG')

rsClient = ResourceStatusClient()

Datetime = datetime.datetime.now()

def test_addAndRemove():

  # TEST insertStatusElement
  # ...............................................................................

  #add an element
  res = rsClient.insertStatusElement('Resource', 'Log', 'TestName1234', 'statusType',
                                   'Active', 'elementType', 'reason', Datetime,
                                   Datetime, 'tokenOwner', Datetime)
  #check if the insert query was executed properly
  assert res['OK'] == True


  #select the previously entered element
  res = rsClient.selectStatusElement('Resource', 'Log', 'TestName1234')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234'
  assert res['Value'][0][2] == 'TestName1234'


  # TEST addOrModifyStatusElement
  # ...............................................................................

  #modify the previously entered element
  res = rsClient.addOrModifyStatusElement('Resource', 'Log', 'TestName1234_Modified')
  #check if the addOrModify query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Log', 'TestName1234_Modified')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234_Modified'
  assert res['Value'][0][2] == 'TestName1234_Modified'


  # TEST modifyStatusElement
  # ...............................................................................

  #modify the previously entered element
  res = rsClient.modifyStatusElement('Resource', 'Log', 'TestName1234_Modified2')
  #check if the modify query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Log', 'TestName1234_Modified2')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234_Modified2'
  assert res['Value'][0][2] == 'TestName1234_Modified2'


  # TEST updateStatusElement
  # ...............................................................................

  #update the previously entered element
  res = rsClient.updateStatusElement('Resource', 'Log', 'TestName1234_Modified3',
                                     'statusType', 'Active', 'elementType', 'reason',
                                     Datetime, Datetime, 'tokenOwner', Datetime)
  #check if the updateStatusElement query was executed properly
  assert res['OK'] == True


  #select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Log', 'TestName1234_Modified3')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234_Modified3'
  assert res['Value'][0][2] == 'TestName1234_Modified3'


  # TEST deleteStatusElement
  # ...............................................................................

  #delete the element
  res = rsClient.deleteStatusElement('Resource', 'Log', 'TestName1234_Modified3')
  #check if the delete query was executed properly
  assert res['OK'] == True


  #try to select the previously deleted element
  res = rsClient.selectStatusElement('Resource', 'Log', 'TestName1234_Modified3')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']


  # TEST addIfNotThereStatusElement
  # ...............................................................................

  #delete the element
  res = rsClient.addIfNotThereStatusElement('Resource', 'Log', 'TestName1234_Test')
  #check if the addIfNotThereStatus query was executed properly
  assert res['OK'] == True


  #try to select the previously deleted element
  res = rsClient.selectStatusElement('Resource', 'Log', 'TestName1234_Test')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234_Test'
  assert res['Value'][0][2] == 'TestName1234_Test'

  #delete it
  res = rsClient.deleteStatusElement('Resource', 'Log', 'TestName1234_Test')
  #check if the delete query was executed properly
  assert res['OK'] == True


  # ...............................................................................
  # The below values should be empty since they were modified

  #try to select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Log', 'TestName1234_Modified2')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']

  #try to select the previously modified element
  res = rsClient.selectStatusElement('Resource', 'Log', 'TestName1234_Modified')
  #check if the select query was executed properly
  assert res['OK'] == True
  #check if the returned value is empty
  assert not res['Value']
