""" This is a test of the chain
    DataStoreClient -> DataStoreHandler -> AccountingDB

    It supposes that the DB is present, and that the service is running

    this is pytest!
"""

# pylint: disable=invalid-name,wrong-import-position

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger

from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation

gLogger.setLevel('DEBUG')


def createAccountingRecord():
  accountingDict = {}
  accountingDict['OperationType'] = 'putAndRegister'
  accountingDict['User'] = 'system'
  accountingDict['Protocol'] = 'DataManager'
  accountingDict['RegistrationTime'] = 0.0
  accountingDict['RegistrationOK'] = 0
  accountingDict['RegistrationTotal'] = 0
  accountingDict['Destination'] = 'se'
  accountingDict['TransferTotal'] = 1
  accountingDict['TransferOK'] = 1
  accountingDict['TransferSize'] = 1
  accountingDict['TransferTime'] = 0.0
  accountingDict['FinalStatus'] = 'Successful'
  accountingDict['Source'] = 'testSite'
  oDataOperation = DataOperation()
  oDataOperation.setValuesFromDict(accountingDict)
  return oDataOperation


def test_addAndRemove():

  # just inserting one record
  record = createAccountingRecord()
  record.setStartTime()
  record.setEndTime()
  res = gDataStoreClient.addRegister(record)
  assert res['OK']
  res = gDataStoreClient.commit()
  assert res['OK']

  # now removing that record
  res = gDataStoreClient.remove(record)
  assert res['OK']
