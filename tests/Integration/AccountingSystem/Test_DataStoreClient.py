""" This is a test of the chain
    DataStoreClient -> DataStoreHandler -> AccountingDB

    It supposes that the DB is present, and that the service is running

    this is pytest!
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger

from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation

gLogger.setLevel('DEBUG')

dsc = DataStoreClient()

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
  oDataOperation.setValuesFromDict( accountingDict )
  return oDataOperation

def test_addAndRemove():

  #just inserting one record
  record = createAccountingRecord()
  record.setStartTime()
  record.setEndTime()
  res = dsc.addRegister( record )
  assert res['OK'] == True
  res = dsc.commit()
  assert res['OK'] == True

  #now removing that record
  res = dsc.remove(record)
  assert res['OK'] == True
