# pylint: disable=protected-access
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.Types.StorageOccupancy import StorageOccupancy


def createDataOperationAccountingRecord():
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


def createStorageOccupancyAccountingRecord():
  accountingDict = {}
  accountingDict['Site'] = 'LCG.PIPPO.it'
  accountingDict['Endpoint'] = 'somewhere.in.topolinea.it'
  accountingDict['StorageElement'] = 'PIPPO-SE'
  accountingDict['SpaceType'] = 'Total'
  accountingDict['Space'] = 123456
  oSO = StorageOccupancy()
  oSO.setValuesFromDict(accountingDict)
  return oSO
