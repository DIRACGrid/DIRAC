""" This is a test of the chain
    DataStoreClient -> DataStoreHandler -> AccountingDB

    It supposes that the DB is present, and that the service is running

    this is pytest!
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger

from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.Types.StorageOccupancy import StorageOccupancy
from DIRAC.tests.Utilities.Accounting import createDataOperationAccountingRecord
from DIRAC.tests.Utilities.Accounting import createStorageOccupancyAccountingRecord

gLogger.setLevel('DEBUG')


def test_addAndRemoveDataperation():

  # just inserting one record
  record = createDataOperationAccountingRecord()
  record.setStartTime()
  record.setEndTime()
  res = gDataStoreClient.addRegister(record)
  assert res['OK']
  res = gDataStoreClient.commit()
  assert res['OK']
  # now removing that record
  res = gDataStoreClient.remove(record)
  assert res['OK']


def test_addAndRemoveStorageOccupancy():

  # just inserting one record
  record = createStorageOccupancyAccountingRecord()
  record.setStartTime()
  record.setEndTime()
  res = gDataStoreClient.addRegister(record)
  assert res['OK']
  res = gDataStoreClient.commit()
  assert res['OK']
  # now removing that record
  res = gDataStoreClient.remove(record)
  assert res['OK']
