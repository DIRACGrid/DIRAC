""" This is a test of the chain
    ReportsClient -> ReportsGeneratorHandler -> AccountingDB

    It supposes that the DB is present, and that the service is running.
    Also the service DataStore has to be up and running.

    this is pytest!
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

import datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger

from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation

from DIRAC.tests.Utilities.Accounting import createDataOperationAccountingRecord
from DIRAC.tests.Utilities.Accounting import createStorageOccupancyAccountingRecord

gLogger.setLevel('DEBUG')


def test_addAndRemoveDataOperation():

  # just inserting one record
  record = createDataOperationAccountingRecord()
  record.setStartTime()
  record.setEndTime()
  res = gDataStoreClient.addRegister(record)
  assert res['OK']
  res = gDataStoreClient.commit()
  assert res['OK']

  rc = ReportsClient()

  res = rc.listReports('DataOperation')
  assert res['OK']

  res = rc.listUniqueKeyValues('DataOperation')
  assert res['OK']

  res = rc.getReport('DataOperation', 'Successful transfers',
                     datetime.datetime.utcnow(), datetime.datetime.utcnow(),
                     {}, 'Destination')
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

  rc = ReportsClient()

  res = rc.listReports('StorageOccupancy')
  assert res['OK']

  res = rc.listUniqueKeyValues('StorageOccupancy')
  assert res['OK']

  res = rc.getReport('StorageOccupancy', 'Free and Used Space',
                     datetime.datetime.utcnow(), datetime.datetime.utcnow(),
                     {}, 'StorageElement')
  assert res['OK']

  # now removing that record
  res = gDataStoreClient.remove(record)
  assert res['OK']
