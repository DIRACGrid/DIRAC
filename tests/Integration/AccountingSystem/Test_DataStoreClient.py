""" This is a test of the chain
    DataStoreClient -> DataStoreHandler -> AccountingDB

    It supposes that the DB is present, and that the service is running

    this is pytest!
"""
# pylint: disable=wrong-import-position, missing-docstring

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger

from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.tests.Utilities.Accounting import createStorageOccupancyAccountingRecord

gLogger.setLevel("DEBUG")


def test_addAndRemoveStorageOccupancy():

    # just inserting one record
    record = createStorageOccupancyAccountingRecord()
    record.setStartTime()
    record.setEndTime()
    res = gDataStoreClient.addRegister(record)
    assert res["OK"]
    res = gDataStoreClient.commit()
    assert res["OK"]
    # now removing that record
    res = gDataStoreClient.remove(record)
    assert res["OK"]
