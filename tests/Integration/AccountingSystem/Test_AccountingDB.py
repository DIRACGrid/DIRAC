""" Test for AccountingDB
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import pytest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB


gLogger.setLevel('DEBUG')

acDB = AccountingDB()

startTime = 1262200000
endTime = 1262300400
keyValues = [
    "User_1",
    "UserGroup_1",
    "Site_1",
    "GridCE_1",
    "GridMiddleware_1",
    "GridResourceBroker_1",
    "GridStatus_1",
]
nonKeyValue = [123]


@pytest.fixture
def inout():
  yield inout

  res = acDB.deleteRecord(
      'dirac-JenkinsSetup_Pilot',
      startTime,
      endTime,
      keyValues + nonKeyValue,
  )
  assert res['OK'], res['Message']


def test_mix():
  res = acDB.getRegisteredTypes()
  assert res['OK'], res['Message']


def test_insert(inout):
  res = acDB.insertRecordDirectly(
      "dirac-JenkinsSetup_Pilot",
      startTime,
      endTime,
      keyValues + nonKeyValue
  )
  assert res['OK'], res['Message']
  # empty
  res = acDB.retrieveRawRecords(
      'dirac-JenkinsSetup_Pilot', startTime, endTime, {}, ''
  )
  assert res['OK'], res['Message']
  assert len(res['Value']) == 1
  assert res['Value'] == (
      tuple([startTime, endTime] + keyValues + nonKeyValue),
  )
