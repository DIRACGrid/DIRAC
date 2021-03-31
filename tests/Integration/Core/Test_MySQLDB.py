"""
This is used to test the MySQLDB module.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import time
import pytest

import DIRAC
from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.MySQL import MySQL


# Useful methods

def setupDB():
  """ Get configuration from a cfg file and instantiate a DB
  """
  gLogger.setLevel('DEBUG')

  result = gConfig.getOption('/Systems/Databases/Host')
  if not result['OK']:
    result['Value'] = 'mysql'
  host = result['Value']

  result = gConfig.getOption('/Systems/Databases/User')
  if not result['OK']:
    result['Value'] = 'Dirac'
  user = result['Value']

  result = gConfig.getOption('/Systems/Databases/Password')
  if not result['OK']:
    result['Value'] = 'Dirac'
  password = result['Value']

  result = gConfig.getOption('/Systems/Databases/Port')
  if not result['OK']:
    result['Value'] = 3306
  port = int(result['Value'])

  return getDB(host, user, password, 'AccountingDB', port)


def getDB(host, user, password, dbName, port):
  """ Return a MySQL object
  """
  return MySQL(host, user, password, dbName, port)


def setupDBCreateTableInsertFields(table, requiredFields, values):
  """ Setup a DB, create a given table and insert values within it
  """
  mysqlDB = setupDB()

  result = mysqlDB._createTables(table, force=True)
  assert result['OK']

  for j in range(len(values)):
    value = values[j]
    result = mysqlDB.insertFields(name, requiredFields, value)
    assert result['OK']
    assert result['Value'] == 1
    assert result['lastRowId'] == j + 1

  return mysqlDB

# Pytest values


name = 'TestTable'

fields = ['Name', 'Surname']
reqFields = ['Name', 'Surname', 'Count']
allFields = ['Name', 'Surname', 'Count', 'Time']

cond0 = {}
cond10 = {'Count': list(range(10))}

table = {'TestTable': {'Fields': {'ID': "INTEGER UNIQUE NOT NULL AUTO_INCREMENT",
                                  'Name': "VARCHAR(255) NOT NULL DEFAULT 'Yo'",
                                  'Surname': "VARCHAR(255) NOT NULL DEFAULT 'Tu'",
                                  'Count': "INTEGER NOT NULL DEFAULT 0",
                                  'Time': "DATETIME"},
                       'PrimaryKey': 'ID'}}


def genVal1():
  values = []
  for i in range(100):
    values.append(['name1', 'Surn1', i])
  return values


def genVal2():
  values = []
  for i in range(2):
    values.append(['name1', 'Surn1', 1, 'UTC_TIMESTAMP()'])
  return values

# Tests


@pytest.mark.parametrize("host, user, password, dbName, port, expected", [
    ('mysql', 'Dirac', 'Dirac', 'AccountingDB', 3306, True),
    ('fake', 'fake', 'fake', 'FakeDB', 0000, False),
])
def test_connection(host, user, password, dbName, port, expected, monkeypatch):
  """ Try to connect to a DB
  """
  # Avoid having many retries which sleep for long durations
  monkeypatch.setattr(DIRAC.Core.Utilities.MySQL, "MAXCONNECTRETRY", 1)
  monkeypatch.setattr(DIRAC.Core.Utilities.MySQL, "RETRY_SLEEP_DURATION", 0.5)

  mysqlDB = getDB(host, user, password, dbName, port)
  result = mysqlDB._connect()
  assert result['OK'] is expected
  assert mysqlDB._connected is expected


@pytest.mark.parametrize("name, fields, table, cond, expected", [
    (name, fields, table, cond0, True),
])
def test_createEmptyTable(name, fields, table, cond, expected):
  """ Create a table and check that there is no element
  """
  mysqlDB = setupDB()

  result = mysqlDB._createTables(table, force=True)
  assert result['OK'] is expected

  result = mysqlDB.getCounters(name, fields, cond0)
  assert result['OK']
  assert result['Value'] == []

  result = mysqlDB.getDistinctAttributeValues(name, fields[0], cond0)
  assert result['OK']
  assert result['Value'] == []

  result = mysqlDB.getFields(name, fields)
  assert result['OK']
  assert result['Value'] == ()


@pytest.mark.parametrize("name, fields, values, table, expected", [
    (name, allFields, ['name1', 'Surn1', 1, 'UTC_TIMESTAMP()'], table, 1)
])
def test_insertElements(name, fields, values, table, expected):
  """ Create a table, insert an element
  """
  mysqlDB = setupDB()

  result = mysqlDB._createTables(table, force=True)
  assert result['OK']

  allDict = dict(zip(fields, values))

  result = mysqlDB.insertFields(name, inFields=fields, inValues=values)
  assert result['OK']
  assert result['Value'] == 1

  time.sleep(1)

  result = mysqlDB.insertFields(name, inDict=allDict)
  assert result['OK']
  assert result['Value'] == 1


@pytest.mark.parametrize("name, fields, requiredFields, values, table, cond, expected", [
    (name, fields, reqFields, genVal1(), table, cond0, [({'Surname': 'Surn1', 'Name': 'name1'}, 100)]),
    (name, fields, reqFields, genVal1(), table, cond10, [({'Surname': 'Surn1', 'Name': 'name1'}, 10)])
])
def test_getCounters(name, fields, requiredFields, values, table, cond, expected):
  """ Create a table, insert elements, test getCounters using various conditions
  """
  mysqlDB = setupDBCreateTableInsertFields(table, requiredFields, values)

  result = mysqlDB.getCounters(name, fields, cond)
  assert result['OK']
  assert result['Value'] == expected


@pytest.mark.parametrize("name, fields, requiredFields, values, table, cond, expected", [
    (name, fields, reqFields, genVal1(), table, cond0, ['name1'])
])
def test_getDistinctAttributeValues(name, fields, requiredFields, values, table, cond, expected):
  """ Create a table, insert elements, test getDistinctAttributeValues using various conditions
  """
  mysqlDB = setupDBCreateTableInsertFields(table, requiredFields, values)

  result = mysqlDB.getDistinctAttributeValues(name, fields[0], cond)
  assert result['OK']
  assert result['Value'] == expected


# use time.toString and call it inside the tests as to have the value of it
# when it is executed
@pytest.mark.parametrize("table, reqFields, values, name, args, expected, isExpectedCount", [
    (table, reqFields, genVal1(), name,
        {'outFields': fields}, 100, True),
    (table, reqFields, genVal1(), name,
        {'outFields': reqFields, 'condDict': cond10}, 10, True),
    (table, reqFields, genVal1(), name,
        {'limit': 1}, 1, True),
    (table, reqFields, genVal1(), name,
        {'outFields': ['Count'], 'orderAttribute': 'Count:DESC', 'limit': 1}, ((99,),), False),
    (table, reqFields, genVal1(), name,
        {'outFields': ['Count'], 'orderAttribute': 'Count:ASC', 'limit': 1}, ((0,),), False),
    (table, allFields, genVal2(), name,
        {'older': 'UTC_TIMESTAMP()', 'timeStamp': 'Time'}, 2, True),
    (table, allFields, genVal2(), name,
        {'newer': 'UTC_TIMESTAMP()', 'timeStamp': 'Time'}, 0, True),
    (table, allFields, genVal2(), name,
        {'older': Time.toString, 'timeStamp': 'Time'}, 2, True),
    (table, allFields, genVal2(), name,
        {'newer': Time.toString, 'timeStamp': 'Time'}, 0, True)
])
def test_getFields(table, reqFields, values, name, args, expected, isExpectedCount):
  """ Create a table, insert elements, test getFields using various conditions
  """
  mysqlDB = setupDBCreateTableInsertFields(table, reqFields, values)

  # Sleep one second to make sure that there is no race condition
  # when running with UTC_TIMESTAMP
  time.sleep(1)

  # If the newer or older parameter is a callable
  # (so typically Time.toString), evaluate it now
  for timeCmp in ('newer', 'older'):
    timeCmpArg = args.get(timeCmp)
    if callable(timeCmpArg):
      args[timeCmp] = timeCmpArg()

  result = mysqlDB.getFields(name, **args)
  assert result['OK']

  if isExpectedCount:
    assert len(result['Value']) == expected
  else:
    assert result['Value'] == expected


@pytest.mark.parametrize("name, fields, requiredFields, values, newValues, table, cond, expected1, expected2", [
    (name, fields, reqFields, genVal1(), ['name2', 'Surn2'], table, cond10, 10, 0)
])
def test_updateFields(name, fields, requiredFields, values, newValues, table, cond, expected1, expected2):
  """ Create a table, insert elements and update them using various conditions
  """
  mysqlDB = setupDBCreateTableInsertFields(table, requiredFields, values)

  result = mysqlDB.updateFields(name, fields, newValues, cond)
  assert result['OK']
  assert result['Value'] == expected1

  result = mysqlDB.updateFields(name, fields, newValues, cond)
  assert result['OK']
  assert result['Value'] == expected2


@pytest.mark.parametrize("name, fields, requiredFields, values, table, cond, expected1, expected2", [
    (name, fields, reqFields, genVal1(), table, cond10, 10, 90)
])
def test_deleteEntries(name, fields, requiredFields, values, table, cond, expected1, expected2):
  """ Create a table, insert elements and delete some of them
  """
  mysqlDB = setupDBCreateTableInsertFields(table, requiredFields, values)

  result = mysqlDB.deleteEntries(name, cond)
  assert result['OK']
  assert result['Value'] == expected1

  result = mysqlDB.deleteEntries(name)
  assert result['OK']
  assert result['Value'] == expected2

  result = mysqlDB.getCounters(name, fields, {})
  assert result['OK']
  assert result['Value'] == []
