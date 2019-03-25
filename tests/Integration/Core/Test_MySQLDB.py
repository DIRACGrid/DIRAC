""" This is a test code for this class, it requires access to a MySQL DB
"""

from __future__ import print_function
import os
import sys
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.MySQL import MySQL

if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
  gLogger.info( 'Unset python optimization "PYTHONOPTIMIZE"' )
  sys.exit( 0 )

gLogger.info( 'Testing MySQL class...' )

HOST = '127.0.0.1'
USER = 'Dirac'
PWD = 'Dirac'
DB = 'AccountingDB'

TESTDB = MySQL( HOST, USER, PWD, DB )
assert TESTDB._connect()['OK']

TESTDICT = { 'TestTable' : { 'Fields': { 'ID'      : "INTEGER UNIQUE NOT NULL AUTO_INCREMENT",
                                         'Name'    : "VARCHAR(255) NOT NULL DEFAULT 'Yo'",
                                         'Surname' : "VARCHAR(255) NOT NULL DEFAULT 'Tu'",
                                         'Count'   : "INTEGER NOT NULL DEFAULT 0",
                                         'Time'    : "DATETIME",
                                       },
                             'PrimaryKey': 'ID'
                           }
           }

NAME = 'TestTable'
FIELDS = [ 'Name', 'Surname' ]
NEWVALUES = [ 'Name2', 'Surn2' ]
SOMEFIELDS = [ 'Name', 'Surname', 'Count' ]
ALLFIELDS = [ 'ID', 'Name', 'Surname', 'Count', 'Time' ]
ALLVALUES = [ 1, 'Name1', 'Surn1', 1, 'UTC_TIMESTAMP()' ]
ALLDICT = dict( Name = 'Name1', Surname = 'Surn1', Count = 1, Time = 'UTC_TIMESTAMP()' )
COND0 = {}
COND10 = {'Count': range( 10 )}

try:
  RESULT = TESTDB._createTables( TESTDICT, force = True )
  assert RESULT['OK']
  print('Table Created')

  RESULT = TESTDB.getCounters( NAME, FIELDS, COND0 )
  assert RESULT['OK']
  assert RESULT['Value'] == []

  RESULT = TESTDB.getDistinctAttributeValues( NAME, FIELDS[0], COND0 )
  assert RESULT['OK']
  assert RESULT['Value'] == []

  RESULT = TESTDB.getFields( NAME, FIELDS )
  assert RESULT['OK']
  assert RESULT['Value'] == ()

  print('Inserting')

  for J in range( 100 ):
    RESULT = TESTDB.insertFields( NAME, SOMEFIELDS, ['Name1', 'Surn1', J] )
    assert RESULT['OK']
    assert RESULT['Value'] == 1
    assert RESULT['lastRowId'] == J + 1

  print('Querying')

  RESULT = TESTDB.getCounters( NAME, FIELDS, COND0 )
  assert RESULT['OK']
  assert RESULT['Value'] == [({'Surname': 'Surn1', 'Name': 'Name1'}, 100)]

  RESULT = TESTDB.getDistinctAttributeValues( NAME, FIELDS[0], COND0 )
  assert RESULT['OK']
  assert RESULT['Value'] == ['Name1']

  RESULT = TESTDB.getFields( NAME, FIELDS )
  assert RESULT['OK']
  assert len( RESULT['Value'] ) == 100

  RESULT = TESTDB.getFields( NAME, SOMEFIELDS, COND10 )
  assert RESULT['OK']
  assert len( RESULT['Value'] ) == 10

  RESULT = TESTDB.getFields( NAME, limit = 1 )
  assert RESULT['OK']
  assert len( RESULT['Value'] ) == 1

  RESULT = TESTDB.getFields( NAME, ['Count'], orderAttribute = 'Count:DESC', limit = 1 )
  assert RESULT['OK']
  assert RESULT['Value'] == ( ( 99, ), )

  RESULT = TESTDB.getFields( NAME, ['Count'], orderAttribute = 'Count:ASC', limit = 1 )
  assert RESULT['OK']
  assert RESULT['Value'] == ( ( 0, ), )

  RESULT = TESTDB.getCounters( NAME, FIELDS, COND10 )
  assert RESULT['OK']
  assert RESULT['Value'] == [({'Surname': 'Surn1', 'Name': 'Name1'}, 10)]

  RESULT = TESTDB._getFields( NAME, FIELDS, COND10.keys(), COND10.values() )
  assert RESULT['OK']
  assert len( RESULT['Value'] ) == 10

  RESULT = TESTDB.updateFields( NAME, FIELDS, NEWVALUES, COND10 )
  assert RESULT['OK']
  assert RESULT['Value'] == 10

  RESULT = TESTDB.updateFields( NAME, FIELDS, NEWVALUES, COND10 )
  assert RESULT['OK']
  assert RESULT['Value'] == 0

  print('Removing')

  RESULT = TESTDB.deleteEntries( NAME, COND10 )
  assert RESULT['OK']
  assert RESULT['Value'] == 10

  RESULT = TESTDB.deleteEntries( NAME )
  assert RESULT['OK']
  assert RESULT['Value'] == 90

  RESULT = TESTDB.getCounters( NAME, FIELDS, COND0 )
  assert RESULT['OK']
  assert RESULT['Value'] == []

  RESULT = TESTDB.insertFields( NAME, inFields = ALLFIELDS, inValues = ALLVALUES )
  assert RESULT['OK']
  assert RESULT['Value'] == 1

  time.sleep( 1 )

  RESULT = TESTDB.insertFields( NAME, inDict = ALLDICT )
  assert RESULT['OK']
  assert RESULT['Value'] == 1

  time.sleep( 2 )
  RESULT = TESTDB.getFields( NAME, older = 'UTC_TIMESTAMP()', timeStamp = 'Time' )
  assert RESULT['OK']
  assert len( RESULT['Value'] ) == 2

  RESULT = TESTDB.getFields( NAME, newer = 'UTC_TIMESTAMP()', timeStamp = 'Time' )
  assert len( RESULT['Value'] ) == 0

  RESULT = TESTDB.getFields( NAME, older = Time.toString(), timeStamp = 'Time' )
  assert RESULT['OK']
  assert len( RESULT['Value'] ) == 2

  RESULT = TESTDB.getFields( NAME, newer = Time.dateTime(), timeStamp = 'Time' )
  assert RESULT['OK']
  assert len( RESULT['Value'] ) == 0

  RESULT = TESTDB.deleteEntries( NAME )
  assert RESULT['OK']
  assert RESULT['Value'] == 2

  print('OK')

except AssertionError:
  print('ERROR ', end=' ')
  if not RESULT['OK']:
    print(RESULT['Message'])
  else:
    print(RESULT)
