#!/usr/bin/env python
""" Tests the MySQL class
"""

# FIXME: to bring back to life

import time
import DIRAC
from DIRAC.Core.Utilities.MySQL import MySQL

nThread = 3
nRetrieval = 100000

DIRAC.gLogger.initialize('test_MySQL', '/testSectionVerbose')
# DIRAC.gLogger.initialize('test_MySQL','/testSection')


class MyDB(MySQL):

  def __init__(self, *stArgs, **stKeyArgs):
    self.gLogger = DIRAC.gLogger.getSubLogger('MyDB')
    MySQL.__init__(self, *stArgs, **stKeyArgs)

  def checktable(self):
    retDict = self._update('DROP TABLE IF EXISTS `MyDB_testTable`')
    if not retDict['OK']:
      return retDict
    retDict = self._update('CREATE TABLE `MyDB_testTable` ( '
                           '`ID` INTEGER NOT NULL AUTO_INCREMENT, '
                           '`LastUpdate` TIMESTAMP, '
                           '`Status` varchar(128), '
                           'PRIMARY KEY (`ID`) )')
    if not retDict['OK']:
      return retDict
    return DIRAC.S_OK()

  def filltable(self, entries):
    for i in xrange(1, entries + 1):
      retDict = self.insertFields('MyDB_testTable',
                                  inFields=['Status'],
                                  inValues=[i])
      if not retDict['OK']:
        return retDict
    return DIRAC.S_OK(i)

  def listtable(self, entries):
    for i in xrange(1, entries + 1):
      retDict = self._getFields('MyDB_testTable', [],
                                inFields=['Status'],
                                inValues=[i])
      if not retDict['OK']:
        return retDict
    return DIRAC.S_OK(i)

  def insert(self, status):
    return self.insertFields('MyDB_testTable',
                             inFields=['Status'],
                             inValues=[status])

  def retrieve(self, id):
    return self._getFields('MyDB_testTable', ['Status'],
                           inFields=['ID'], inValues=[id])

  def droptable(self):
    retDict = self._update('DROP TABLE IF EXISTS `MyDB_testTable`')
    if not retDict['OK']:
      return retDict
    return DIRAC.S_OK()


DB = MyDB('Ricardo', 'Dirac', 'CKM-best', 'test', nThread)

testMajorStatusTable = {'Table': 'State',
                        'Description': 'VARCHAR(128)'}
testMinorStatusTable = {'Table': 'State',
                        'Description': 'VARCHAR(128)'}
testTable = {'Id': 'INT NOT NULL AUTO_INCREMENT',
             'Name': 'VARCHAR(128)',
             'MajorState': testMajorStatusTable,
             'MinorState': testMajorStatusTable,
             'Site': 'VARCHAR(128)',
             }

testTableIndexDict = {'Site': ['`Site`']}

testKeys = ['Id']

tableDict = {'test': {'Fields': {'ID': 'INT NOT NULL AUTO_INCREMENT',
                                 'Name': 'VARCHAR(128)',
                                 },
                      'ForeignKeys': {'Name': 'Site',
                                      'Major': 'Status',
                                      'Minor': 'Status',
                                      'Application': 'Status',
                                      },
                      'PrimaryKey': 'ID',
                      'Indexes': {'test': ['`Name`', '`ID`']}
                      },
             'Site': {'Fields': {'Name': 'VARCHAR(128)',
                                 },
                      },
             'Status': {'Fields': {'Major': 'VARCHAR(24)',
                                   'Minor': 'VARCHAR(24)',
                                   'Application': 'VARCHAR(128)',
                                   },
                        }
             }

# print DB._createTables( tableDict , force = True )

# DIRAC.exit()

import threading
semaphore = threading.Semaphore(nThread)
lock = threading.Lock()

Success = 0
Error = 0


def testMultiThreading(tries):
  import random
  DIRAC.gLogger.info('Testing MySQL MultiThreading')
  DIRAC.gLogger.info('First adding 10 K records')
  if not DB.checktable()['OK']:
    return DIRAC.S_ERROR()
  if not DB.filltable(10000)['OK']:
    return DIRAC.S_ERROR()

  i = 0
  # overthread = 0
  DIRAC.gLogger.info('Now querying 100 K in MultiThread mode')
  while i < tries:
    if not i % 1000:
      DIRAC.gLogger.info('Query:', i)
      overthread = 0
    i += 1
    id = int(random.uniform(0, 10000)) + 1
    t = threading.Thread(target=testRetrieve, args=(id, ))
    semaphore.acquire()
    t.start()
  n = threading.activeCount()
  while n > 1:
    DIRAC.gLogger.info('Waiting for Treads to end:', n)
    n = threading.activeCount()
    time.sleep(0.1)

  DIRAC.gLogger.info('Total retrieved values', Success)
  DIRAC.gLogger.info('Total Errors', Error)
  return DIRAC.S_OK((Success, Error))


def testRetrieve(id):
  global Success
  global Error

  retDict = DB.retrieve(id)
  while not retDict['OK']:
    lock.acquire()
    Error += 1
    lock.release()
    retDict = DB.retrieve(id)
  if retDict['Value'] == ((str(id), ), ):
    lock.acquire()
    Success += 1
    lock.release()
  else:
    DIRAC.gLogger.error(id)
  semaphore.release()


testlist = [{'method': DB._connect,
             'arguments': (),
             'output': {'OK': True, 'Value': ''}
             },
            {'method': DB.checktable,
             'arguments': (),
             'output': {'OK': True, 'Value': ''}
             },
            {'method': DB.filltable,
             'arguments': (10, ),
             'output': {'OK': True, 'Value': 10}
             },
            {'method': DB.insert,
             'arguments': ('`', ),
             'output': {'OK': True, 'Value': 1}
             },
            {'method': DB.retrieve,
             'arguments': (11, ),
             'output': {'OK': True, 'Value': (('`', ), )}
             },
            {'method': DB.insert,
             'arguments': ('"', ),
             'output': {'OK': True, 'Value': 1}
             },
            {'method': DB.retrieve,
             'arguments': (12, ),
             'output': {'OK': True, 'Value': (('"', ), )}
             },
            {'method': DB.insert,
             'arguments': ('\'', ),
             'output': {'OK': True, 'Value': 1}
             },
            {'method': DB.retrieve,
             'arguments': (13, ),
             'output': {'OK': True, 'Value': (("'", ), )}
             },
            {'method': DB.insert,
             'arguments': ('`', ),
             'output': {'OK': True, 'Value': 1}
             },
            {'method': DB.retrieve,
             'arguments': (14, ),
             'output': {'OK': True, 'Value': (("`", ), )}
             },
            {'method': DB.listtable,
             'arguments': (10, ),
             'output': {'OK': True, 'Value': 10}
             },
            {'method': testMultiThreading,
             'arguments': (nRetrieval, ),
             'output': {'OK': True, 'Value': (nRetrieval, 0)}
             },
            #            { 'method'    : DB.droptable,
            #              'arguments' : ( ),
            #              'output'    : {'OK': True, 'Value': ''}
            #            },
            ]

testdict = {'DIRAC.MySQL': testlist}

DIRAC.exit()
