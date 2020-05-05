""" A test DB in DIRAC, using MySQL as backend
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Base.DB import DB


class AtomDB(DB):

  def __init__(self):
    DB.__init__(self, 'AtomDB', 'Test/AtomDB')
    retVal = self.__initializeDB()
    if not retVal['OK']:
      raise Exception("Can't create tables: %s" % retVal['Message'])

  def __initializeDB(self):
    """
    Create the tables
    """
    retVal = self._query("show tables")
    if not retVal['OK']:
      return retVal

    tablesInDB = [t[0] for t in retVal['Value']]
    tablesD = {}

    if 'atom_mytable' not in tablesInDB:
      tablesD['atom_mytable'] = {'Fields': {'Id': 'INTEGER NOT NULL AUTO_INCREMENT', 'Stuff': 'VARCHAR(64) NOT NULL'},
                                 'PrimaryKey': ['Id']
                                 }

    return self._createTables(tablesD)

  def addStuff(self, something):
    return self.insertFields('atom_mytable', ['stuff'], [something])
