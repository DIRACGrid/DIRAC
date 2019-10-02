""" DIRAC FileCatalog Storage Element Manager base class for mix-in classes """

__RCSID__ = "$Id$"

import threading

from DIRAC import S_ERROR


class SEManagerBase(object):

  def _refreshSEs(self, connection=False):
    """Refresh the SE cache"""

    return S_ERROR("To be implemented on derived class")

  def __init__(self, database=None):
    self.db = database
    if self.db:
      self.db.seNames = {}
      self.db.seids = {}
      self.db.seDefinitions = {}
    self.lock = threading.Lock()
    self.seUpdatePeriod = 600

    # last time the cache was updated (epoch)
    self.lastUpdate = 0
    self._refreshSEs()

  def setUpdatePeriod(self, period):
    self.seUpdatePeriod = period

  def setSEDefinitions(self, seDefinitions):
    self.db.seDefinitions = seDefinitions
    self.seNames = {}
    for seID, seDef in self.db.seDefinitions.iteritems():
      seName = seDef['SEName']
      self.seNames[seName] = seID

  def setDatabase(self, database):
    self.db = database
    self.db.seNames = {}
    self.db.seids = {}
    self.db.seDefinitions = {}
    self._refreshSEs()
