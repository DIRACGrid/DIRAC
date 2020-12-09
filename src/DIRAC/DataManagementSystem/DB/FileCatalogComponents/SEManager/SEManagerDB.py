""" DIRAC FileCatalog Storage Element Manager mix-in class for SE definitions within the FC database"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import time
import random

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities.Pfn import pfnunparse
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SEManager.SEManagerBase import SEManagerBase


class SEManagerDB(SEManagerBase):

  def _refreshSEs(self, connection=False):

    req = "SELECT SEID,SEName FROM FC_StorageElements;"
    res = self.db._query(req)
    if not res['OK']:
      return res
    seNames = set([se[1] for se in res['Value']])

    # If there are no changes between the DB and the cache
    # and we updated the cache recently enough, just return
    if seNames == set(self.db.seNames) and ((time.time() - self.lastUpdate) < self.seUpdatePeriod):
      return S_OK()

    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("SEManager RefreshSEs lock created. Waited %.3f seconds." % (waitTime - startTime))

    # Check once more the lastUpdate because it could have been updated while we were waiting for the lock
    if (time.time() - self.lastUpdate) < self.seUpdatePeriod:
      self.lock.release()
      return S_OK()

    for seName, seId in list(self.db.seNames.items()):
      if seName not in seNames:
        del self.db.seNames[seName]
        del self.db.seids[seId]

    for seid, seName in res['Value']:
      self.db.seNames[seName] = seid
      self.db.seids[seid] = seName
    gLogger.debug("SEManager RefreshSEs lock released. Used %.3f seconds." % (time.time() - waitTime))

    # Update the lastUpdate time
    self.lastUpdate = time.time()
    self.lock.release()
    return S_OK()

  def __addSE(self, seName, connection=False):
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("SEManager AddSE lock created. Waited %.3f seconds. %s" % (waitTime - startTime, seName))
    if seName in self.db.seNames.keys():
      seid = self.db.seNames[seName]
      gLogger.debug("SEManager AddSE lock released. Used %.3f seconds. %s" % (time.time() - waitTime, seName))
      self.lock.release()
      return S_OK(seid)
    connection = self.db._getConnection()
    res = self.db.insertFields('FC_StorageElements', ['SEName'], [seName], connection)
    if not res['OK']:
      gLogger.debug("SEManager AddSE lock released. Used %.3f seconds. %s" % (time.time() - waitTime, seName))
      self.lock.release()
      if "Duplicate entry" in res['Message']:
        result = self._refreshSEs(connection)
        if not result['OK']:
          return result
        if seName in self.db.seNames.keys():
          seid = self.db.seNames[seName]
          return S_OK(seid)
      return res
    seid = res['lastRowId']
    self.db.seids[seid] = seName
    self.db.seNames[seName] = seid
    gLogger.debug("SEManager AddSE lock released. Used %.3f seconds. %s" % (time.time() - waitTime, seName))
    self.lock.release()
    return S_OK(seid)

  def __removeSE(self, seName, connection=False):
    connection = self.db._getConnection()
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("SEManager RemoveSE lock created. Waited %.3f seconds. %s" % (waitTime - startTime, seName))
    seid = self.db.seNames.get(seName, 'Missing')
    req = "DELETE FROM FC_StorageElements WHERE SEName='%s'" % seName
    res = self.db._update(req, connection)
    if not res['OK']:
      gLogger.debug("SEManager RemoveSE lock released. Used %.3f seconds. %s" % (time.time() - waitTime, seName))
      self.lock.release()
      return res
    if seid != 'Missing':
      self.db.seNames.pop(seName)
      self.db.seids.pop(seid)
    gLogger.debug("SEManager RemoveSE lock released. Used %.3f seconds. %s" % (time.time() - waitTime, seName))
    self.lock.release()
    return S_OK()

  def findSE(self, seName):
    return self.getSEID(seName)

  def getSEID(self, seName):
    """ Get ID for a SE specified by its name """
    if isinstance(seName, six.integer_types):
      return S_OK(seName)
    if seName in self.db.seNames.keys():
      return S_OK(self.db.seNames[seName])
    return self.__addSE(seName)

  def addSE(self, seName):
    return self.getSEID(seName)

  def getSEName(self, seID):
    """Return the name of the SE.

    Refresh list of SEs if not found, an SE might have been added by a different FileCatalog instance.

    :param int seID: ID of a storage element
    :return: S_OK/S_ERROR
    """
    if seID in self.db.seids:
      return S_OK(self.db.seids[seID])
    gLogger.info("getSEName: seID not found, refreshing", "ID: %s" % seID)
    result = self._refreshSEs(connection=False)
    if not result['OK']:
      gLogger.error("getSEName: refreshing failed", result['Message'])
      return result
    if seID in self.db.seids:
      return S_OK(self.db.seids[seID])
    gLogger.error("getSEName: seID not found after refreshing", "ID: %s" % seID)
    return S_ERROR('SE id %d not found' % seID)

  def deleteSE(self, seName, force=True):
    # ToDo: Check first if there are replicas using this SE
    if not force:
      pass
    return self.__removeSE(seName)
