""" DIRAC FileCatalog Storage Element Manager mix-in class """

__RCSID__ = "$Id$"

import threading
import time
import random

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities.Pfn import pfnunparse


class SEManagerBase:

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
    for seID, seDef in self.db.seDefinitions.items():
      seName = seDef['SEName']
      self.seNames[seName] = seID

  def setDatabase(self, database):
    self.db = database
    self.db.seNames = {}
    self.db.seids = {}
    self.db.seDefinitions = {}
    self._refreshSEs()


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

    for seName, seId in self.db.seNames.items():
      if seName not in seNames:
        del self.db.seNames[seName]
        del self.db.seids[seId]
        del self.db.seDefinitions[seId]

    for seid, seName in res['Value']:
      self.getSEDefinition(seid)
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
    self.getSEDefinition(seid)
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
      self.db.seDefinitions.pop(seid)
    gLogger.debug("SEManager RemoveSE lock released. Used %.3f seconds. %s" % (time.time() - waitTime, seName))
    self.lock.release()
    return S_OK()

  def findSE(self, seName):
    return self.getSEID(seName)

  def getSEID(self, seName):
    """ Get ID for a SE specified by its name """
    if isinstance(seName, (int, long)):
      return S_OK(seName)
    if seName in self.db.seNames.keys():
      return S_OK(self.db.seNames[seName])
    return self.__addSE(seName)

  def addSE(self, seName):
    return self.getSEID(seName)

  def getSEName(self, seID):
    if seID in self.db.seids.keys():
      return S_OK(self.db.seids[seID])
    return S_ERROR('SE id %d not found' % seID)

  def deleteSE(self, seName, force=True):
    # ToDo: Check first if there are replicas using this SE
    if not force:
      pass
    return self.__removeSE(seName)

  def getSEDefinition(self, seID):
    """ Get the Storage Element definition
    """
    if isinstance(seID, str):
      result = self.getSEID(seID)
      if not result['OK']:
        return result
      seID = result['Value']

    if seID in self.db.seDefinitions:
      if (time.time() - self.db.seDefinitions[seID]['LastUpdate']) < self.seUpdatePeriod:
        if self.db.seDefinitions[seID]['SEDict']:
          return S_OK(self.db.seDefinitions[seID])
      se = self.db.seDefinitions[seID]['SEName']
    else:
      result = self.getSEName(seID)
      if not result['OK']:
        return result
      se = result['Value']
      self.db.seDefinitions[seID] = {}
      self.db.seDefinitions[seID]['SEName'] = se
      self.db.seDefinitions[seID]['SEDict'] = {}
      self.db.seDefinitions[seID]['LastUpdate'] = 0.

    # We have to refresh the SE definition from the CS
    result = gConfig.getSections('/Resources/StorageElements/%s' % se)
    if not result['OK']:
      return result
    pluginSection = result['Value'][0]
    result = gConfig.getOptionsDict('/Resources/StorageElements/%s/%s' % (se, pluginSection))
    if not result['OK']:
      return result
    seDict = result['Value']
    self.db.seDefinitions[seID]['SEDict'] = seDict
    # Get VO paths if any
    voPathDict = None
    result = gConfig.getOptionsDict('/Resources/StorageElements/%s/%s/VOPath' % (se, pluginSection))
    if result['OK']:
      voPathDict = result['Value']
    if seDict:
      # A.T. Ports can be multiple, this can be better done using the Storage plugin
      # to provide the replica prefix to keep implementations in one place
      if 'Port' in seDict:
        ports = seDict['Port']
        if ',' in ports:
          portList = [x.strip() for x in ports.split(',')]
          random.shuffle(portList)
          seDict['Port'] = portList[0]
      tmpDict = dict(seDict)
      tmpDict['FileName'] = ''
      result = pfnunparse(tmpDict)
      if result['OK']:
        self.db.seDefinitions[seID]['SEDict']['PFNPrefix'] = result['Value']
      if voPathDict is not None:
        for vo in voPathDict:
          tmpDict['Path'] = voPathDict[vo]
          result = pfnunparse(tmpDict)
          if result['OK']:
            self.db.seDefinitions[seID]['SEDict'].setdefault("VOPrefix", {})[vo] = result['Value']
    self.db.seDefinitions[seID]['LastUpdate'] = time.time()
    return S_OK(self.db.seDefinitions[seID])

  def getSEPrefixes(self, connection=False):

    result = self._refreshSEs(connection)
    if not result['OK']:
      return result

    resultDict = {}

    for seID in self.db.seDefinitions:
      resultDict[self.db.seDefinitions[seID]['SEName']] = \
          self.db.seDefinitions[seID]['SEDict'].get('PFNPrefix', '')

      # Check if some paths are specific for VO's and add these definitions
      if self.db.seDefinitions[seID]['SEDict'].get('VOPrefix'):
        resultDict.setdefault('VOPrefix', {})
        resultDict['VOPrefix'][self.db.seDefinitions[seID]['SEName']] = \
            self.db.seDefinitions[seID]['SEDict'].get('VOPrefix')

    return S_OK(resultDict)


class SEManagerCS(SEManagerBase):

  def findSE(self, se):
    return S_OK(se)

  def addSE(self, se):
    return S_OK(se)

  def getSEDefinition(self, se):
    # TODO Think about using a cache for this information
    return gConfig.getOptionsDict('/Resources/StorageElements/%s/AccessProtocol.1' % se)
