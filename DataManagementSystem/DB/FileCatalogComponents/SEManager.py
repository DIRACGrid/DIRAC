########################################################################
# $Id$
########################################################################
""" DIRAC FileCatalog Storage Element Manager mix-in class """

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Pfn import pfnunparse
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
import threading, time, random
from types import StringTypes, IntType, LongType

class SEManagerBase:

  _tables = {}
  _tables['FC_StorageElements'] = { "Fields":
                                     { 
                                       "SEID": "INTEGER AUTO_INCREMENT",
                                       "SEName": "VARCHAR(127) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL",
                                       "SEPrefix": "VARCHAR(127) NOT NULL", 
                                       "AliasName": "VARCHAR(127) DEFAULT ''"
                                     }, 
                                     "PrimaryKey": "SEID",
                                     "UniqueIndexes": {"SEName":["SEName"]}  
                                   }

  def __init__( self, database=None ):
    self.db = None
    if database is not None:
      self.setDatabase( database )
    self.lock = threading.Lock()
    self.seUpdatePeriod = 600
    self.resourcesHelper = Resources()
    self._refreshSEs()
    
  def _refreshSEs( self ):
    return S_ERROR( 'Should be implemented in a derived class' )  
    
  def setUpdatePeriod( self, period ): 
    self.seUpdatePeriod = period
    
  def setSEDefinitions( self, seDefinitions ):
    self.db.seDefinitions = seDefinitions
    self.seNames= {}
    for seID,seDef in self.db.seDefinitions.items():
      seName = seDef['SEName']
      self.seNames[seName] = seID

  def setDatabase(self,database):
    self.db = database  
    result = self.db._createTables( self._tables )
    if not result['OK']:
      gLogger.error( "Failed to create tables", str( self._tables.keys() ) )
    elif result['Value']:
      gLogger.info( "Tables created: %s" % ','.join( result['Value'] ) )  
    return result

  def _getConnection(self,connection):
    if connection:
      return connection
    res = self.db._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn("Failed to get MySQL connection",res['Message'])
    return connection
    
class SEManagerDB(SEManagerBase):

  def _refreshSEs(self,connection=False):
    connection = self._getConnection(connection)
    req = "SELECT SEID,SEName FROM FC_StorageElements;"   
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("SEManager RefreshSEs lock created. Waited %.3f seconds." % (waitTime-startTime))
    res = self.db._query(req,connection)
    if not res['OK']:
      gLogger.debug("SEManager RefreshSEs lock released. Used %.3f seconds." % (time.time()-waitTime))
      self.lock.release()
      return res
    self.db.seNames = {}
    self.db.seids = {}
    self.db.seDefinitions = {}
    for seid,seName in res['Value']:
      self.db.seNames[seName] = seid
      self.db.seids[seid] = seName
      self.getSEDefinition(seid)
    gLogger.debug("SEManager RefreshSEs lock released. Used %.3f seconds." % (time.time()-waitTime))
    self.lock.release()
    return S_OK()

  def __addSE(self,seName,connection=False):
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("SEManager AddSE lock created. Waited %.3f seconds. %s" % (waitTime-startTime,seName))
    if seName in self.db.seNames.keys():
      seid = self.db.seNames[seName]
      gLogger.debug("SEManager AddSE lock released. Used %.3f seconds. %s" % (time.time()-waitTime,seName))
      self.lock.release()
      return S_OK(seid)
    connection = self._getConnection(connection)
    res = self.db._insert('FC_StorageElements',['SEName'],[seName],connection)
    if not res['OK']:
      gLogger.debug("SEManager AddSE lock released. Used %.3f seconds. %s" % (time.time()-waitTime,seName))
      self.lock.release()
      if "Duplicate entry" in res['Message']:
        result = self._refreshSEs( connection )
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
    gLogger.debug("SEManager AddSE lock released. Used %.3f seconds. %s" % (time.time()-waitTime,seName))
    self.lock.release()
    return S_OK(seid)

  def __removeSE(self,seName,connection=False):
    connection = self._getConnection(connection)
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("SEManager RemoveSE lock created. Waited %.3f seconds. %s" % (waitTime-startTime,seName))
    seid = self.db.seNames.get(seName,'Missing')
    req = "DELETE FROM FC_StorageElements WHERE SEName='%s'" % seName
    res = self.db._update(req,connection)
    if not res['OK']:
      gLogger.debug("SEManager RemoveSE lock released. Used %.3f seconds. %s" % (time.time()-waitTime,seName))
      self.lock.release()
      return res
    if seid != 'Missing':
      self.db.seNames.pop(seName)
      self.db.seids.pop(seid)
      self.db.seDefinitions.pop(seid)
    gLogger.debug("SEManager RemoveSE lock released. Used %.3f seconds. %s" % (time.time()-waitTime,seName))
    self.lock.release()
    return S_OK()
  
  def findSE(self,seName):
    return self.getSEID(seName)

  def getSEID(self,seName):
    """ Get ID for a SE specified by its name """
    if type(seName) in [IntType,LongType]:
      return S_OK(seName)
    if seName in self.db.seNames.keys():
      return S_OK(self.db.seNames[seName])
    return self.__addSE(seName)

  def addSE(self,seName):
    return self.getSEID(seName)

  def getSEName(self,seID):
    if seID in self.db.seids.keys():
      return S_OK(self.db.seids[seID])
    return S_ERROR('SE id %d not found' % seID)

  def deleteSE(self,seName,force=True):
    # ToDo: Check first if there are replicas using this SE
    if not force:
      pass
    return self.__removeSE(seName)
  
  def getSEDefinition(self,seID):
    """ Get the Storage Element definition
    """
    if type(seID) in StringTypes:
      result = self.getSEID(seID)
      if not result['OK']:
        return result
      seID = result['Value']

    if seID in self.db.seDefinitions:
      if (time.time()-self.db.seDefinitions[seID]['LastUpdate']) < self.seUpdatePeriod:
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
    result = self.resourcesHelper.getStorageElementOptionsDict( se )
    if not result['OK']:
      return result
    seDict = result['Value']
    self.db.seDefinitions[seID]['SEDict'] = seDict
    if seDict:
      # A.T. Ports can be multiple, this can be better done using the Storage plugin
      # to provide the replica prefix to keep implementations in one place
      if 'Port' in seDict:
        ports = seDict['Port']
        if ',' in ports:
          portList = [ x.strip() for x in ports.split(',') ]
          random.shuffle( portList )
          seDict['Port'] = portList[0]  
      tmpDict = dict(seDict)
      tmpDict['FileName'] = ''
      result = pfnunparse(tmpDict)
      if result['OK']:
        self.db.seDefinitions[seID]['SEDict']['PFNPrefix'] = result['Value'] 
    self.db.seDefinitions[seID]['LastUpdate'] = time.time()
    return S_OK(self.db.seDefinitions[seID])

  def getSEPrefixes( self, connection=False ):
    
    result = self._refreshSEs(connection)
    if not result['OK']:
      return result
    
    resultDict = {}
    
    for seID in self.db.seDefinitions:
      resultDict[self.db.seDefinitions[seID]['SEName']] = \
         self.db.seDefinitions[seID]['SEDict'].get( 'PFNPrefix', '' )

    return S_OK( resultDict )

class SEManagerCS(SEManagerBase):

  def findSE(self,se):
    return S_OK(se)

  def addSE(self,se):
    return S_OK(se)
  
  def getSEDefinition(self,se):
    #TODO Think about using a cache for this information
    return self.resourcesHelper.getStorageElementOptionsDict( se )
