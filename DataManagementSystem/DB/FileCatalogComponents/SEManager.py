########################################################################
# $Id: SEManager.py 22623 2010-03-09 19:54:25Z acsmith $
########################################################################
""" DIRAC FileCatalog Storage Element Manager mix-in class """

__RCSID__ = "$Id: SEManager.py 22623 2010-03-09 19:54:25Z acsmith $"

import time
from DIRAC import S_OK, S_ERROR, gConfig

class SEManagerBase:

  def __init__(self,database=None):
    self.db = database
    self.seDefinitions = {}
    self.seNames = {}
    self.seUpdatePeriod = 600
    
  def setUpdatePeriod(self,period): 
    self.seUpdatePeriod = period
    
  def setSEDefinitions(self,seDefinitions):
    self.seDefinitions = seDefinitions
    self.seNames= {}
    for seID,seDef in self.seDefinitions.items():
      seName = seDef['SEName']
      self.seNames[seName] = seID

  def setDatabase(self,database):
    self.db = database  
    
class SEManagerDB(SEManagerBase):

  def findSE(self,se):
    """ Find the ID of the given SE, add it to the catalog if it is not yet there
    """
    if se in self.seNames.keys():
      return S_OK(self.seNames[se])
    
    # Look for the SE definition in the database
    seID = 0
    query = "SELECT SEID FROM FC_StorageElements WHERE SEName='%s'" % se   
    resQuery = self.db._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        seID = int(resQuery['Value'][0][0])
    else:
      return S_ERROR('Failed to query SE database') 

    if not seID:    
      # The SE is not yet in the catalog, add it
      result = self.addSE(se)
      if not result['OK']:
        return result
      seID = result['Value']
    
    self.seNames[se] = seID
    
    return S_OK(seID)
  
  def getSEName(self,seID):
    """ Get the name of Storage Element specified by seID
    """
    if seID in self.seDefinitions:
      return S_OK(self.seDefinitions[seID]['SEName'])
    
    query = "SELECT SEName FROM FC_StorageElements WHERE SEID=%d" % seID   
    resQuery = self.db._query(query)
    if not resQuery['OK']:
      return resQuery
    if not resQuery['Value']:
      return S_ERROR('SE Name for %d not found' % seID)
    return S_OK(resQuery['Value'][0][0])
  
  def addSE(self,se):
    """ Add a Storage Element to the catalog
    """
    req = "SELECT SEID FROM FC_StorageElements WHERE SEName='%s'" % se
    result = self.db._query(req)
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK(result['Value'][0][0])
    
    # Add a new SE
    result = self.db._insert('FC_StorageElements',['SEName'],[se])
    if not result['OK']:
      return result
    seID = result['lastRowId']
    return S_OK(seID)
  
  def getSEDefinition(self,seID):
    """ Get the Storage Element definition
    """
    if seID in self.seDefinitions:
      if (time.time()-self.seDefinitions[seID]['LastUpdate']) < self.seUpdatePeriod:
        if self.seDefinitions[seID]['SEDict']:
          return S_OK(self.seDefinitions[seID])
      se = self.seDefinitions[seID]['SEName']  
    else:
      result = self.getSEName(seID)
      if not result['OK']:
        return result  
      se = result['Value']
      self.seDefinitions[seID] = {}
      self.seDefinitions[seID]['SEName'] = se
      self.seDefinitions[seID]['SEDict'] = {}
      self.seDefinitions[seID]['LastUpdate'] = 0.
      
    
    # We have to refresh the SE definition from the CS
    result = gConfig.getOptionsDict('/Resources/StorageElements/%s/AccessProtocol.1' % se)
    if not result['OK']:
      return result
    seDict = result['Value']
    self.seDefinitions[seID]['SEDict'] = seDict
    self.seDefinitions[seID]['LastUpdate'] = time.time()
    
    return S_OK(self.seDefinitions[seID])
  
class SEManagerCS(SEManagerBase):

  def findSE(self,se):
    return S_OK(se)

  def addSE(self,se):
    return S_OK(se)
  
  def getSEDefinition(self,se):
    #TODO Think about using a cache for this information
    return gConfig.getOptionsDict('/Resources/StorageElements/%s/AccessProtocol.1' % se)

