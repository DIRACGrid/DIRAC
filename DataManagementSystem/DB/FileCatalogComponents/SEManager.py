########################################################################
# $HeadURL:  $
########################################################################
""" DIRAC FileCatalog Storage Element Manager mix-in class
"""

__RCSID__ = "$Id:  $"

import time
from DIRAC import S_OK, S_ERROR, gConfig

class SEManager:

  def findSE(self,se):
    """ Find the ID of the given SE, add it to the catalog if it is not yet there
    """
    if se in self.seNames:
      return S_OK(self.seNames[se])
    
    # Look for the SE definition in the database
    seID = 0
    query = "SELECT SEID FROM FC_StorageElements WHERE SEName='%s'" % se   
    resQuery = self._query(query)
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
  
  def __getSEName(self,seID):
    """ Get the name of Storage Element specified by seID
    """
    if seID in self.seDefinitions:
      return S_OK(self.seDefinitions[seID]['SEName'])
    
    query = "SELECT SEName FROM FC_StorageElements WHERE SEID=%d" % seID   
    resQuery = self._query(query)
    if not resQuery['OK']:
      return resQuery
    if not resQuery['Value']:
      return S_ERROR('SE Name for %d not found' % seID)
    return S_OK(resQuery['Value'][0][0])
  
  def addSE(self,se):
    """ Add a Storage Element to the catalog
    """
    req = "SELECT SEID FROM FC_StorageElements WHERE SEName='%s'" % se
    result = self._query(req)
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK(result['Value'][0][0])
    
    # Add a new SE
    result = self._insert('FC_StorageElements',['SEName'],[se])
    if not result['OK']:
      return result
    seID = result['lastRowId']
    return S_OK(seID)
  
  def getSEDefinition(self,seID):
    """ Get the Storage Element definition
    """
    if seID in self.seDefinitions:
      if (time.time()-self.seDefinitions[seID]['LastUpdate']) < self.SEUpdatePeriod:
        if self.seDefinitions[seID]['SEDict']:
          return S_OK(self.seDefinitions[seID])
      se = self.seDefinitions[seID]['SEName']  
    else:
      result = self.__getSEName(seID)
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