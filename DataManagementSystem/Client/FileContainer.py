# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Client/FileContainer.py,v 1.3 2009/08/03 14:24:10 acsmith Exp $
__RCSID__ = "$Id: FileContainer.py,v 1.3 2009/08/03 14:24:10 acsmith Exp $"

""" This module contains classes associated to Files.

    The File class contains the member elements: LFN, Status, Size, GUID and Checksum and provides access methods for each (inluding type checking).
"""

import types
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogFile
from DIRAC.DataManagementSystem.Client.ReplicaContainers import CatalogReplica

class File:

  def __init__(self, lfn='', status='', size=0, guid='', checksum=''):
    # These are the possible attributes for a file
    if not type(lfn) in types.StringTypes:
      raise AttributeError, "lfn should be string type"
    self.lfn = str(lfn)
    if not type(status) in types.StringTypes:
      raise AttributeError, "status should be string type"
    self.status = str(status)
    try:
      self.size = int(size)  
    except:
      raise AttributeError, "size should be integer type"
    if not type(guid) in types.StringTypes:
      raise AttributeError, "guid should be string type"
    self.guid = str(guid)
    if not type(checksum) in types.StringTypes:
      raise AttributeError, "checksum should be string type"
    self.checksum = str(checksum)
    self.catalogReplicas = []

  def setLFN(self,lfn):
    if not type(lfn) in types.StringTypes: 
      return S_ERROR("LFN should be %s and not %s" % (types.StringType,type(se)))
    self.lfn = str(lfn)  
    return S_OK()
      
  def setStatus(self,status):
    if not type(status) in types.StringTypes:
      return S_ERROR("Status should be %s and not %s" % (types.StringType,type(status)))
    self.status = str(status)
    return S_OK()

  def setSize(self,size):
    try:
      self.size = int(size)
      return S_OK()
    except:
      return S_ERROR("Size should be %s and not %s" % (types.IntType,type(size)))
      
  def setGUID(self,guid):
    if not type(guid) in types.StringTypes:
      return S_ERROR("GUID should be %s and not %s" % (types.StringType,type(guid)))
    self.guid = str(guid)
    return S_OK()

  def setChecksum(self,checksum):
    if not type(checksum) in types.StringTypes:
      return S_ERROR("Checksum should be %s and not %s" % (types.StringType,type(checksum)))
    self.checksum = str(checksum)
    return S_OK()

  def addCatalogReplica(self,se,pfn,status='U'):
    for replica in self.catalogReplicas:
      if (replica.pfn == pfn) and (replica.se == se):
        return S_OK()
    oCatalogReplica = CatalogReplica(pfn=pfn,storageElement=se,status=status)
    self.catalogReplicas.append(oCatalogReplica)
    return S_OK()
  
  def getLFN(self):
    return S_OK(self.lfn)

  def getStatus(self):
    if self.status:
      return S_OK(self.status)
    if not self.lfn:
      return S_ERROR('No LFN is known')
    res = self.__populateMetadata()
    if not res['OK']:
      return res
    return S_OK(self.status)

  def getSize(self):
    if self.size:
      return S_OK(self.size)
    if not self.lfn:
      return S_ERROR('No LFN is known')
    res = self.__populateMetadata()
    if not res['OK']:
      return res
    return S_OK(self.size)

  def getGUID(self):
    if self.guid:
      return S_OK(self.guid)
    if not self.lfn:
      return S_ERROR('No LFN is known')
    res = self.__populateMetadata()
    if not res['OK']:
      return res
    return S_OK(self.guid)

  def getChecksum(self):
    if self.checksum:
      return S_OK(self.checksum)
    if not self.lfn:
      return S_ERROR('No LFN is known')
    res = self.__populateMetadata()
    if not res['OK']:
      return res
    return S_OK(self.checksum)

  def __populateMetadata(self):
    oCatalog = CatalogFile()
    res = oCatalog.getCatalogFileMetadata(self.lfn,singleFile=True)
    if not res['OK']:
      return res
    metadata = res['Value']
    self.setChecksum(metadata['CheckSumValue'])
    self.setGUID(metadata['GUID'])
    self.setSize(metadata['Size']) 
    self.setStatus(metadata['Status'])
    return S_OK()

  def getReplicas(self):
    if not self.lfn:
      return S_ERROR('No LFN is known')
    if self.catalogReplicas:
      replicas = {}
      for replica in self.catalogReplicas:
        replicas[replica.se] = replica.pfn
      return S_OK(replicas)
    oCatalog = CatalogFile()
    res = oCatalog.getCatalogReplicas(self.lfn,singleFile=True)
    if not res['OK']:
      return res
    replicas = res['Value']
    for se,pfn in replicas.items():
      oCatalogReplica = CatalogReplica(pfn=pfn,storageElement=se,status='U')
      self.catalogReplicas.append(oCatalogReplica)
    return S_OK(replicas)
  
  def digest(self):
    """ Get short description string of file attributes
    """
    return S_OK("%s:%s:%d:%s:%s" % (self.lfn,self.status,self.size,self.guid,self.checksum))
