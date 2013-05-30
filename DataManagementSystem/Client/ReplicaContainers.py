# $HeadURL$
__RCSID__ = "$Id$"

""" This module contains three classes associated to Replicas.

    The Replica class contains simply three member elements:  SE, PFN and Status and provides access methods for each (inluding type checking).
    The CatalogReplica class inherits the Replica class.
    The PhysicalReplica class inherits the Replica class and adds the 'size','checksum','online' and 'migrated' members.

    In this context Replica refers to any copy of a file. This can be the first or an additional copy.

    OBSOLETE?
    K.C.
"""

import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.CFG import CFG   

class Replica:

  def __init__(self,pfn='',storageElement='',status=''):
    # These are the possible attributes for a replica
    if not type(pfn) in types.StringTypes:
      raise AttributeError, "pfn should be string type"
    self.pfn = str(pfn)
    if not type(storageElement) in types.StringTypes:
      raise AttributeError, "storageElement should be string type"
    self.se = str(storageElement)
    if not type(status) in types.StringTypes:
      raise AttributeError, "status should be string type"
    self.status = str(status)

  def setPFN(self,pfn):
    if not type(pfn) in types.StringTypes:
      return S_ERROR("PFN should be %s and not %s" % (types.StringType,type(pfn)))
    self.pfn = str(pfn)
    return S_OK()

  def setSE(self,se):
    if not type(se) in types.StringTypes:
      return S_ERROR("SE should be %s and not %s" % (types.StringType,type(se)))
    self.se = str(se)
    return S_OK()

  def setStatus(self,status):
    if not type(status) in types.StringTypes:
      return S_ERROR("Status should be %s and not %s" % (types.StringType,type(status)))
    self.status = str(status)
    return S_OK()

  def getPFN(self):
    return S_OK(self.pfn)

  def getSE(self):
    return S_OK(self.se)

  def getStatus(self):
    return S_OK(self.status)

  def digest(self):
    """ Get short description string of replica and status
    """
    return S_OK("%s:%s:%s" % (self.se,self.pfn,self.status))

  def toCFG(self):
    oCFG = CFG()
    oCFG.createNewSection(self.se)
    oCFG.setOption('%s/Status' % (self.se), self.status)
    oCFG.setOption('%s/PFN' % (self.se), self.pfn)
    return S_OK(str(oCFG))

class CatalogReplica(Replica):

  def __init__(self,pfn='',storageElement='',status='U'):
    Replica.__init__(self,pfn,storageElement,status)

class PhysicalReplica(Replica):

  def __init__(self,pfn='',storageElement='',status='',size=0,checksum='',online=False,migrated=False):
    # These are the possible attributes for a physical replica
    Replica.__init__(self,pfn,storageElement,status)
    try:
      self.size = int(size)
    except:
      raise AttributeError, "size should be integer type"
    if not type(checksum) in types.StringTypes:
      raise AttributeError, "checksum should be string type"
    self.checksum = str(checksum)
    if not type(online) == types.BooleanType:
      raise AttributeError, "online should be bool type"
    self.online = online
    if not type(migrated) == types.BooleanType:
      raise AttributeError, "migrated should be bool type"
    self.migrated = migrated

  def setSize(self,size):
    try:
      self.size = int(size)
      return S_OK()
    except:
      return S_ERROR("Size should be %s and not %s" % (types.IntType,type(size)))

  def setChecksum(self,checksum):
    if not type(checksum) in types.StringTypes:
      return S_ERROR("Checksum should be %s and not %s" % (types.StringType,type(checksum)))
    self.checksum = str(checksum)
    return S_OK()

  def setOnline(self,online):
    if not type(online) == types.BooleanType:
      return S_ERROR("online should be %s and not %s" % (types.BooleanType,type(online)))
    self.online = online
    return S_OK() 

  def setMigrated(self,migrated):
    if not type(migrated) == types.BooleanType:
      return S_ERROR("migrated should be %s and not %s" % (types.BooleanType,type(migrated)))
    self.migrated = migrated
    return S_OK() 

  def getSize(self):
    return S_OK(self.size)

  def getChecksum(self):
    return S_OK(self.checksum)

  def getOnline(self):
    return S_OK(self.online)
   
  def getMigrated(self):
    return S_OK(self.migrated)

  def digest(self):
    online = 'NotOnline'
    if self.online:
      online = 'Online'
    migrated = 'NotMigrated'
    if self.migrated:
      migrated = 'Migrated'
    return S_OK("%s:%s:%d:%s:%s:%s" % (self.se,self.pfn,self.size,self.status,online,migrated))
