########################################################################
# $Id: FileManager.py 22623 2010-03-09 19:54:25Z acsmith $
########################################################################

__RCSID__ = "$Id: FileManager.py 22623 2010-03-09 19:54:25Z acsmith $"

from DIRAC                                  import S_OK,S_ERROR,gLogger
from DIRAC.Core.Utilities.List              import stringListToString,intListToString
from DIRAC.Core.Utilities.Pfn               import pfnparse, pfnunparse

import time,os
from types import *


class FileManagerBase:

  def __init__(self,database=None):
    self.db = database

  def setDatabase(self,database):
    self.db = database  

  def exists(self,lfns):
    return S_ERROR("Not implemented")

  def getFileSize(self,lfns):
    return S_ERROR("Not implemented")

  def getFileMetadata(self,lfns):
    return S_ERROR("Not implemented")

  def getReplicas(self,lfns):
    return S_ERROR("Not implemented")

  def getReplicaStatus(self,lfns):
    return S_ERROR("Not implemented")

  def isFile(self,lfns):
    return S_ERROR("Not implemented")

  def addFile(self,lfns,credDict):
    return S_ERROR("Not implemented")

  def removeFile(self,lfns):
    return S_ERROR("Not implemented")

  def addReplica(self,lfns):
    return S_ERROR("Not implemented")

  def removeReplica(self,lfns):
    return S_ERROR("Not implemented")

  def setReplicaStatus(self,lfns):
    return S_ERROR("Not implemented")

  def setReplicaHost(self,lfns):
    return S_ERROR("Not implemented")
  
  def getFileCounters(self):
    return S_ERROR("Not implemented")

  def getReplicaCounters(self):
    return S_ERROR("Not implemented") 

  def _getFileDirectories(self,lfns):
    dirDict = {}
    for lfn in lfns:
      lfnDir = os.path.dirname(lfn)
      lfnFile = os.path.basename(lfn)
      if not lfnDir in dirDict:
        dirDict[lfnDir] = []
      dirDict[lfnDir].append(lfnFile)
    return dirDict
  
  def _checkLFNPFNConvention(self,lfn,pfn,se):
    """ Check that the PFN corresponds to the LFN-PFN convention """
    if pfn == lfn:
      return S_OK()
    if (len(pfn)<len(lfn)) or (pfn[-len(lfn):] != lfn) :
      return S_ERROR('PFN does not correspond to the LFN convention')
    return S_OK()
