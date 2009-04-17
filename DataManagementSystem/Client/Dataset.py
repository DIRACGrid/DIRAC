""" Dataset class allows the creation and management of datasets.
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
import types,re

class Dataset:

  def __init__(self,handle=None):
    """ Default constructor
    """
    self.lfcPath = '/lhcb/dataset'
    self.lfc = FileCatalog(['LcgFileCatalogCombined'])
    if not self.lfc.isOK():
      self.valid=False
    else:
      self.lfns = []
      self.replicas = {}
      self.handle = ''
      self.valid = True
      if handle:
        self.handle = handle
        res = self.__retriveDataset()
        if not res['OK']:
          gLogger.fatal("Failed to create dataset.", res['Message'])
          self.valid = False

  def isOK(self):
    return self.valid

  def setLfns(self,lfns):
    self.lfns = lfns

  def getLfns(self):
    if not self.lfns:
      res = self.__retriveDataset()
      if not res['OK']:
        return res
    return S_OK(self.lfns)

  def setHandle(self,handle):
    self.handle = handle

  def getHandle(self):
    if not self.handle:
      return S_ERROR("No handle defined")
    return S_OK(self.handle)

  def getReplicas(self):
    if not self.replicas:
      res = self.__retriveDataset()
      if not res['OK']:
        return res
    return S_OK(self.replicas)

  def createDataset(self):
    if not self.handle:
      return S_ERROR("No handle defined")
    if not self.lfns:
      return S_ERROR("No LFNs defined")
    lfcDir = "%s%s" % (self.lfcPath,self.handle)
    res = self.lfc.createDataset({lfcDir:self.lfns})
    if not res['OK']:
      return res
    elif not lfcDir in res['Value']['Successful'].keys():
      return S_ERROR("Failed to create dataset: %s" % res['Value']['Failed'][lfcDir])
    else:
      return S_OK()

  def removeFile(self,lfn):
    if type(lfn) in types.StringTypes:
      lfn = [lfn]
    if not self.handle:
      return S_ERROR("No handle defined")
    lfcDir = "%s%s" % (self.lfcPath,self.handle)
    res = self.lfc.removeFileFromDataset({lfcDir:lfn})
    if not res['OK']:
      return res
    elif not lfcDir in res['Value']['Successful'].keys():
      return S_ERROR(res['Value']['Failed'][lfcDir]) 
    else:
      res = self.__retriveDataset()
      return res

  def removeDataset(self):
    if not self.handle:
      return S_ERROR("No handle defined")
    lfcDir = "%s%s" % (self.lfcPath,self.handle)
    res = self.lfc.removeDataset(lfcDir)
    if not res['OK']:
      return res
    elif not lfcDir in res['Value']['Successful'].keys():
      return S_ERROR("Failed to remove dataset: %s" % res['Value']['Failed'][lfcDir])
    else:
      self.lfns = []
      self.replicas = {}       
      return S_OK()

  def __retriveDataset(self):
    if not self.handle:
      return S_ERROR("No handle defined")
    lfcDir = "%s%s" % (self.lfcPath,self.handle)
    res = self.lfc.resolveDataset(lfcDir)
    if not res['OK']:
      return res
    elif not lfcDir in res['Value']['Successful'].keys():
      return S_ERROR(res['Value']['Failed'][lfcDir])
    self.lfns = res['Value']['Successful'][lfcDir].keys()
    self.replicas = res['Value']['Successful'][lfcDir]
    return S_OK()
