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
    self.lfns = []
    self.replicas = {}
    self.handle = ''
    self.valid = True
    if handle:
      self.handle = handle
      res = self.__retriveDataset()
      if not res['OK']:
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

  def getReplicas(self):
    if not self.replicas:
      res = self.__retriveDataset()
      if not res['OK']:
        return res
    return S_OK(self.replicas)

  def setHandle(self,handle):
    self.handle = handle

  def getHandle(self):
    if not self.handle:
      return S_ERROR("No handle defined")
    return S_OK(self.handle)

  def removeFile(self,lfn):
    if not self.handle:
      return S_ERROR("No handle defined")
    lfcDir = "%s%s" % (self.lfcPath,self.handle)
    res = self.lfc.removeFileFromDataset(lfcDir,lfn)
    if not res['OK']:
      return res
    elif lfn in res['Value']['Failed'].keys():
      return S_ERROR(res['Value']['Failed'][lfn])
    else:
      return S_OK()

  def createDataset(self):
    if not self.handle:
      return S_ERROR("No handle defined")
    if not self.lfns:
      return S_ERROR("No LFNs defined")
    lfcDir = "%s%s" % (self.lfcPath,self.handle)
    res = self.lfc.createDataset(lfcDir,self.lfns)
    if not res['OK']:
      return res
    elif not len(res['Value']['Failed'].keys()) == 0:
      self.removeDataset()
      return S_ERROR("Failed to create dataset")
    else:
      return S_OK()

  def removeDataset(self):
    if not self.handle:
      return S_ERROR("No handle defined")
    lfcDir = "%s%s" % (self.lfcPath,self.handle)
    res = self.lfc.removeDataset(lfcDir)
    if not res['OK']:
      return res
    elif not len(res['Value']['Failed'].keys()) == 0:
      return S_ERROR("Failed to remove dataset")
    else:
      return S_OK()

  def __retriveDataset(self):
    if not self.handle:
      return S_ERROR("No handle defined")
    lfcDir = "%s%s" % (self.lfcPath,self.handle)
    res = self.lfc.resolveDataset(lfcDir)
    if not res['OK']:
      return res
    self.lfns = res['Value'].keys()
    self.replicas = res['Value']
    return S_OK()
