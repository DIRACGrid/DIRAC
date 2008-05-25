""" DISET request handler base class for the DatasetDB.
"""
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.DatasetDB import DatasetDB
from DIRAC.DataManagementSystem.Client.Dataset import Dataset
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog

# This is a global instance of the DataLoggingDB class
datasetDB = False

def initializeDatasetHandler(serviceInfo):
  global datasetDB
  datasetDB = DatasetDB()
  return S_OK()

class DatasetHandler(RequestHandler):

  types_publishDataset = [StringType,ListType]
  def export_publishDatset(self,handle,lfns,metadataTags):
    """ This method will create the data set in the LFC the datasetDB
    """
    oDataset = Dataset()
    oDataset.setHandle(handle)
    oDataset.setLfns(lfns)
    res = oDataset.createLinks()
    if not res['OK']:
      oDataset.removeDataset()
      return res
    else:
      res = datasetDB.publishDataset(handle,metadataTags)
      if not res['OK']:
        oDataset.removeDataset()
      else:
        authorDN = self._clientTransport.peerCredentials['DN']
        message = 'Created'
        res = datasetDB.updateDatasetLogging(handle,message,authorDN)
    return res

  types_deleteDataset = [StringType]
  def export_deleteDatset(self,handle):
    """ This method will remove the supplied dataset from the catalog and the LFC
    """
    res = datasetDB.removeDataset(handle)
    if not res['OK']:
      return res
    else:
      authorDN = self._clientTransport.peerCredentials['DN']
      message = 'Deleted'
      res = datasetDB.updateDatasetLogging(handle,message,authorDN)
      if not res['OK']:
        return res
      else:
        oDataset = Dataset()
        oDataset.setHandle(handle)
        res = oDataset.removeDataset()
    return res

  types_removeFileFromDataset = [StringType,StringType]
  def export_removeFileFromDataset(self,handle,lfn):
    """ This method will remove the supplied file from the dataset
    """
    oDataset = Dataset()
    oDataset.setHandle(handle)
    res = oDataset.removeFile(lfn)
    if res['OK']:
      authorDN = self._clientTransport.peerCredentials['DN']
      message = 'Removed %s' % lfn
      res = datasetDB.updateDatasetLogging(handle,message,authorDN)
    return res

  types_getAllDatasets = []
  def export_getAllDatasets(self):
    """ Get all the dataset handles
    """
    return datasetDB.getAllDatasets()

  types_getDatasetHistory = [StringType]
  def export_getDatasetHistory(self,handle):
    """ Get the history for a single dataset
    """
    return datasetDB.getDatasetHistory(handle)
