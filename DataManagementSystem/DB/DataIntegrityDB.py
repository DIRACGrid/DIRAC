""" DataIntegrityDB class is a front-end to the Data Integrity Database.
"""

import re, os, sys
import time, datetime
from types import *

from DIRAC import gConfig,gLogger,S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

#############################################################################
class DataIntegrityDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """
    DB.__init__(self,'DataIntegrityDB','DataManagement/DataIntegrityDB',maxQueueSize)

#############################################################################
  def insertProblematic(self,source,fileMetadata):
    """ Insert the supplied file metadata into the problematics table
    """
    prognosis = fileMetadata['Prognosis']
    lfn = fileMetadata['LFN']
    pfn = fileMetadata['PFN']
    storageElement = fileMetadata['StorageElement']
    res = self.__problematicExists(prognosis, lfn, pfn, storageElement)
    if not res['OK']:
      return res
    if res['Value']:
      # Entry already exists for this problematic
      return S_OK()
    req = self.__buildInsertReq(source,fileMetadata)
    res = self._update(req)
    return res

  def __problematicExists(self,prognosis,lfn,pfn,storageElement):
    """  Determine whether the file already exists in the problematics table.
    """
    req = "SELECT FileID FROM Problematics WHERE Prognosis ='%s' AND LFN = '%s' AND PFN = '%s' AND StorageElement = '%s';" % (prognosis,lfn,pfn,storageElement)
    err = "DataIntegrityDB.__problematicExists: Failed to determine whether problematic exists."
    res = self._query(req)
    if not res['OK']:
      return S_ERROR(err,res['Message'])
    if res['Value']:
      return S_OK(True)
    else:
      return S_OK(False)

  def __buildInsertReq(self,source,fileMetadata):
    fields = "(Source,InsertDate"
    values = "('%s',NOW()" % source
    for attrName,attrVal in fileMetadata.items():
      fields = "%s,%s" % (fields,attrName)
      values = "%s,'%s'" % (values,attrVal)
    fields = "%s)" % fields
    values = "%s)" % values
    req = "INSERT INTO Problematics %s VALUES %s;" % (fields,values)
    return req

#############################################################################
  def setProblematicStatus(self,fileID,status):
    req = "UPDATE Problematics SET Status= '%s' WHERE FileID = %s;" % (status,fileID)
    res = self._update(req)
    return res

  def incrementProblematicRetry(self,fileID):
    req = "UPDATE Problematics SET Retries=Retries+1 WHERE FileID = %s;" % (status,fileID)
    res = self._update(req)
    return res

#############################################################################
  def getProblematicsSummary(self):
    """ Get a summary of the current problematics table
    """
    req = "SELECT DISTINCT Prognosis from Problematics;"
    res = self._query(req)
    if not res['OK']:
      return res
    if not res['Value'][0]:
      return S_OK()
    resDict = {}
    for prognosis in res['Value'][0]:
      resDict[prognosis] = {}
      req = "SELECT Status,COUNT(Status) FROM Problematics where Prognosis = '%s' GROUP BY Status;" % prognosis
      res = self._query(req)
      if not res['OK']:
        return res
      for status,count in res['Value']:
        resDict[prognosis][status] = int(count)
    return S_OK(resDict)

#############################################################################
  def getDistinctPrognosis(self):
    """ Get a list of all the current problematic types
    """
    req = "SELECT DISTINCT Prognosis from Problematics;"
    res = self._query(req)
    if not res['OK']:
      return res
    if not res['Value'][0]:
      return S_OK()
    prognosisList = []
    for prognosis in res['Value'][0]:
      prognosisList.append(prognosis)
    return S_OK(prognosisList)

#############################################################################
  def getPrognosisProblematics(self,prognosis):
    """ Get all the active files with the given problematic
    """
    req = "SELECT FileID,LFN,PFN,StorageElement from Problematics WHERE Prognosis = '%s' AND Status = 'New' ORDER BY Retries;" % prognosis
    res = self._query(req)
    if not res['OK']:
      return res
    problematics = []
    for fileID,lfn,pfn,storageElement in res['Value']:
      problematics.append((fileID,lfn,pfn,storageElement))
    return S_OK(problematics)
