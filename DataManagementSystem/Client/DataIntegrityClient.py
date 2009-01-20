""" This is the Data Integrity Client which allows the simple reporting of problematic file and replicas to the IntegrityDB and their status correctly updated in the FileCatalog.""" 

__RCSID__ = "$Id: DataIntegrityClient.py,v 1.2 2009/01/20 17:41:02 acsmith Exp $"

import re, time, commands, random,os
import types

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.Core.DISET.RPCClient import RPCClient

class DataIntegrityClient:

  def __init__(self):
    """ Constructor function.
    """
    pass

  def setFileProblematic(self,lfn,reason,sourceComponent=''):
    """ This method updates the status of the file in the FileCatalog and the IntegrityDB
        The supplied fileTuple should be of the form (lfn,prognosis)
        
        lfn - the lfn of the file
        prognosis - this is given to the integrity DB and should reflect the problem observed with the file
        
        sourceComponent is the component issuing the request.
    """  
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "DataIntegrityClient.setFileProblematic: Supplied file info must be list or a single LFN."
      gLogger.error(errStr) 
      return S_ERROR(errStr)

    gLogger.info("DataIntegrityClient.setFileProblematic: Attempting to update %s files." % len(lfns))
    statusTuples = []
    successful = {}
    failed = {}
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    fileCatalog = FileCatalog()
    for lfn in lfns:
      fileMetadata = {'Prognosis':reason,'LFN':lfn,'PFN':'','StorageElement':''}
      res = integrityDB.insertProblematic(sourceComponent,fileMetadata)
      if res['OK']:
        statusTuples.append((lfn,'Problematic'))
      else:
        failed[lfn] = res['Message']
    res = fileCatalog.setFileStatus(statusTuples)
    if not res['OK']:
      errStr = "DataIntegrityClient.setFileProblematic: Completely failed to update files."
      gLogger.error(errStr,res['Message'])
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def setReplicaProblematic(self,replicaTuple,sourceComponent=''):
    """ This method updates the status of the replica in the FileCatalog and the IntegrityDB
        The supplied replicaTuple should be of the form (lfn,pfn,se,prognosis)

        lfn - the lfn of the file
        pfn - the pfn if available (otherwise '')
        se - the storage element of the problematic replica (otherwise '')
        prognosis - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
    """
    if type(replicaTuple) == types.ListType:
      replicaTuples = replicaTuple
    elif type(replicaTuple) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "DataIntegrityClient.setReplicaProblematic: Supplied replica info must be tuple of list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)

    gLogger.info("DataIntegrityClient.registerReplica: Attempting to update %s replicas." % len(replicaTuples))
    statusTuples = []
    successful = {}
    failed = {}
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    fileCatalog = FileCatalog()
    for lfn,pfn,se,reason in replicaTuples:
      fileMetadata = {'Prognosis':reason,'LFN':lfn,'PFN':pfn,'StorageElement':se}
      res = integrityDB.insertProblematic(sourceComponent,fileMetadata)
      if res['OK']:
        statusTuples.append((lfn,pfn,se,'Problematic'))
      else:
        failed[lfn] = res['Message']
    res = self.fileCatalog.setReplicaStatus(statusTuples)
    if not res['OK']:
      errStr = "DataIntegrityClient.setReplicaProblematic: Completely failed to update replicas."
      gLogger.error(errStr,res['Message'])
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)
