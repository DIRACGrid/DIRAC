# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/RequestPreparation.py,v 1.2 2009/06/16 22:01:56 acsmith Exp $

"""  TransferAgent takes transfer requests from the RequestDB and replicates them
"""

__RCSID__ = "$Id: RequestPreparation.py,v 1.2 2009/06/16 22:01:56 acsmith Exp $"


from DIRAC.Core.DISET.RPCClient import RPCClient
import os,sys,re
 
from DIRAC import gLogger,S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.DISET.RPCClient import RPCClient

import time,os
from types import *


AGENT_NAME = 'Stager/RequestPreparation'

class RequestPreparation(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.fileCatalog = FileCatalog()
    self.stagerClient = RPCClient('Stager/Stager')
    self.dataIntegrityClient = DataIntegrityClient()
    return S_OK()

  def execute(self):
    res = self.prepareNewFiles()
    return res

  def prepareNewFiles(self): 
    """ This is the first logical task to be executed and manages the New->Waiting transition
    """
    res = self.__getNewFiles()
    if not res['OK']:
      gLogger.fatal("RequestPreparation.prepareNewFiles: Failed to get file from StagerDB.",res['Message'])
      return res
    lfnToFileID = res['Value']['Files']
    lfnToSE = res['Value']['FileSEs']
    res = self.__getFileMetadata(lfnToFileID.keys())
    failed = res['Value']['Failed']
    terminal = res['Value']['Terminal']
    fileMetadata = res['Value']['Successful']
    for lfn,errMessage in failed.items():
      gLogger.error("RequestPreparation.prepareNewFiles: Failed to get file metadata.", "%s %s" % (lfn,errMessage))

    # For all the terminal files get all the tasks associated to these files and fail them.
    if terminal:
      #First set the failure reason of the terminal files 
      for lfn,reason in terminal.items():
        pass
      res = self.__getAssociatedTaskFiles(terminal)

    #res = self.__clearFailedFileTasks()
    

    notAtRequestedSE = {}
    selectedFiles = []
    for lfn,metadata in fileMetadata.items():
      size = metadata['Size']
      replicas = metadata['Replicas']
      for storageElement in lfnToSE[lfn].keys():
        if not replicas.has_key(storageElement):
          gLogger.error("RequestPreparation.: File does not have replica at requested storage element","%s %s" % (lfn,storageElement))
          notAtRequestedSE[lfn][storageElement]
        else:
          pfn = replicas[storageElement]
          selectedFiles.append((lfn,storageElement,pfn,size))
    print selectedFiles



    """
    res = self.stagerClient.updateFileInformation(tuples)
    if not res['OK']:
      print res['Message']
    """
    return S_OK()

  def __getAssociatedTaskFiles(self,fileIDs):
    """ This obtains all the FileIDs for the tasks associated with the supplied fileIDs """
    # First get the list of task IDs associated to the files that failed
    res = self.stagerClient.getTasksForFileIDs(fileIDs)
    if not res['OK']:
      gLogger.error("RequestPreparation.__getAssociatedTaskFiles: Failed to get task IDs associated to the fileIDs.",res['Message'])
      return res
    taskIDs = res['Value']
    failedTasks = []
    for fileID,tasks in taskIDs.items():
      for task in tasks:
        if not task in failedTasks:
          failedTasks.append(task)
    # Then get all the files associated to those tasks
    res = self.stagerClient.getFileIDsForTasks(failedTasks)
    if not res['OK']:
      gLogger.error("RequestPreparation.__getAssociatedTaskFiles: Failed to get file IDs associated to the tasks.",res['Message'])
      return res
    fileIDs = res['Value']
    for taskID,files in fileIDs.items():
      for file in files:
        if not file in allFileIDs:
          allFileIDs.append(file)
    
  def __clearFailedFileTasks(self,failedFileIDs):
    """ This obtains the TaskIDs associated to the fileIDs at the supplied storage element """
    # First get the list of task IDs associated to the files that failed
    res = self.stagerClient.getTasksForFileIDs(failedFileIDs)
    if not res['OK']:
      gLogger.error("RequestPreparation.__clearFailedFileTasks: Failed to get task IDs associated to the fileIDs.",res['Message'])
      return res
    taskIDs = res['Value']
    failedTasks = []
    for fileID,tasks in taskIDs.items():
      for task in tasks:
        if not task in failedTasks:
          failedTasks.append(task)
    # Then get all the files associated to those tasks
    res = self.stagerClient.getFileIDsForTasks(failedTasks)
    if not res['OK']:
      gLogger.error("RequestPreparation.__clearFailedFileTasks: Failed to get file IDs associated to the tasks.",res['Message'])
      return res
    fileIDs = res['Value']
    allFileIDs = []
    for taskID,files in fileIDs.items():
      for file in files:
        if not file in allFileIDs:
          allFileIDs.append(file)
    goodFileIDs = []
    for fileID in allFileIDs: 
      if not fileID in failedFileIDs:
        goodFileIDs.append(fileID)

  def __reportProblematicFiles(self,lfns,reason):
    return S_OK()
    res = self.dataIntegrityClient.setFileProblematic(lfns,reason,self.name)
    if not res['OK']:
      gLogger.error("RequestPreparation.__reportProblematicFiles: Failed to report missing files.",res['Message'])
      return res
    if res['Value']['Successful']:
      gLogger.info("RequestPreparation.__reportProblematicFiles: Successfully reported %s missing files." % len(res['Value']['Successful']))
    if res['Value']['Failed']:
      gLogger.info("RequestPreparation.__reportProblematicFiles: Failed to report %s problematic files." % len(res['Value']['Failed']))
    return res


  #####################################################################
  #
  # These are the methods for preparing the file metadata
  #

  def __getNewFiles(self):
    """ This obtains the New files from the Files table and for each LFN the requested storage element """
    # First obtain the New files from the Files table
    res = self.stagerClient.getFilesWithStatus('New')
    if not res['OK']:
      gLogger.error("RequestPreparation.__getNewFiles: Failed to get files with New Status.", res['Message'])
      return res
    if not res['Value']:
      gLogger.debug("RequestPreparation.__getNewFiles: No New files found to process.")
      return S_OK()
    else:
     gLogger.debug("RequestPreparation.__getNewFiles: Obtained %s New file(s) to process." % len(res['Value']))
    fileSEs = {}
    files = {}
    for fileID,info in res['Value'].items():
      lfn,storageElement,size,pfn = info
      files[lfn] = fileID
      if not fileSEs.has_key(lfn):
        fileSEs[lfn] = {}
      fileSEs[lfn][storageElement] = fileID
    return S_OK({'Files':files,'FileSEs':fileSEs})

  def __getFileMetadata(self,lfns):
    """ This method obtains the file metadata and replica information for the supplied LFNs """
    # First check that the files exist in the FileCatalog
    res = self.__getExistingFiles(lfns)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    terminal = res['Value']['Missing']
    exist = res['Value']['Exist']
    if not exist:
      gLogger.error('RequestPreparation.__getFileMetadata: Failed determine existance of any files')
      resDict = {'Successful':{},'Failed':failed,'Terminal':terminal}
      return S_OK(resDict)
    # Then obtain the file sizes from the FileCatalog
    res = self.__getFileSize(exist)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    terminal.update(res['Value']['ZeroSize'])
    fileSizes = res['Value']['FileSizes']
    if not fileSizes:
      gLogger.error('RequestPreparation.__getFileMetadata: Failed determine sizes of any files')
      resDict = {'Successful':{},'Failed':failed,'Terminal':terminal}
      return S_OK(resDict)
    # Finally obtain the replicas from the FileCatalog
    res = self.__getFileReplicas(fileSizes.keys())
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    terminal.update(res['Value']['ZeroReplicas'])
    replicas = res['Value']['Replicas']
    if not replicas:
      gLogger.error('RequestPreparation.__getFileMetadata: Failed determine replicas for any files')
      resDict = {'Successful':{},'Failed':failed,'Terminal':terminal}
      return S_OK(resDict)
    fileMetadata = {}
    for lfn,replicas in replicas.items():
      fileMetadata[lfn] = {}
      fileMetadata[lfn]['Replicas'] = replicas
      fileMetadata[lfn]['Size'] = fileSizes[lfn]
    return S_OK({'Successful':fileMetadata,'Failed':failed,'Terminal':terminal})

  def __getExistingFiles(self,lfns):
    """ This checks that the files exist in the FileCatalog. """  
    filesExist = []
    missing = {}
    res = self.fileCatalog.exists(lfns)
    if not res['OK']:
      gLogger.error("RequestPreparation.__getExistingFiles: Failed to determine whether files exist.",res['Message'])
      return res
    failed = res['Value']['Failed']
    for lfn,exists in res['Value']['Successful'].items():
      if exists:
        filesExist.append(lfn)
      else:
        missing[lfn] = 'LFN not registered in the FileCatalog'
    if missing:
      for lfn,reason in missing.items():
        gLogger.warn("RequestPreparation.__getExistingFiles: %s" % reason,lfn)
      self.__reportProblematicFiles(missing.keys(),'LFN-LFC-DoesntExist')
    return S_OK({'Exist':filesExist,'Missing':missing,'Failed':failed})

  def __getFileSize(self,lfns):
    """ This obtains the file size from the FileCatalog. """
    failed = []
    fileSizes = {}
    zeroSize = {}
    res = self.fileCatalog.getFileSize(lfns)
    if not res['OK']:
      gLogger.error("RequestPreparation.__getFileSize: Failed to get sizes for files.", res['Message'])
      return res
    failed = res['Value']['Failed']
    for lfn,size in res['Value']['Successful'].items():
      if size == 0:
        zeroSize[lfn] = "LFN registered with zero size in the FileCatalog"
      else:
        fileSizes[lfn] = size
    if zeroSize:
      for lfn,reason in zeroSize.items():
        gLogger.warn("RequestPreparation.__getFileSize: %s" % reason,lfn)
      self.__reportProblematicFiles(zeroSize.keys(),'LFN-LFC-ZeroSize')
    return S_OK({'FileSizes':fileSizes,'ZeroSize':zeroSize,'Failed':failed})  

  def __getFileReplicas(self,lfns):
    """ This obtains the replicas from the FileCatalog. """
    replicas = {}
    noReplicas = {}
    res = self.fileCatalog.getReplicas(lfns)
    if not res['OK']:
      gLogger.error("RequestPreparation.__getFileReplicas: Failed to obtain file replicas.",res['Message'])
      return res
    failed = res['Value']['Failed']
    for lfn,lfnReplicas in res['Value']['Successful'].items():
      if len(lfnReplicas.keys()) == 0:
        noReplicas[lfn] = "LFN registered with zero replicas in the FileCatalog"         
      else:
        replicas[lfn] = lfnReplicas
    if noReplicas:
      for lfn,reason in noReplicas.items():
        gLogger.warn("RequestPreparation.__getFileReplicas: %s" % reason,lfn)
      self.__reportProblematicFiles(noReplicas.keys(),'LFN-LFC-NoReplicas')
    return S_OK({'Replicas':replicas,'ZeroReplicas':noReplicas,'Failed':failed})
