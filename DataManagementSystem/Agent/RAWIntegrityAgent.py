"""  RAWIntegrityAgent determines whether RAW files in Castor were migrated correctly
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.RequestManagementSystem.Client.Request import RequestClient
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.Catalog import LcgFileCatalogCombinedClient
import time
from types import *

AGENT_NAME = 'DataManagement/RAWIntegrityAgent'

class RAWIntegrityAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    # need to get the online requestDB URL.'http://lbora01.cern.ch:9135'
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    self.RAWIntegrityDB = RAWIntegrityDB()
    return result

  def execute(self):

    ############################################################
    #
    # Obtain the files which have not yet been migrated
    #
    gLogger.info("RAWIntegrityAgent.execute: Obtaining un-migrated files.")
    res = self.RAWIntegrityDB.getActiveFiles()
    if not res['OK']:
      errStr = "RAWIntegrityAgent.execute: Failed to obtain un-migrated files."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    unMigratedFiles = res['Value']
    gLogger.info("RAWIntegrityAgent.execute: Obtained %s un-migrated files." % len(unMigratedFiles))
    if not len(unMigratedFiles) > 0:
      return S_OK()

    ############################################################
    #
    # Obtain the physical file metadata for the file awating migration
    #
    gLogger.info("RAWIntegrityAgent.execute: Obtaining physical file metadata.")
    res = self.ReplicaManager.getPhysicalFileMetadata(physicalFiles,'CERN-RAW')
    if not res['OK']:
      errStr = "RAWIntegrityAgent.execute: Failed to obtain physical file metadata."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    if len(res['Value']['Failed']) > 0:
      gLogger.info("RAWIntegrityAgent.execute: Failed to obtain physical file metadata for %s files." % len(res['Value']['Failed']))
    gLogger.info("RAWIntegrityAgent.execute: Obtained physical file metadata for %s files." % len(res['Value']['Successful']))

    ############################################################
    #
    # Determine the files that have been newly migrated
    #
    migratedFiles = []
    pfnMetadata = res['Value']['Successful']
    for pfn,metadataDict in pfnMetadata.items():
      if metadataDict['Migrated']:
        gLogger.info("RAWIntegrityAgent.execute: %s is newly migrated." % pfn)
        migratedFiles.append(pfn)
    gLogger.info("RAWIntegrityAgent.execute: Found %s newly migrated files." % len(migratedFiles))
    if not len(migratedFiles) > 0:
      return S_OK()

    ############################################################
    #
    # Obtain the file catalogue checksum information for the newly migrated files
    #
    gLogger.info("RAWIntegrityAgent.execute: Obtaining catalogue checksum information for %s files." % len(migratedFiles))
    res = self.RAWIntegrityDB.getFileMetadata(lfns)
    if not res['OK']:
      errStr = "RAWIntegrityAgent.execute: Failed to obtain checksum information."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    if len(res['Value']['Failed']) > 0:
      gLogger.info("RAWIntegrityAgent.execute: Failed to obtain checksum information for %s files." % len(res['Value']['Failed']))
    gLogger.info("RAWIntegrityAgent.execute: Obtained checksum information for %s files." % len(res['Value']['Successful']))

    ############################################################
    #
    # Determine the files that were migrated (in)correctly
    #
    filesToRemove = []
    filesToTransfer = []
    fileMetadata = res['Value']['Successful']
    for lfn,metadata in fileMetadata.items():
      if metadata['Checksum'] == pfnMetadata[lfn]['Checksum']:
        gLogger.info("RAWIntegrityAgent.execute: %s migrated checksum match." % lfn)
        filesToRemove.append(lfn)
      else:
        gLogger.error("RAWIntegrityAgent.execute: Migrated checksum mis-match.",lfn)
        filesToTransfer.append(lfn)
    gLogger.info("RAWIntegrityAgent.execute: Found %s checksum matches." % len(filesToRemove))
    gLogger.info("RAWIntegrityAgent.execute: Found %s checksum mis-matches." % len(filesToTransfer))

    if len(filesToRemove) > 0:
      ############################################################
      #
      # Register the correctly migrated files to the file catalogue
      #
      gLogger.info("RAWIntegrityAgent.execute: Registering correctly migrated files to the File Catalog.")
      res = self.RAWIntegrityDB.getFileMetadata(filesToRemove)
      if not res['OK']:
        gLogger.error("RAWIntegrityAgent.execute: Failed to get file metadata from RAW integrity database.", res['Message'])
      else:
        for lfn,pfn,size,se,guid in res['Value']:
          fileTuple = (lfn,pfn,size,se,guid)
          res = self.ReplicaManager.registerFile(fileTuple)
          if not res['OK']:
            gLogger.error("RAWIntegrityAgent.execute: Completely failed to register successfully migrated file.", res['Message'])
          elif not res['Value']['Successful'].has_key(lfn):
            gLogger.error("RAWIntegrityAgent.execute: Failed to register lfn in the File Catalog.", res['Value']['Failed'][lfn])
          else:
            gLogger.info("RAWIntegrityAgent.execute: Successfully registered %s in the File Catalog.", lfn)
            ############################################################
            #
            # Create a removal request and set it to the gateway request DB
            #
            gLogger.info("RAWIntegrityAgent.execute: Creating removal request for correctly migrated files.")
            oRequest = DataManagementRequest()
            subRequestIndex = oRequest.initiateSubRequest('removal')['Value']
            attributeDict = {'Operation':'physicalRemoval','TargetSE':'OnlineRunDB'}
            oRequest.setSubRequestAttributes(subRequestIndex,'removal',attributeDict)
            filesDict = [{'LFN':lfn,'PFN':pfn}]
            oRequest.setSubRequestFiles(subRequestIndex,'removal',filesDict)
            fileName = os.path.basename(lfn)
            requestName = 'remove_%s.xml' % fileName
            requestString = oRequest.toXML()['Value']
            gLogger.info("RAWIntegrityAgent.execute: Attempting to put %s to gateway requestDB." %  requestName)
            res = self.RequestDBClient.setRequest(requestName,requestString)
            if not res['OK']:
              gLogger.error("RAWIntegrityAgent.execute: Failed to set removal request to gateway requestDB.", res['Message'])
            else:
              gLogger.info("RAWIntegrityAgent.execute: Successfully put %s to gateway requestDB." %  requestName)
              ############################################################
              #
              # Remove the file from the list of files awaiting migration in database
              #
              gLogger.info("RAWIntegrityAgent.execute: Updating status of %s in raw integrity database." %  lfn)
              res = self.RAWIntegrityDB.setMigrationComplete(lfn)
              if not res['OK']:
                gLogger.error("RAWIntegrityAgent.execute: Failed to update status in raw integrity database.", res['Message'])
              else:
                gLogger.info("RAWIntegrityAgent.execute: Successfully updated status in raw integrity database.")


    if len(filesToTransfer) > 0:
      ############################################################
      #
      # Remove the incorrectly migrated files from the storage element (will be over written anyways)
      #
      gLogger.info("RAWIntegrityAgent.execute: Removing incorrectly migrated files from Storage Element.")
      res = self.RAWIntegrityDB.getFileMetadata(filesToTransfer)
      if not res['OK']:
        gLogger.error("RAWIntegrityAgent.execute: Failed to get file metadata from RAW integrity database.", res['Message'])
      else:
        for lfn,pfn,size,se,guid in res['Value']:
          res = self.ReplicaManager.removePhysicalFile(se,pfn)
          if not res['OK']:
            gLogger.error("RAWIntegrityAgent.execute: Completely failed to remove pfn from the storage element.", res['Message'])
          elif not res['Value']['Successful'].has_key(pfn):
            gLogger.error("RAWIntegrityAgent.execute: Failed to remove pfn from the storage element.", res['Value']['Failed'][pfn])
          else:
            gLogger.info("RAWIntegrityAgent.execute: Successfully removed pfn from the storage element.")
            ############################################################
            #
            # Create a transfer request for the files incorrectly migrated
            #
            gLogger.info("RAWIntegrityAgent.execute: Creating (re)transfer request for incorrectly migrated files.")
            oRequest = DataManagementRequest()
            subRequestIndex = oRequest.initiateSubRequest('transfer')['Value']
            attributeDict = {'Operation':'putAndRegister','TargetSE':se}
            oRequest.setSubRequestAttributes(subRequestIndex,'transfer',attributeDict)
            fileName = os.path.basename(lfn)
            filesDict = [{'LFN':lfn,'PFN':fileName,'GUID':guid}]
            oRequest.setSubRequestFiles(subRequestIndex,'transfer',filesDict)
            requestName = 'retransfer_%s.xml' % fileName
            requestString = oRequest.toXML()['Value']
            gLogger.info("RAWIntegrityAgent.execute: Attempting to put %s to gateway requestDB." %  requestName)
            res = self.RequestDBClient.setRequest(requestName,requestString)
            if not res['OK']:
              gLogger.error("RAWIntegrityAgent.execute: Failed to set removal request to gateway requestDB.", res['Message'])
            else:
              gLogger.info("RAWIntegrityAgent.execute: Successfully put %s to gateway requestDB." %  requestName)
              ############################################################
              #
              # Remove the file from the list of files awaiting migration in database
              #
              gLogger.info("RAWIntegrityAgent.execute: Updating status of %s in raw integrity database." %  lfn)
              res = self.RAWIntegrityDB.setMigrationComplete(lfn)
              if not res['OK']:
                gLogger.error("RAWIntegrityAgent.execute: Failed to update status in raw integrity database.", res['Message'])
              else:
                gLogger.info("RAWIntegrityAgent.execute: Successfully updated status in raw integrity database.")

    return S_OK()




