"""  RAWIntegrityAgent determines whether RAW files in Castor were migrated correctly
"""
from DIRAC  import gLogger, gConfig, gMonitor,S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.DB.RAWIntegrityDB import RAWIntegrityDB
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

import time,os
from types import *

AGENT_NAME = 'DataManagement/RAWIntegrityAgent'

class RAWIntegrityAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    self.RAWIntegrityDB = RAWIntegrityDB()
    self.DataLog = DataLoggingClient()

    self.proxyLocation = gConfig.getValue( self.section+'/ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    self.gatewayUrl = PathFinder.getServiceURL( 'RequestManagement/onlineGateway')

    gMonitor.registerActivity("Iteration","Agent Loops/min","RAWIntegriryAgent", "Loops", gMonitor.OP_SUM)
    gMonitor.registerActivity("WaitingFiles","Files waiting for migration","RAWIntegriryAgent","Files",gMonitor.OP_MEAN)
    gMonitor.registerActivity("NewlyMigrated","Newly migrated files","RAWIntegriryAgent","Files", gMonitor.OP_SUM)
    gMonitor.registerActivity("TotMigrated","Total migrated files","RAWIntegriryAgent","Files", gMonitor.OP_ACUM)
    gMonitor.registerActivity("SuccessfullyMigrated","Successfully migrated files","RAWIntegriryAgent","Files", gMonitor.OP_SUM)
    gMonitor.registerActivity("TotSucMigrated","Total successfully migrated files","RAWIntegriryAgent","Files", gMonitor.OP_ACUM)
    gMonitor.registerActivity("FailedMigrated","Erroneously migrated files","RAWIntegriryAgent","Files", gMonitor.OP_SUM)
    gMonitor.registerActivity("TotFailMigrated","Total erroneously migrated files","RAWIntegriryAgent","Files", gMonitor.OP_ACUM)
    gMonitor.registerActivity("MigrationTime","Average migration time","RAWIntegriryAgent","Seconds",gMonitor.OP_MEAN)

    gMonitor.registerActivity("TotMigratedSize","Total migrated file size","RAWIntegriryAgent","GB",gMonitor.OP_ACUM)
    gMonitor.registerActivity("TimeInQueue","Average current wait for migration","RAWIntegriryAgent","Minutes",gMonitor.OP_MEAN)
    gMonitor.registerActivity("WaitSize","Size of migration buffer","RAWIntegriryAgent","GB",gMonitor.OP_MEAN)
    gMonitor.registerActivity("MigrationRate","Observed migration rate","RAWIntegriryAgent","MB/s",gMonitor.OP_MEAN)

    return result


  def execute(self):
    gMonitor.addMark("Iteration",1)

    result = setupShifterProxyInEnv( "DataManager", self.proxyLocation )
    if not result[ 'OK' ]:
      self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
      return result


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
    activeFiles = res['Value']

    gMonitor.addMark("WaitingFiles",len(activeFiles.keys()))
    gLogger.info("RAWIntegrityAgent.execute: Obtained %s un-migrated files." % len(activeFiles.keys()))
    if not len(activeFiles.keys()) > 0:
      return S_OK()
    totalSize = 0
    for lfn,fileDict in activeFiles.items():
      totalSize += int(fileDict['Size'])
      gMonitor.addMark("TimeInQueue", (fileDict['WaitTime']/60))
    gMonitor.addMark("WaitSize",(totalSize/(1024*1024*1024.0)))

    ############################################################
    #
    # Obtain the physical file metadata for the files awating migration
    #
    gLogger.info("RAWIntegrityAgent.execute: Obtaining physical file metadata.")
    sePfns = {}
    pfnDict = {}
    for lfn,metadataDict in activeFiles.items():
      pfn = metadataDict['PFN']
      pfnDict[pfn] = lfn
      se = metadataDict['SE']
      if not sePfns.has_key(se):
        sePfns[se] = []
      sePfns[se].append(pfn)
    pfnMetadata = {'Successful':{},'Failed':{}}
    for se,pfnList in sePfns.items():
      res = self.ReplicaManager.getPhysicalFileMetadata(pfnList,se)
      if not res['OK']:
        errStr = "RAWIntegrityAgent.execute: Failed to obtain physical file metadata."
        gLogger.error(errStr,res['Message'])
        for pfn in pfnList:
          pfnMetadata['Failed'][pfn] = errStr
      else:
        pfnMetadata['Successful'].update(res['Value']['Successful'])
        pfnMetadata['Failed'].update(res['Value']['Failed'])
    if len(pfnMetadata['Failed']) > 0:
      gLogger.info("RAWIntegrityAgent.execute: Failed to obtain physical file metadata for %s files." % len(pfnMetadata['Failed']))
    gLogger.info("RAWIntegrityAgent.execute: Obtained physical file metadata for %s files." % len(pfnMetadata['Successful']))
    if not len(pfnMetadata['Successful']) > 0:
      return S_OK()

    ############################################################
    #
    # Determine the files that have been newly migrated and their success
    #
    filesToRemove = []
    filesToTransfer = []
    filesMigrated = []
    for pfn,pfnMetadataDict in pfnMetadata['Successful'].items():
      if pfnMetadataDict['Migrated']:
        lfn = pfnDict[pfn]
        filesMigrated.append(lfn)
        gLogger.info("RAWIntegrityAgent.execute: %s is newly migrated." % lfn)
        if not pfnMetadataDict.has_key('Checksum'):
          gLogger.error("RAWIntegrityAgent.execute: No checksum information available.",lfn)
          comm = "nsls -lT --checksum /castor/%s" % pfn.split('/castor/')[-1]
          res = shellCall(180,comm)
          returnCode,stdOut,stdErr = res['Value']
          if not returnCode:
            pfnMetadataDict['Checksum'] = stdOut.split()[9]
          else:
            pfnMetadataDict['Checksum'] = 'Not available'
        castorChecksum = pfnMetadataDict['Checksum']
        onlineChecksum = activeFiles[lfn]['Checksum']
        if castorChecksum.lower().lstrip('0') == onlineChecksum.lower().lstrip('0').lstrip('x'):
          gLogger.info("RAWIntegrityAgent.execute: %s migrated checksum match." % lfn)
          self.DataLog.addFileRecord(lfn,'Checksum match',castorChecksum,'','RAWIntegrityAgent')
          filesToRemove.append(lfn)
          activeFiles[lfn]['Checksum'] = castorChecksum
        elif pfnMetadataDict['Checksum'] == 'Not available':
          gLogger.info("RAWIntegrityAgent.execute: Unable to determine checksum.", lfn)
        else:
          gLogger.error("RAWIntegrityAgent.execute: Migrated checksum mis-match.","%s %s %s" % (lfn,castorChecksum.lstrip('0'),onlineChecksum.lstrip('0').lstrip('x')))
          self.DataLog.addFileRecord(lfn,'Checksum mismatch','%s %s' % (castorChecksum.lower().lstrip('0'),onlineChecksum.lower().lstrip('0')),'','RAWIntegrityAgent')
          filesToTransfer.append(lfn)

    migratedSize = 0
    for lfn in filesMigrated:
      migratedSize += int(activeFiles[lfn]['Size'])
    res = self.RAWIntegrityDB.getLastMonitorTimeDiff()
    if res['OK']:
      timeSinceLastMonitor = res['Value']
      migratedSizeMB = migratedSize/(1024*1024.0)
      gMonitor.addMark("MigrationRate",migratedSizeMB/timeSinceLastMonitor)
    res = self.RAWIntegrityDB.setLastMonitorTime()
    migratedSizeGB = migratedSize/(1024*1024*1024.0)
    gMonitor.addMark("TotMigratedSize",migratedSizeGB)
    gMonitor.addMark("NewlyMigrated",len(filesMigrated))
    gMonitor.addMark("TotMigrated",len(filesMigrated))
    gMonitor.addMark("SuccessfullyMigrated",len(filesToRemove))
    gMonitor.addMark("TotSucMigrated",len(filesToRemove))
    gMonitor.addMark("FailedMigrated",len(filesToTransfer))
    gMonitor.addMark("TotFailMigrated",len(filesToTransfer))
    gLogger.info("RAWIntegrityAgent.execute: %s files newly migrated." % len(filesMigrated))
    gLogger.info("RAWIntegrityAgent.execute: Found %s checksum matches." % len(filesToRemove))
    gLogger.info("RAWIntegrityAgent.execute: Found %s checksum mis-matches." % len(filesToTransfer))


    if len(filesToRemove) > 0:
      ############################################################
      #
      # Register the correctly migrated files to the file catalogue
      #
      gLogger.info("RAWIntegrityAgent.execute: Registering correctly migrated files to the File Catalog.")
      for lfn in filesToRemove:
        pfn = activeFiles[lfn]['PFN']
        size = activeFiles[lfn]['Size']
        se = activeFiles[lfn]['SE']
        guid = activeFiles[lfn]['GUID']
        checksum = activeFiles[lfn]['Checksum']
        fileTuple = (lfn,pfn,size,se,guid,checksum)
        res = self.ReplicaManager.registerFile(fileTuple)
        if not res['OK']:
          self.DataLog.addFileRecord(lfn,'RegisterFailed',se,'','RAWIntegrityAgent')
          gLogger.error("RAWIntegrityAgent.execute: Completely failed to register successfully migrated file.", res['Message'])
        elif not res['Value']['Successful'].has_key(lfn):
          self.DataLog.addFileRecord(lfn,'RegisterFailed',se,'','RAWIntegrityAgent')
          gLogger.error("RAWIntegrityAgent.execute: Failed to register lfn in the File Catalog.", res['Value']['Failed'][lfn])
        else:
          self.DataLog.addFileRecord(lfn,'Register',se,'','RAWIntegrityAgent')
          gLogger.info("RAWIntegrityAgent.execute: Successfully registered %s in the File Catalog." % lfn)
          ############################################################
          #
          # Create a removal request and set it to the gateway request DB
          #
          gLogger.info("RAWIntegrityAgent.execute: Creating removal request for correctly migrated files.")
          oRequest = RequestContainer()
          subRequestIndex = oRequest.initiateSubRequest('removal')['Value']
          attributeDict = {'Operation':'physicalRemoval','TargetSE':'OnlineRunDB'}
          oRequest.setSubRequestAttributes(subRequestIndex,'removal',attributeDict)
          filesDict = [{'LFN':lfn,'PFN':pfn}]
          oRequest.setSubRequestFiles(subRequestIndex,'removal',filesDict)
          fileName = os.path.basename(lfn)
          requestName = 'remove_%s.xml' % fileName
          requestString = oRequest.toXML()['Value']
          gLogger.info("RAWIntegrityAgent.execute: Attempting to put %s to gateway requestDB." %  requestName)
          res = self.RequestDBClient.setRequest(requestName,requestString,self.gatewayUrl)
          if not res['OK']:
            gLogger.error("RAWIntegrityAgent.execute: Failed to set removal request to gateway requestDB.", res['Message'])
          else:
            gLogger.info("RAWIntegrityAgent.execute: Successfully put %s to gateway requestDB." %  requestName)
            ############################################################
            #
            # Remove the file from the list of files awaiting migration in database
            #
            gLogger.info("RAWIntegrityAgent.execute: Updating status of %s in raw integrity database." %  lfn)
            res = self.RAWIntegrityDB.setFileStatus(lfn,'Done')
            if not res['OK']:
              gLogger.error("RAWIntegrityAgent.execute: Failed to update status in raw integrity database.", res['Message'])
            else:
              gLogger.info("RAWIntegrityAgent.execute: Successfully updated status in raw integrity database.")
              gMonitor.addMark("MigrationTime",activeFiles[lfn]['WaitTime'])

    if len(filesToTransfer) > 0:
      ############################################################
      #
      # Remove the incorrectly migrated files from the storage element (will be over written anyways)
      #
      gLogger.info("RAWIntegrityAgent.execute: Removing incorrectly migrated files from Storage Element.")
      for lfn in filesToTransfer:
        pfn = activeFiles[lfn]['PFN']
        size = activeFiles[lfn]['Size']
        se = activeFiles[lfn]['SE']
        guid = activeFiles[lfn]['GUID']
        res = self.ReplicaManager.removePhysicalFile(se,pfn)
        if not res['OK']:
          self.DataLog.addFileRecord(lfn,'RemoveReplicaFailed',se,'','RAWIntegrityAgent')
          gLogger.error("RAWIntegrityAgent.execute: Completely failed to remove pfn from the storage element.", res['Message'])
        elif not res['Value']['Successful'].has_key(pfn):
          self.DataLog.addFileRecord(lfn,'RemoveReplicaFailed',se,'','RAWIntegrityAgent')
          gLogger.error("RAWIntegrityAgent.execute: Failed to remove pfn from the storage element.", res['Value']['Failed'][pfn])
        else:
          self.DataLog.addFileRecord(lfn,'RemoveReplica',se,'','RAWIntegrityAgent')
          gLogger.info("RAWIntegrityAgent.execute: Successfully removed pfn from the storage element.")
          ############################################################
          #
          # Create a transfer request for the files incorrectly migrated
          #
          gLogger.info("RAWIntegrityAgent.execute: Creating (re)transfer request for incorrectly migrated files.")
          oRequest = RequestContainer()
          subRequestIndex = oRequest.initiateSubRequest('removal')['Value']
          attributeDict = {'Operation':'reTransfer','TargetSE':'OnlineRunDB'}
          oRequest.setSubRequestAttributes(subRequestIndex,'removal',attributeDict)
          fileName = os.path.basename(lfn)
          filesDict = [{'LFN':lfn,'PFN':fileName}]
          oRequest.setSubRequestFiles(subRequestIndex,'removal',filesDict)
          requestName = 'retransfer_%s.xml' % fileName
          requestString = oRequest.toXML()['Value']
          gLogger.info("RAWIntegrityAgent.execute: Attempting to put %s to gateway requestDB." %  requestName)
          res = self.RequestDBClient.setRequest(requestName,requestString,self.gatewayUrl)
          if not res['OK']:
            gLogger.error("RAWIntegrityAgent.execute: Failed to set removal request to gateway requestDB.", res['Message'])
          else:
            gLogger.info("RAWIntegrityAgent.execute: Successfully put %s to gateway requestDB." %  requestName)
            ############################################################
            #
            # Remove the file from the list of files awaiting migration in database
            #
            gLogger.info("RAWIntegrityAgent.execute: Updating status of %s in raw integrity database." %  lfn)
            res = self.RAWIntegrityDB.setFileStatus(lfn,'Failed')
            if not res['OK']:
              gLogger.error("RAWIntegrityAgent.execute: Failed to update status in raw integrity database.", res['Message'])
            else:
              gLogger.info("RAWIntegrityAgent.execute: Successfully updated status in raw integrity database.")

    return S_OK()



