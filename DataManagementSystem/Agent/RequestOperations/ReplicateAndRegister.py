########################################################################
# File: ReplicateAndRegister.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 18:49:12
########################################################################
""" :mod: ReplicateAndRegister

    ==========================

    .. module: ReplicateAndRegister

    :synopsis: ReplicateAndRegister operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReplicateAndRegister operation handler
"""
__RCSID__ = "$Id$"
# #
# @file ReplicateAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 18:49:28
# @brief Definition of ReplicateAndRegister class.

# # imports
import re
from collections import defaultdict
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Adler import compareAdler, hexAdlerToInt, intAdlerToHex
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase

from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3TransferOperation
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Client import FTS3Client
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient


def filterReplicas(opFile, logger=None, dataManager=None):
  """ filter out banned/invalid source SEs """

  if logger is None:
    logger = gLogger
  if dataManager is None:
    dataManager = DataManager()

  log = logger.getSubLogger("filterReplicas")
  result = defaultdict(list)

  replicas = dataManager.getActiveReplicas(opFile.LFN, getUrl=False)
  if not replicas["OK"]:
    log.error('Failed to get active replicas', replicas["Message"])
    return replicas
  reNotExists = re.compile(r".*such file.*")
  replicas = replicas["Value"]
  failed = replicas["Failed"].get(opFile.LFN, "")
  if reNotExists.match(failed.lower()):
    opFile.Status = "Failed"
    opFile.Error = failed
    return S_ERROR(failed)

  replicas = replicas["Successful"].get(opFile.LFN, {})
  noReplicas = False
  if not replicas:
    allReplicas = dataManager.getReplicas(opFile.LFN, getUrl=False)
    if allReplicas['OK']:
      allReplicas = allReplicas['Value']['Successful'].get(opFile.LFN, {})
      if not allReplicas:
        result['NoReplicas'].append(None)
        noReplicas = True
      else:
        # There are replicas but we cannot get metadata because the replica is not active
        result['NoActiveReplicas'] += list(allReplicas)
      log.verbose("File has no%s replica in File Catalog" % ('' if noReplicas else ' active'), opFile.LFN)
    else:
      return allReplicas

  if not opFile.Checksum or hexAdlerToInt(opFile.Checksum) is False:
    # Set Checksum to FC checksum if not set in the request
    fcMetadata = FileCatalog().getFileMetadata(opFile.LFN)
    fcChecksum = fcMetadata.get(
        'Value',
        {}).get(
        'Successful',
        {}).get(
        opFile.LFN,
        {}).get('Checksum')
    # Replace opFile.Checksum if it doesn't match a valid FC checksum
    if fcChecksum:
      if hexAdlerToInt(fcChecksum) is not False:
        opFile.Checksum = fcChecksum
        opFile.ChecksumType = fcMetadata['Value']['Successful'][opFile.LFN].get('ChecksumType', 'Adler32')
      else:
        opFile.Checksum = None

  # If no replica was found, return what we collected as information
  if not replicas:
    return S_OK(result)

  for repSEName in replicas:
    repSEMetadata = StorageElement(repSEName).getFileMetadata(opFile.LFN)
    error = repSEMetadata.get('Message', repSEMetadata.get('Value', {}).get('Failed', {}).get(opFile.LFN))
    if error:
      log.warn('unable to get metadata at %s for %s' % (repSEName, opFile.LFN), error.replace('\n', ''))
      if 'File does not exist' in error:
        result['NoReplicas'].append(repSEName)
      else:
        result["NoMetadata"].append(repSEName)
    elif not noReplicas:
      repSEMetadata = repSEMetadata['Value']['Successful'][opFile.LFN]

      seChecksum = hexAdlerToInt(repSEMetadata.get("Checksum"))
      # As from here seChecksum is an integer or False, not a hex string!
      if seChecksum is False and opFile.Checksum:
        result['NoMetadata'].append(repSEName)
      elif not seChecksum and opFile.Checksum:
        opFile.Checksum = None
        opFile.ChecksumType = None
      elif seChecksum and (not opFile.Checksum or opFile.Checksum == 'False'):
        # Use the SE checksum (convert to hex) and force type to be Adler32
        opFile.Checksum = intAdlerToHex(seChecksum)
        opFile.ChecksumType = 'Adler32'
      if not opFile.Checksum or not seChecksum or compareAdler(
              intAdlerToHex(seChecksum), opFile.Checksum):
        # # All checksums are OK
        result["Valid"].append(repSEName)
      else:
        log.warn(" %s checksum mismatch, FC: '%s' @%s: '%s'" %
                 (opFile.LFN, opFile.Checksum, repSEName, intAdlerToHex(seChecksum)))
        result["Bad"].append(repSEName)
    else:
      # If a replica was found somewhere, don't set the file as no replicas
      result['NoReplicas'] = []

  return S_OK(result)


########################################################################
class ReplicateAndRegister(DMSRequestOperationsBase):
  """
  .. class:: ReplicateAndRegister

  ReplicateAndRegister operation handler
  """

  def __init__(self, operation=None, csPath=None):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    super(ReplicateAndRegister, self).__init__(operation, csPath)
    # # own gMonitor stuff for files
    gMonitor.registerActivity("ReplicateAndRegisterAtt", "Replicate and register attempted",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("ReplicateOK", "Replications successful",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("ReplicateFail", "Replications failed",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("RegisterOK", "Registrations successful",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("RegisterFail", "Registrations failed",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    # # for FTS
    gMonitor.registerActivity("FTSScheduleAtt", "Files schedule attempted",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("FTSScheduleOK", "File schedule successful",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("FTSScheduleFail", "File schedule failed",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    # # SE cache

    # Clients
    self.fc = FileCatalog()

  def __call__(self):
    """ call me maybe """
    # # check replicas first
    checkReplicas = self.__checkReplicas()
    if not checkReplicas["OK"]:
      self.log.error('Failed to check replicas', checkReplicas["Message"])
    if hasattr(self, "FTSMode") and getattr(self, "FTSMode"):
      bannedGroups = getattr(self, "FTSBannedGroups") if hasattr(self, "FTSBannedGroups") else ()
      if self.request.OwnerGroup in bannedGroups:
        self.log.verbose("usage of FTS system is banned for request's owner")
        return self.dmTransfer()

      if getattr(self, 'UseNewFTS3', False):
        return self.fts3Transfer()
      else:
        return self.ftsTransfer()

    return self.dmTransfer()

  def __checkReplicas(self):
    """ check done replicas and update file states  """
    waitingFiles = dict([(opFile.LFN, opFile) for opFile in self.operation
                         if opFile.Status in ("Waiting", "Scheduled")])
    targetSESet = set(self.operation.targetSEList)

    replicas = self.fc.getReplicas(waitingFiles.keys())
    if not replicas["OK"]:
      self.log.error('Failed to get replicas', replicas["Message"])
      return replicas

    reMissing = re.compile(r".*such file.*")
    for failedLFN, errStr in replicas["Value"]["Failed"].iteritems():
      waitingFiles[failedLFN].Error = errStr
      if reMissing.search(errStr.lower()):
        self.log.error("File does not exists", failedLFN)
        gMonitor.addMark("ReplicateFail", len(targetSESet))
        waitingFiles[failedLFN].Status = "Failed"

    for successfulLFN, reps in replicas["Value"]["Successful"].iteritems():
      if targetSESet.issubset(set(reps)):
        self.log.info("file %s has been replicated to all targets" % successfulLFN)
        waitingFiles[successfulLFN].Status = "Done"

    return S_OK()

  def _addMetadataToFiles(self, toSchedule):
    """ Add metadata to those files that need to be scheduled through FTS

        toSchedule is a dictionary:
        {'lfn1': opFile, 'lfn2': opFile}
    """
    if toSchedule:
      self.log.info("found %s files to schedule, getting metadata from FC" % len(toSchedule))
    else:
      self.log.verbose("No files to schedule")
      return S_OK([])

    res = self.fc.getFileMetadata(toSchedule.keys())
    if not res['OK']:
      return res
    else:
      if res['Value']['Failed']:
        self.log.warn("Can't schedule %d files: problems getting the metadata: %s" %
                      (len(res['Value']['Failed']), ', '.join(res['Value']['Failed'])))
      metadata = res['Value']['Successful']

    filesToSchedule = {}

    for lfn, lfnMetadata in metadata.iteritems():
      opFileToSchedule = toSchedule[lfn][0]
      opFileToSchedule.GUID = lfnMetadata['GUID']
      # In principle this is defined already in filterReplicas()
      if not opFileToSchedule.Checksum:
        opFileToSchedule.Checksum = metadata[lfn]['Checksum']
        opFileToSchedule.ChecksumType = metadata[lfn]['ChecksumType']
      opFileToSchedule.Size = metadata[lfn]['Size']

      filesToSchedule[opFileToSchedule.LFN] = opFileToSchedule

    return S_OK(filesToSchedule)

  def _filterReplicas(self, opFile):
    """ filter out banned/invalid source SEs """
    return filterReplicas(opFile, logger=self.log, dataManager=self.dm)

  def ftsTransfer(self):
    """ replicate and register using FTS """

    self.log.info("scheduling files in FTS...")

    bannedTargets = self.checkSEsRSS()
    if not bannedTargets['OK']:
      gMonitor.addMark("FTSScheduleAtt")
      gMonitor.addMark("FTSScheduleFail")
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK("%s targets are banned for writing" % ",".join(bannedTargets['Value']))

    # Can continue now
    self.log.verbose("No targets banned for writing")

    toSchedule = {}

    delayExecution = 0
    errors = defaultdict(int)
    for opFile in self.getWaitingFilesList():
      opFile.Error = ''
      gMonitor.addMark("FTSScheduleAtt")
      # # check replicas
      replicas = self._filterReplicas(opFile)
      if not replicas["OK"]:
        continue
      replicas = replicas["Value"]

      validReplicas = replicas.get("Valid")
      noMetaReplicas = replicas.get("NoMetadata")
      noReplicas = replicas.get('NoReplicas')
      badReplicas = replicas.get('Bad')
      noActiveReplicas = replicas.get('NoActiveReplicas')

      if validReplicas:
        validTargets = list(set(self.operation.targetSEList) - set(validReplicas))
        if not validTargets:
          self.log.info("file %s is already present at all targets" % opFile.LFN)
          opFile.Status = "Done"
        else:
          toSchedule[opFile.LFN] = [opFile, validReplicas, validTargets]
      else:
        gMonitor.addMark("FTSScheduleFail")
        if noMetaReplicas:
          err = "Couldn't get metadata"
          errors[err] += 1
          self.log.verbose(
              "unable to schedule '%s', %s at %s" %
              (opFile.LFN, err, ','.join(noMetaReplicas)))
          opFile.Error = err
        elif noReplicas:
          err = "File doesn't exist"
          errors[err] += 1
          self.log.error("Unable to schedule transfer",
                         "%s %s at %s" % (opFile.LFN, err, ','.join(noReplicas)))
          opFile.Error = err
          opFile.Status = 'Failed'
        elif badReplicas:
          err = "All replicas have a bad checksum"
          errors[err] += 1
          self.log.error("Unable to schedule transfer",
                         "%s, %s at %s" % (opFile.LFN, err, ','.join(badReplicas)))
          opFile.Error = err
          opFile.Status = 'Failed'
        elif noActiveReplicas:
          err = "No active replica found"
          errors[err] += 1
          self.log.verbose("Unable to schedule transfer",
                           "%s, %s at %s" % (opFile.LFN, err, ','.join(noActiveReplicas)))
          opFile.Error = err
          # All source SEs are banned, delay execution by 1 hour
          delayExecution = 60

    if delayExecution:
      self.log.info("Delay execution of the request by %d minutes" % delayExecution)
      self.request.delayNextExecution(delayExecution)
    # Log error counts
    for error, count in errors.iteritems():
      self.log.error(error, 'for %d files' % count)

    filesToScheduleList = []
    res = self._addMetadataToFiles(toSchedule)
    if not res['OK']:
      return res
    else:
      filesToSchedule = res['Value']

      for lfn in filesToSchedule:
        filesToScheduleList.append((filesToSchedule[lfn][0].toJSON()['Value'],
                                    toSchedule[lfn][1],
                                    toSchedule[lfn][2]))

    if filesToScheduleList:

      ftsSchedule = FTSClient().ftsSchedule(self.request.RequestID,
                                            self.operation.OperationID,
                                            filesToScheduleList)
      if not ftsSchedule["OK"]:
        self.log.error("Completely failed to schedule to FTS:", ftsSchedule["Message"])
        return ftsSchedule

      # might have nothing to schedule
      ftsSchedule = ftsSchedule["Value"]
      if not ftsSchedule:
        return S_OK()

      self.log.info("%d files have been scheduled to FTS" % len(ftsSchedule['Successful']))
      for opFile in self.operation:
        fileID = opFile.FileID
        if fileID in ftsSchedule["Successful"]:
          gMonitor.addMark("FTSScheduleOK", 1)
          opFile.Status = "Scheduled"
          self.log.debug("%s has been scheduled for FTS" % opFile.LFN)
        elif fileID in ftsSchedule["Failed"]:
          gMonitor.addMark("FTSScheduleFail", 1)
          opFile.Error = ftsSchedule["Failed"][fileID]
          if 'sourceSURL equals to targetSURL' in opFile.Error:
            # In this case there is no need to continue
            opFile.Status = 'Failed'
          self.log.warn("unable to schedule %s for FTS: %s" % (opFile.LFN, opFile.Error))
    else:
      self.log.info("No files to schedule after metadata checks")

    # Just in case some transfers could not be scheduled, try them with RM
    return self.dmTransfer(fromFTS=True)

  def _checkExistingFTS3Operations(self):
    """
       Check if there are ongoing FTS3Operation for the current RMS Operation

       Under some conditions, we can be trying to schedule files while
       there is still an FTS transfer going on. This typically happens
       when the REA hangs. To prevent further race condition, we check
       if there are FTS3Operations in a non Final state matching the
       current operation ID. If so, we put the corresponding files in
       scheduled mode. We will then wait till the FTS3 Operation performs
       the callback

       :returns: S_OK with True if we can go on, False if we should stop the processing
    """

    res = FTS3Client().getOperationsFromRMSOpID(self.operation.OperationID)

    if not res['OK']:
      self.log.debug(
          "Could not get FTS3Operations matching OperationID",
          self.operation.OperationID)
      return res

    existingFTSOperations = res['Value']
    # It is ok to have FTS Operations in a final state, so we
    # care only about the others
    unfinishedFTSOperations = [
        ops for ops in existingFTSOperations if ops.status not in FTS3TransferOperation.FINAL_STATES]

    if not unfinishedFTSOperations:
      self.log.debug("No ongoing FTS3Operations, all good")
      return S_OK(True)

    self.log.warn("Some FTS3Operations already exist for the RMS Operation:",
                  [op.operationID for op in unfinishedFTSOperations])

    # This would really be a screwed up situation !
    if len(unfinishedFTSOperations) > 1:
      self.log.warn("That's a serious problem !!")

    # We take the rmsFileID of the files in the Operations,
    # find the corresponding File object, and set them scheduled
    rmsFileIDsToSetScheduled = set(
        [ftsFile.rmsFileID for ftsOp in unfinishedFTSOperations for ftsFile in ftsOp.ftsFiles])

    for opFile in self.operation:
      # If it is in the DB, it has a FileID
      opFileID = opFile.FileID
      if opFileID in rmsFileIDsToSetScheduled:
        self.log.warn("Setting RMSFile as already scheduled", opFileID)
        opFile.Status = "Scheduled"

    # We return here such that the Request is set back to Scheduled in the DB
    # With no further modification
    return S_OK(False)

  def fts3Transfer(self):
    """ replicate and register using FTS3 """

    self.log.info("scheduling files in FTS3...")

    # Check first if we do not have ongoing transfers

    res = self._checkExistingFTS3Operations()
    if not res['OK']:
      return res

    # if res['Value'] is False
    # it means that there are ongoing transfers
    # and we should stop here
    if res['Value'] is False:
      # return S_OK such that the request is put back
      return S_OK()

    fts3Files = []
    toSchedule = {}

    # Dict which maps the FileID to the object
    rmsFilesIds = {}

    for opFile in self.getWaitingFilesList():
      rmsFilesIds[opFile.FileID] = opFile

      opFile.Error = ''
      gMonitor.addMark("FTSScheduleAtt")
      # # check replicas
      replicas = self._filterReplicas(opFile)
      if not replicas["OK"]:
        continue
      replicas = replicas["Value"]

      validReplicas = replicas["Valid"]
      noMetaReplicas = replicas["NoMetadata"]
      noReplicas = replicas['NoReplicas']
      badReplicas = replicas['Bad']
      noPFN = replicas['NoPFN']

      if validReplicas:
        validTargets = list(set(self.operation.targetSEList) - set(validReplicas))
        if not validTargets:
          self.log.info("file %s is already present at all targets" % opFile.LFN)
          opFile.Status = "Done"
        else:
          toSchedule[opFile.LFN] = [opFile, validTargets]

      else:
        gMonitor.addMark("FTSScheduleFail")
        if noMetaReplicas:
          self.log.warn("unable to schedule '%s', couldn't get metadata at %s" % (opFile.LFN, ','.join(noMetaReplicas)))
          opFile.Error = "Couldn't get metadata"
        elif noReplicas:
          self.log.error(
              "Unable to schedule transfer", "File %s doesn't exist at %s" %
              (opFile.LFN, ','.join(noReplicas)))
          opFile.Error = 'No replicas found'
          opFile.Status = 'Failed'
        elif badReplicas:
          self.log.error(
              "Unable to schedule transfer",
              "File %s, all replicas have a bad checksum at %s" %
              (opFile.LFN,
               ','.join(badReplicas)))
          opFile.Error = 'All replicas have a bad checksum'
          opFile.Status = 'Failed'
        elif noPFN:
          self.log.warn(
              "unable to schedule %s, could not get a PFN at %s" %
              (opFile.LFN, ','.join(noPFN)))

    res = self._addMetadataToFiles(toSchedule)
    if not res['OK']:
      return res
    else:
      filesToSchedule = res['Value']

      for lfn in filesToSchedule:
        opFile = filesToSchedule[lfn]
        validTargets = toSchedule[lfn][1]
        for targetSE in validTargets:
          ftsFile = FTS3File.fromRMSFile(opFile, targetSE)
          fts3Files.append(ftsFile)

    if fts3Files:
      res = Registry.getUsernameForDN(self.request.OwnerDN)
      if not res['OK']:
        self.log.error(
            "Cannot get username for DN", "%s %s" %
            (self.request.OwnerDN, res['Message']))
        return res

      username = res['Value']
      fts3Operation = FTS3TransferOperation.fromRMSObjects(self.request, self.operation, username)
      fts3Operation.ftsFiles = fts3Files

      ftsSchedule = FTS3Client().persistOperation(fts3Operation)
      if not ftsSchedule["OK"]:
        self.log.error("Completely failed to schedule to FTS3:", ftsSchedule["Message"])
        return ftsSchedule

      # might have nothing to schedule
      ftsSchedule = ftsSchedule["Value"]
      self.log.info("Scheduled with FTS3Operation id %s" % ftsSchedule)

      self.log.info("%d files have been scheduled to FTS3" % len(fts3Files))

      for ftsFile in fts3Files:
        opFile = rmsFilesIds[ftsFile.rmsFileID]
        gMonitor.addMark("FTSScheduleOK", 1)
        opFile.Status = "Scheduled"
        self.log.debug("%s has been scheduled for FTS" % opFile.LFN)
    else:
      self.log.info("No files to schedule after metadata checks")

    # Just in case some transfers could not be scheduled, try them with RM
    return self.dmTransfer(fromFTS=True)

  def dmTransfer(self, fromFTS=False):
    """ replicate and register using dataManager  """
    # # get waiting files. If none just return
    # # source SE
    sourceSE = self.operation.SourceSE if self.operation.SourceSE else None
    if sourceSE:
      # # check source se for read
      bannedSource = self.checkSEsRSS(sourceSE, 'ReadAccess')
      if not bannedSource["OK"]:
        gMonitor.addMark("ReplicateAndRegisterAtt", len(self.operation))
        gMonitor.addMark("ReplicateFail", len(self.operation))
        return bannedSource

      if bannedSource["Value"]:
        self.operation.Error = "SourceSE %s is banned for reading" % sourceSE
        self.log.info(self.operation.Error)
        return S_OK(self.operation.Error)

    # # check targetSEs for write
    bannedTargets = self.checkSEsRSS()
    if not bannedTargets['OK']:
      gMonitor.addMark("ReplicateAndRegisterAtt", len(self.operation))
      gMonitor.addMark("ReplicateFail", len(self.operation))
      return bannedTargets

    if bannedTargets['Value']:
      self.operation.Error = "%s targets are banned for writing" % ",".join(bannedTargets['Value'])
      return S_OK(self.operation.Error)

    # Can continue now
    self.log.verbose("No targets banned for writing")

    waitingFiles = self.getWaitingFilesList()
    if not waitingFiles:
      return S_OK()
    # # loop over files
    if fromFTS:
      self.log.info("Trying transfer using replica manager as FTS failed")
    else:
      self.log.info("Transferring files using Data manager...")
    errors = defaultdict(int)
    delayExecution = 0
    for opFile in waitingFiles:
      if opFile.Error in ("Couldn't get metadata",
                          "File doesn't exist",
                          'No active replica found',
                          "All replicas have a bad checksum",):
        err = "File already in error status"
        errors[err] += 1

      gMonitor.addMark("ReplicateAndRegisterAtt", 1)
      opFile.Error = ''
      lfn = opFile.LFN

      # Check if replica is at the specified source
      replicas = self._filterReplicas(opFile)
      if not replicas["OK"]:
        self.log.error('Failed to check replicas', replicas["Message"])
        continue
      replicas = replicas["Value"]
      validReplicas = replicas.get("Valid")
      noMetaReplicas = replicas.get("NoMetadata")
      noReplicas = replicas.get('NoReplicas')
      badReplicas = replicas.get('Bad')
      noActiveReplicas = replicas.get('NoActiveReplicas')

      if not validReplicas:
        gMonitor.addMark("ReplicateFail")
        if noMetaReplicas:
          err = "Couldn't get metadata"
          errors[err] += 1
          self.log.verbose(
              "unable to replicate '%s', couldn't get metadata at %s" %
              (opFile.LFN, ','.join(noMetaReplicas)))
          opFile.Error = err
        elif noReplicas:
          err = "File doesn't exist"
          errors[err] += 1
          self.log.verbose(
              "Unable to replicate", "File %s doesn't exist at %s" %
              (opFile.LFN, ','.join(noReplicas)))
          opFile.Error = err
          opFile.Status = 'Failed'
        elif badReplicas:
          err = "All replicas have a bad checksum"
          errors[err] += 1
          self.log.error(
              "Unable to replicate", "%s, all replicas have a bad checksum at %s" %
              (opFile.LFN, ','.join(badReplicas)))
          opFile.Error = err
          opFile.Status = 'Failed'
        elif noActiveReplicas:
          err = "No active replica found"
          errors[err] += 1
          self.log.verbose("Unable to schedule transfer",
                           "%s, %s at %s" % (opFile.LFN, err, ','.join(noActiveReplicas)))
          opFile.Error = err
          # All source SEs are banned, delay execution by 1 hour
          delayExecution = 60
        continue
      # # get the first one in the list
      if sourceSE not in validReplicas:
        if sourceSE:
          err = "File not at specified source"
          errors[err] += 1
          self.log.warn(
              "%s is not at specified sourceSE %s, changed to %s" %
              (lfn, sourceSE, validReplicas[0]))
        sourceSE = validReplicas[0]

      # # loop over targetSE
      catalogs = self.operation.Catalog
      if catalogs:
        catalogs = [cat.strip() for cat in catalogs.split(',')]

      for targetSE in self.operation.targetSEList:

        # # call DataManager
        if targetSE in validReplicas:
          self.log.warn("Request to replicate %s to an existing location: %s" % (lfn, targetSE))
          continue
        res = self.dm.replicateAndRegister(lfn, targetSE, sourceSE=sourceSE, catalog=catalogs)
        if res["OK"]:

          if lfn in res["Value"]["Successful"]:

            if "replicate" in res["Value"]["Successful"][lfn]:

              repTime = res["Value"]["Successful"][lfn]["replicate"]
              prString = "file %s replicated at %s in %s s." % (lfn, targetSE, repTime)

              gMonitor.addMark("ReplicateOK", 1)

              if "register" in res["Value"]["Successful"][lfn]:

                gMonitor.addMark("RegisterOK", 1)
                regTime = res["Value"]["Successful"][lfn]["register"]
                prString += ' and registered in %s s.' % regTime
                self.log.info(prString)
              else:

                gMonitor.addMark("RegisterFail", 1)
                prString += " but failed to register"
                self.log.warn(prString)

                opFile.Error = "Failed to register"
                # # add register replica operation
                registerOperation = self.getRegisterOperation(
                    opFile, targetSE, type='RegisterReplica')
                self.request.insertAfter(registerOperation, self.operation)

            else:

              self.log.error("Failed to replicate", "%s to %s" % (lfn, targetSE))
              gMonitor.addMark("ReplicateFail", 1)
              opFile.Error = "Failed to replicate"

          else:

            gMonitor.addMark("ReplicateFail", 1)
            reason = res["Value"]["Failed"][lfn]
            self.log.error(
                "Failed to replicate and register", "File %s at %s:" %
                (lfn, targetSE), reason)
            opFile.Error = reason

        else:

          gMonitor.addMark("ReplicateFail", 1)
          opFile.Error = "DataManager error: %s" % res["Message"]
          self.log.error("DataManager error", res["Message"])

      if not opFile.Error:
        if len(self.operation.targetSEList) > 1:
          self.log.info("file %s has been replicated to all targetSEs" % lfn)
        opFile.Status = "Done"
    # Log error counts
    if delayExecution:
      self.log.info("Delay execution of the request by %d minutes" % delayExecution)
      self.request.delayNextExecution(delayExecution)
    for error, count in errors.iteritems():
      self.log.error(error, 'for %d files' % count)

    return S_OK()
