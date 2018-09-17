__RCSID__ = "$Id $"


import datetime

# Requires at least version 3.3.3
import fts3.rest.client.easy as fts3
from fts3.rest.client.exceptions import FTS3ClientException
from fts3.rest.client.request import Request as ftsSSLRequest

from DIRAC.Resources.Storage.StorageElement import StorageElement

from DIRAC.FrameworkSystem.Client.Logger import gLogger

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

from DIRAC.DataManagementSystem.private.FTS3Utilities import FTS3Serializable
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File


class FTS3Job(FTS3Serializable):
  """ Abstract class to represent a job to be executed by FTS. It belongs
      to an FTS3Operation
  """

  # States from FTS doc
  ALL_STATES = ['Submitted',  # Initial state of a job as soon it's dropped into the database
                'Ready',  # One of the files within a job went to Ready state
                'Active',  # One of the files within a job went to Active state
                'Finished',  # All files Finished gracefully
                'Canceled',  # Job canceled
                'Failed',  # All files Failed
                'Finisheddirty',  # Some files Failed
                'Staging',  # One of the files within a job went to Staging state
               ]

  FINAL_STATES = ['Canceled', 'Failed', 'Finished', 'Finisheddirty']
  INIT_STATE = 'Submitted'

  _attrToSerialize = ['jobID', 'operationID', 'status', 'error', 'submitTime',
                      'lastUpdate', 'ftsServer', 'ftsGUID', 'completeness',
                      'username', 'userGroup']

  def __init__(self):

    self.submitTime = None
    self.lastUpdate = None
    self.lastMonitor = None

    self.ftsGUID = None
    self.ftsServer = None

    self.error = None
    self.status = FTS3Job.INIT_STATE

    self.completeness = None
    self.operationID = None

    self.username = None
    self.userGroup = None

    # temporary used only for submission
    # Set by FTS Operation when preparing
    self.type = None  # Transfer, Staging, Removal

    self.sourceSE = None
    self.targetSE = None
    self.filesToSubmit = []
    self.activity = None
    self.priority = None
    self.vo = None

    # temporary used only for accounting
    # it is set by the monitor method
    # when a job is in a final state
    self.accountingDict = None

  def monitor(self, context=None, ftsServer=None, ucert=None):
    """ Queries the fts server to monitor the job

        This method assumes that the attribute self.ftsGUID is set

        :param context: fts3 context. If not given, it is created (see ftsServer & ucert param)
        :param ftsServer: the address of the fts server to submit to. Used only if context is
                          not given. if not given either, use the ftsServer object attribute

        :param ucert: path to the user certificate/proxy. Might be infered by the fts cli (see its doc)

        :returns {FileID: { status, error } }
    """

    if not self.ftsGUID:
      return S_ERROR("FTSGUID not set, FTS job not submitted?")

    if not context:
      if not ftsServer:
        ftsServer = self.ftsServer
      context = fts3.Context(
          endpoint=ftsServer,
          ucert=ucert,
          request_class=ftsSSLRequest,
          verify=False)

    jobStatusDict = None
    try:
      jobStatusDict = fts3.get_job_status(context, self.ftsGUID, list_files=True)
    except FTS3ClientException as e:
      return S_ERROR("Error getting the job status %s" % e)

    now = datetime.datetime.utcnow().replace(microsecond=0)
    self.lastMonitor = now

    newStatus = jobStatusDict['job_state'].capitalize()
    if newStatus != self.status:
      self.status = newStatus
      self.lastUpdate = now
      self.error = jobStatusDict['reason']

    if newStatus in self.FINAL_STATES:
      self._fillAccountingDict(jobStatusDict)

    filesInfoList = jobStatusDict['files']
    filesStatus = {}
    statusSummary = {}

    for fileDict in filesInfoList:
      file_state = fileDict['file_state'].capitalize()
      file_id = fileDict['file_metadata']
      file_error = fileDict['reason']
      filesStatus[file_id] = {'status': file_state, 'error': file_error}

      statusSummary[file_state] = statusSummary.get(file_state, 0) + 1

    total = len(filesInfoList)
    completed = sum([statusSummary.get(state, 0) for state in FTS3File.FTS_FINAL_STATES])
    self.completeness = 100 * completed / total

    return S_OK(filesStatus)

  @staticmethod
  def __fetchSpaceToken(seName):
    """ Fetch the space token of storage element

        :param seName name of the storageElement

        :returns space token
    """
    seToken = None
    if seName:
      seObj = StorageElement(seName)

      res = seObj.getStorageParameters(protocol='srm')
      if not res['OK']:
        return res

      seToken = res["Value"].get("SpaceToken")

    return S_OK(seToken)

  @staticmethod
  def __isTapeSE(seName):
    """ Check whether a given SE is a tape storage

        :param seName name of the storageElement

        :returns True/False
                 In case of error, returns True.
                 It is better to loose a bit of time on the FTS
                 side, rather than failing jobs because the FTS default
                 pin time is too short
    """
    isTape = StorageElement(seName).getStatus()\
        .get('Value', {})\
        .get('TapeSE', True)

    return isTape

  def _constructTransferJob(self, context, pinTime, allTargetSURLs, failedLFNs, target_spacetoken):
    """ Build a job for transfer

        Some attributes of the job are expected to be set
          * sourceSE
          * targetSE
          * activity (optional)
          * priority (optional)
          * filesToSubmit
          * operationID (optional, used as metadata for the job)


        :param context: fts3 context
        :param pinTime: pining time in case staging is needed
        :param allTargetSURLs: dict {lfn:surl} for the target
        :param failedLFNs: set of LFNs in filesToSubmit for which there was a problem
        :param target_spacetoken: the space token of the target

        :return: S_OK( (job object, list of ftsFileIDs in the job))
    """

    log = gLogger.getSubLogger(
        "constructTransferJob/%s/%s_%s" %
        (self.operationID, self.sourceSE, self.targetSE), True)

    res = self.__fetchSpaceToken(self.sourceSE)
    if not res['OK']:
      return res
    source_spacetoken = res['Value']

    # getting all the source surls
    res = StorageElement(self.sourceSE, vo=self.vo).getURL(allTargetSURLs, protocol='srm')
    if not res['OK']:
      return res

    for lfn, reason in res['Value']['Failed'].iteritems():
      failedLFNs.add(lfn)
      log.error("Could not get source SURL", "%s %s" % (lfn, reason))

    allSourceSURLs = res['Value']['Successful']

    transfers = []

    fileIDsInTheJob = []

    for ftsFile in self.filesToSubmit:

      if ftsFile.lfn in failedLFNs:
        log.debug("Not preparing transfer for file %s" % ftsFile.lfn)
        continue

      sourceSURL = allSourceSURLs[ftsFile.lfn]
      targetSURL = allTargetSURLs[ftsFile.lfn]

      if sourceSURL == targetSURL:
        log.error("sourceSURL equals to targetSURL", "%s" % ftsFile.lfn)
        ftsFile.error = "sourceSURL equals to targetSURL"
        ftsFile.status = 'Defunct'
        continue

      trans = fts3.new_transfer(sourceSURL,
                                targetSURL,
                                checksum='ADLER32:%s' % ftsFile.checksum,
                                filesize=ftsFile.size,
                                metadata=getattr(ftsFile, 'fileID'),
                                activity=self.activity)

      transfers.append(trans)
      fileIDsInTheJob.append(getattr(ftsFile, 'fileID'))

    # If the source is not an tape SE, we should set the
    # copy_pin_lifetime and bring_online params to None,
    # otherwise they will do an extra useless queue in FTS
    sourceIsTape = self.__isTapeSE(self.sourceSE)
    copy_pin_lifetime = pinTime if sourceIsTape else None
    bring_online = 86400 if sourceIsTape else None

    if not transfers:
      log.error("No transfer possible!")
      return S_ERROR("No transfer possible")

    # We add a few metadata to the fts job so that we can reuse them later on without
    # querying our DB.
    # source and target SE are just used for accounting purpose
    job_metadata = {
        'operationID': self.operationID,
        'sourceSE': self.sourceSE,
        'targetSE': self.targetSE}

    job = fts3.new_job(transfers=transfers,
                       overwrite=True,
                       source_spacetoken=source_spacetoken,
                       spacetoken=target_spacetoken,
                       bring_online=bring_online,
                       copy_pin_lifetime=copy_pin_lifetime,
                       retry=3,
                       metadata=job_metadata,
                       priority=self.priority)

    return S_OK((job, fileIDsInTheJob))

  def _constructRemovalJob(self, context, allTargetSURLs, failedLFNs, target_spacetoken):
    """ Build a job for removal

        Some attributes of the job are expected to be set
          * targetSE
          * activity (optional)
          * priority (optional)
          * filesToSubmit
          * operationID (optional, used as metadata for the job)


        :param context: fts3 context
        :param allTargetSURLs: dict {lfn:surl} for the target
        :param failedLFNs: set of LFNs in filesToSubmit for which there was a problem
        :param target_spacetoken: the space token of the target

        :return: S_OK( (job object, list of ftsFileIDs in the job))
    """

    log = gLogger.getSubLogger(
        "constructRemovalJob/%s/%s" %
        (self.operationID, self.targetSE), True)

    transfers = []
    fileIDsInTheJob = []
    for ftsFile in self.filesToSubmit:

      if ftsFile.lfn in failedLFNs:
        log.debug("Not preparing transfer for file %s" % ftsFile.lfn)
        continue

      transfers.append({'surl': allTargetSURLs[ftsFile.lfn],
                        'metadata': getattr(ftsFile, 'fileID')})
      fileIDsInTheJob.append(getattr(ftsFile, 'fileID'))

    # We add a few metadata to the fts job so that we can reuse them later on without
    # querying our DB.
    # source and target SE are just used for accounting purpose
    job_metadata = {
        'operationID': self.operationID,
        'sourceSE': self.sourceSE,
        'targetSE': self.targetSE}

    job = fts3.new_delete_job(transfers,
                              spacetoken=target_spacetoken,
                              metadata=job_metadata)
    job['params']['retry'] = 3
    job['params']['priority'] = self.priority

    return S_OK((job, fileIDsInTheJob))

  def _constructStagingJob(self, context, pinTime, allTargetSURLs, failedLFNs, target_spacetoken):
    """ Build a job for staging

        Some attributes of the job are expected to be set
          * targetSE
          * activity (optional)
          * priority (optional)
          * filesToSubmit
          * operationID (optional, used as metadata for the job)

        :param context: fts3 context
        :param pinTime: pining time in case staging is needed
        :param allTargetSURLs: dict {lfn:surl} for the target
        :param failedLFNs: set of LFNs in filesToSubmit for which there was a problem
        :param target_spacetoken: the space token of the target

        :return: S_OK( (job object, list of ftsFileIDs in the job))
    """

    log = gLogger.getSubLogger(
        "constructStagingJob/%s/%s" %
        (self.operationID, self.targetSE), True)

    transfers = []
    fileIDsInTheJob = []

    for ftsFile in self.filesToSubmit:

      if ftsFile.lfn in failedLFNs:
        log.debug("Not preparing transfer for file %s" % ftsFile.lfn)
        continue

      sourceSURL = targetSURL = allTargetSURLs[ftsFile.lfn]
      trans = fts3.new_transfer(sourceSURL,
                                targetSURL,
                                checksum='ADLER32:%s' % ftsFile.checksum,
                                filesize=ftsFile.size,
                                metadata=getattr(ftsFile, 'fileID'),
                                activity=self.activity)

      transfers.append(trans)
      fileIDsInTheJob.append(getattr(ftsFile, 'fileID'))

    # If the source is not an tape SE, we should set the
    # copy_pin_lifetime and bring_online params to None,
    # otherwise they will do an extra useless queue in FTS
    sourceIsTape = self.__isTapeSE(self.sourceSE)
    copy_pin_lifetime = pinTime if sourceIsTape else None
    bring_online = 86400 if sourceIsTape else None

    # We add a few metadata to the fts job so that we can reuse them later on without
    # querying our DB.
    # source and target SE are just used for accounting purpose
    job_metadata = {
        'operationID': self.operationID,
        'sourceSE': self.sourceSE,
        'targetSE': self.targetSE}

    job = fts3.new_job(transfers=transfers,
                       overwrite=True,
                       source_spacetoken=target_spacetoken,
                       spacetoken=target_spacetoken,
                       bring_online=bring_online,
                       copy_pin_lifetime=copy_pin_lifetime,
                       retry=3,
                       metadata=job_metadata,
                       priority=self.priority)

    return S_OK((job, fileIDsInTheJob))

  def submit(self, context=None, ftsServer=None, ucert=None, pinTime=36000, ):
    """ submit the job to the FTS server

        Some attributes are expected to be defined for the submission to work:
          * type (set by FTS3Operation)
          * sourceSE (only for Transfer jobs)
          * targetSE
          * activity (optional)
          * priority (optional)
          * username
          * userGroup
          * filesToSubmit
          * operationID (optional, used as metadata for the job)

        We also expect the FTSFiles have an ID defined, as it is given as transfer metadata

        :param pinTime: Time the file should be pinned on disk (used for transfers and staging)
                        Used only if he source SE is a tape storage
        :param context: fts3 context. If not given, it is created (see ftsServer & ucert param)
        :param ftsServer: the address of the fts server to submit to. Used only if context is
                          not given. if not given either, use the ftsServer object attribute

        :param ucert: path to the user certificate/proxy. Might be inferred by the fts cli (see its doc)

        :returns S_OK([FTSFiles ids of files submitted])
    """

    log = gLogger.getSubLogger("submit/%s/%s_%s" %
                               (self.operationID, self.sourceSE, self.targetSE), True)

    if not context:
      if not ftsServer:
        ftsServer = self.ftsServer
      context = fts3.Context(
          endpoint=ftsServer,
          ucert=ucert,
          request_class=ftsSSLRequest,
          verify=False)

    # Construct the target SURL
    res = self.__fetchSpaceToken(self.targetSE)
    if not res['OK']:
      return res
    target_spacetoken = res['Value']

    allLFNs = [ftsFile.lfn for ftsFile in self.filesToSubmit]

    failedLFNs = set()

    # getting all the target surls
    res = StorageElement(self.targetSE, vo=self.vo).getURL(allLFNs, protocol='srm')
    if not res['OK']:
      return res

    for lfn, reason in res['Value']['Failed'].iteritems():
      failedLFNs.add(lfn)
      log.error("Could not get target SURL", "%s %s" % (lfn, reason))

    allTargetSURLs = res['Value']['Successful']

    if self.type == 'Transfer':
      res = self._constructTransferJob(
          context,
          pinTime,
          allTargetSURLs,
          failedLFNs,
          target_spacetoken)
    elif self.type == 'Staging':
      res = self._constructStagingJob(
          context,
          pinTime,
          allTargetSURLs,
          failedLFNs,
          target_spacetoken)
    elif self.type == 'Removal':
      res = self._constructRemovalJob(context, allTargetSURLs, failedLFNs, target_spacetoken)

    if not res['OK']:
      return res

    job, fileIDsInTheJob = res['Value']
    setFileIdsInTheJob = set(fileIDsInTheJob)

    try:
      self.ftsGUID = fts3.submit(context, job)
      log.info("Got GUID %s" % self.ftsGUID)

      # Only increase the amount of attempt
      # if we succeeded in submitting -> no ! Why did I do that ??
      for ftsFile in self.filesToSubmit:
        ftsFile.attempt += 1
        if ftsFile.fileID in setFileIdsInTheJob:
          ftsFile.status = 'Submitted'

      now = datetime.datetime.utcnow().replace(microsecond=0)
      self.submitTime = now
      self.lastUpdate = now
      self.lastMonitor = now

    except FTS3ClientException as e:
      log.exception("Error at submission", repr(e))
      return S_ERROR("Error at submission: %s" % e)

    return S_OK(fileIDsInTheJob)

  @staticmethod
  def generateContext(ftsServer, ucert):
    """ This method generates an fts3 context

        :param ftsServer: address of the fts3 server
        :param ucert: the path to the certificate to be used

        :returns: an fts3 context
    """
    try:
      context = fts3.Context(
          endpoint=ftsServer,
          ucert=ucert,
          request_class=ftsSSLRequest,
          verify=False)
      return S_OK(context)
    except FTS3ClientException as e:
      gLogger.exception("Error generating context", repr(e))
      return S_ERROR(repr(e))

  def _fillAccountingDict(self, jobStatusDict):
    """ This methods generates the necessary information to create a DataOperation
        accounting record, and stores them as a instance attribute.

        For it to be relevant, it should be called only when the job is in a final state.

        :param jobStatusDict: output of fts3.get_job_status

        :returns: None
    """

    accountingDict = dict()
    sourceSE = None
    targetSE = None

    accountingDict["OperationType"] = "ReplicateAndRegister"

    accountingDict["User"] = self.username
    accountingDict["Protocol"] = "FTS3"
    accountingDict['ExecutionSite'] = self.ftsServer

    # We cannot rely on all the transient attributes (like self.filesToSubmit)
    # because it is probably not filed by the time we monitor !

    filesInfoList = jobStatusDict['files']
    successfulFiles = []

    for fileDict in filesInfoList:
      file_state = fileDict['file_state'].capitalize()
      if file_state in FTS3File.FTS_SUCCESS_STATES:
        successfulFiles.append(fileDict)

    job_metadata = jobStatusDict['job_metadata']
    # previous version of the code did not have dictionary as
    # job_metadata
    if isinstance(job_metadata, dict):
      sourceSE = job_metadata.get('sourceSE')
      targetSE = job_metadata.get('targetSE')

    accountingDict["TransferOK"] = len(successfulFiles)
    accountingDict["TransferTotal"] = len(filesInfoList)
    accountingDict["TransferSize"] = sum([fileDict['filesize'] for fileDict in successfulFiles])
    accountingDict["FinalStatus"] = self.status
    accountingDict["Source"] = sourceSE
    accountingDict["Destination"] = targetSE
    accountingDict['TransferTime'] = sum(int(fileDict['tx_duration'])
                                         for fileDict in successfulFiles)

    # Registration values must be set anyway
    accountingDict['RegistrationTime'] = 0.0
    accountingDict['RegistrationOK'] = 0
    accountingDict['RegistrationTotal'] = 0

    self.accountingDict = accountingDict
