"""
Data recovery agent: sets as unused files that are really undone.
"""

""" In general for data processing producitons we need to completely abandon the 'by hand'
    reschedule operation such that accidental reschedulings don't result in data being processed twice.

    For all above cases the following procedure should be used to achieve 100%:

    - Starting from the data in the Production DB for each transformation
      look for files in the following status:
         Assigned
         MaxReset
      some of these will correspond to the final WMS status 'Failed'.

    For files in MaxReset and Assigned:
    - Discover corresponding job WMS ID
    - Check that there are no outstanding requests for the job
      o wait until all are treated before proceeding
    - Check that none of the job input data has BK descendants for the current production
      o if the data has a replica flag it means all was uploaded successfully - should be investigated by hand
      o if there is no replica flag can proceed with file removal from LFC / storage (can be disabled by flag)
    - Mark the recovered input file status as 'Unused' in the ProductionDB
"""

__RCSID__ = "$Id: $"
__VERSION__ = "$Revision: $"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Core.Utilities.Time import dateTime
from DIRAC.Core.Workflow.Workflow import fromXMLString
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from ILCDIRAC.Core.Utilities.ProductionData import constructProductionLFNs

import string
import datetime

AGENT_NAME = 'TransformationSystem/DataRecoveryAgent'


class DataRecoveryAgent(AgentModule):
  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'DataRecoveryAgent'
    self.log = gLogger
  #############################################################################

  def initialize(self):
    """Sets defaults
    """
    self.enableFlag = ''  # defined below
    self.replicaManager = ReplicaManager()
    self.prodDB = TransformationClient()
    self.requestClient = RequestClient()
    self.taskIDName = 'TaskID'
    self.externalStatus = 'ExternalStatus'
    self.externalID = 'ExternalID'
    self.am_setOption('PollingTime', 2 * 60 * 60)  # no stalled jobs are considered so can be frequent
    self.enableFlag = self.am_getOption('EnableFlag', False)
    self.am_setModuleParam("shifterProxy", "ProductionManager")
    self.ops = Operations()
    return S_OK()
  #############################################################################

  def execute(self):
    """ The main execution method.
    """
    self.log.info('Enable flag is %s' % self.enableFlag)
    self.removalOKFlag = True

    transformationTypes = ['MCReconstruction', 'MCSimulation', 'MCReconstruction_Overlay', 'Merge']
    transformationStatus = ['Active', 'Completing']
    fileSelectionStatus = ['Assigned', 'MaxReset']
    updateStatus = 'Unused'
    wmsStatusList = ['Failed']
    #only worry about files > 12hrs since last update
    selectDelay = self.am_getOption("Delay", 2)  # hours

    transformationDict = {}
    for transStatus in transformationStatus:
      result = self.getEligibleTransformations(transStatus, transformationTypes)
      if not result['OK']:
        self.log.error(result)
        return S_ERROR('Could not obtain eligible transformations for status "%s"' % (transStatus))

      if not result['Value']:
        self.log.info('No "%s" transformations of types %s to process.' %
                      (transStatus, string.join(transformationTypes, ', ')))
        continue

      transformationDict.update(result['Value'])

    self.log.info(
        'Selected %s transformations of types %s' %
        (len(
            transformationDict.keys()), string.join(
            transformationTypes, ', ')))
    self.log.verbose('The following transformations were selected out of %s:\n%s' %
                     (string.join(transformationTypes, ', '), string.join(transformationDict.keys(), ', ')))

    trans = []
    #initially this was useful for restricting the considered list
    #now we use the DataRecoveryAgent in setups where IDs are low
    ignoreLessThan = self.ops.getValue("Transformations/IgnoreLessThan", '724')

    if trans:
      self.log.info('Skipping all transformations except %s' % (string.join(trans, ', ')))

    for transformation, typeName in transformationDict.items():
      if trans:
        if transformation not in trans:
          continue
      if ignoreLessThan:
        if int(ignoreLessThan) > int(transformation):
          self.log.verbose(
              'Ignoring transformation %s ( is less than specified limit %s )' %
              (transformation, ignoreLessThan))
          continue

      self.log.info('=' * len('Looking at transformation %s type %s:' % (transformation, typeName)))
      self.log.info('Looking at transformation %s:' % (transformation))

      result = self.selectTransformationFiles(transformation, fileSelectionStatus)
      if not result['OK']:
        self.log.error(result)
        self.log.error('Could not select files for transformation %s' % transformation)
        continue

      if not result['Value']:
        self.log.info('No files in status %s selected for transformation %s' %
                      (string.join(fileSelectionStatus, ', '), transformation))
        continue

      fileDict = result['Value']
      result = self.obtainWMSJobIDs(transformation, fileDict, selectDelay, wmsStatusList)
      if not result['OK']:
        self.log.error(result)
        self.log.error('Could not obtain WMS jobIDs for files of transformation %s' % (transformation))
        continue
      if not result['Value']:
        self.log.info('No eligible WMS jobIDs found for %s files in list:\n%s ...' %
                      (len(fileDict.keys()), fileDict.keys()[0]))
        continue

      jobFileDict = result['Value']
      fileCount = 0
      for lfnList in jobFileDict.values():
        fileCount += len(lfnList)

      if not fileCount:
        self.log.info('No files were selected for transformation %s after examining WMS jobs.' % transformation)
        continue

      self.log.info('%s files are selected after examining related WMS jobs' % (fileCount))
      result = self.checkOutstandingRequests(jobFileDict)
      if not result['OK']:
        self.log.error(result)
        continue

      if not result['Value']:
        self.log.info('No WMS jobs without pending requests to process.')
        continue

      jobFileNoRequestsDict = result['Value']
      fileCount = 0
      for lfnList in jobFileNoRequestsDict.values():
        fileCount += len(lfnList)

      self.log.info('%s files are selected after removing any relating to jobs with pending requests' % (fileCount))
      result = self.checkDescendents(transformation, fileDict, jobFileNoRequestsDict)
      if not result['OK']:
        self.log.error(result)
        continue

      jobsWithFilesOKToUpdate = result['Value']['filesToMarkUnused']
      jobsWithFilesProcessed = result['Value']['filesprocessed']
      self.log.info('====> Transformation %s total files that can be updated now: %s' %
                    (transformation, len(jobsWithFilesOKToUpdate)))

      filesToUpdateUnused = []
      for fileList in jobsWithFilesOKToUpdate:
        filesToUpdateUnused.append(fileList)

      if len(filesToUpdateUnused):
        result = self.updateFileStatus(transformation, filesToUpdateUnused, updateStatus)
        if not result['OK']:
          self.log.error('Recoverable files were not updated with result:\n%s' % (result['Message']))
          continue
      else:
        self.log.info('There are no files with failed jobs to update for production %s in this cycle' % transformation)

      filesToUpdateProcessed = []
      for fileList in jobsWithFilesProcessed:
        filesToUpdateProcessed.append(fileList)

      if len(filesToUpdateProcessed):
        result = self.updateFileStatus(transformation, filesToUpdateProcessed, 'Processed')
        if not result['OK']:
          self.log.error('Recoverable files were not updated with result:\n%s' % (result['Message']))
          continue
      else:
        self.log.info('There are no files processed to update for production %s in this cycle' % transformation)

    return S_OK()

  #############################################################################
  def getEligibleTransformations(self, status, typeList):
    """ Select transformations of given status and type.
    """
    res = self.prodDB.getTransformations(condDict={'Status': status, 'Type': typeList})
    self.log.debug(res)
    if not res['OK']:
      return res
    transformations = {}
    for prod in res['Value']:
      prodID = prod['TransformationID']
      transformations[str(prodID)] = prod['Type']
    return S_OK(transformations)

  #############################################################################
  def selectTransformationFiles(self, transformation, statusList):
    """ Select files, production jobIDs in specified file status for a given transformation.
    """
    #Until a query for files with timestamp can be obtained must rely on the
    #WMS job last update
    res = self.prodDB.getTransformationFiles(condDict={'TransformationID': transformation, 'Status': statusList})
    self.log.debug(res)
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      if not fileDict.has_key('LFN') or not fileDict.has_key(self.taskIDName) or not fileDict.has_key('LastUpdate'):
        self.log.info('LFN, %s and LastUpdate are mandatory, >=1 are missing for:\n%s' % (self.taskIDName, fileDict))
        continue
      lfn = fileDict['LFN']
      jobID = fileDict[self.taskIDName]
      resDict[lfn] = jobID
    if resDict:
      self.log.info('Selected %s files overall for transformation %s' % (len(resDict.keys()), transformation))
    return S_OK(resDict)

  #############################################################################
  def obtainWMSJobIDs(self, transformation, fileDict, selectDelay, wmsStatusList):
    """ Group files by the corresponding WMS jobIDs, check the corresponding
        jobs have not been updated for the delay time.  Can't get into any
        mess because we start from files only in MaxReset / Assigned and check
        corresponding jobs.  Mixtures of files for jobs in MaxReset and Assigned
        statuses only possibly include some files in Unused status (not Processed
        for example) that will not be touched.
    """
    prodJobIDs = uniqueElements(fileDict.values())
    self.log.info('The following %s production jobIDs apply to the selected files:\n%s' % (len(prodJobIDs), prodJobIDs))

    jobFileDict = {}
    condDict = {'TransformationID': transformation, self.taskIDName: prodJobIDs}
    delta = datetime.timedelta(hours=selectDelay)
    now = dateTime()
    olderThan = now - delta

    res = self.prodDB.getTransformationTasks(condDict=condDict, older=olderThan,
                                             timeStamp='LastUpdateTime', inputVector=True)
    self.log.debug(res)
    if not res['OK']:
      self.log.error('getTransformationTasks returned an error:\n%s')
      return res

    for jobDict in res['Value']:
      missingKey = False
      for key in [self.taskIDName, self.externalID, 'LastUpdateTime', self.externalStatus, 'InputVector']:
        if not jobDict.has_key(key):
          self.log.info('Missing key %s for job dictionary, the following is available:\n%s' % (key, jobDict))
          missingKey = True
          continue

      if missingKey:
        continue

      job = jobDict[self.taskIDName]
      wmsID = jobDict[self.externalID]
      lastUpdate = jobDict['LastUpdateTime']
      wmsStatus = jobDict[self.externalStatus]
      jobInputData = jobDict['InputVector']
      jobInputData = [lfn.replace('LFN:', '') for lfn in jobInputData.split(';')]

      if not int(wmsID):
        self.log.info('Prod job %s status is %s (ID = %s) so will not recheck with WMS' % (job, wmsStatus, wmsID))
        continue

      self.log.info(
          'Job %s, prod job %s last update %s, production management system status %s' %
          (wmsID, job, lastUpdate, wmsStatus))
      #Exclude jobs not having appropriate WMS status - have to trust that production management status is correct
      if wmsStatus not in wmsStatusList:
        self.log.info('Job %s is in status %s, not %s so will be ignored' %
                      (wmsID, wmsStatus, string.join(wmsStatusList, ', ')))
        continue

      finalJobData = []
      #Must map unique files -> jobs in expected state
      for lfn, prodID in fileDict.items():
        if int(prodID) == int(job):
          finalJobData.append(lfn)

      self.log.info('Found %s files for job %s' % (len(finalJobData), job))
      jobFileDict[wmsID] = finalJobData

    return S_OK(jobFileDict)

  #############################################################################
  def checkOutstandingRequests(self, jobFileDict):
    """ Before doing anything check that no outstanding requests are pending
        for the set of WMS jobIDs.
    """
    jobs = jobFileDict.keys()
    result = self.requestClient.getRequestForJobs(jobs)
    if not result['OK']:
      return result

    if not result['Value']:
      self.log.info('None of the jobs have pending requests')
      return S_OK(jobFileDict)

    for jobID in result['Value'].keys():
      del jobFileDict[str(jobID)]
      self.log.info('Removing jobID %s from consideration until requests are completed' % (jobID))

    return S_OK(jobFileDict)

  ############################################################################
  def checkDescendents(self, transformation, filedict, jobFileDict):
    """ look that all jobs produced, or not output
    """
    res = self.prodDB.getTransformationParameters(transformation, ['Body'])
    if not res['OK']:
      self.log.error('Could not get Body from TransformationDB')
      return res
    body = res['Value']
    workflow = fromXMLString(body)
    workflow.resolveGlobalVars()

    olist = []
    jtype = workflow.findParameter('JobType')
    if not jtype:
      self.log.error('Type for transformation %d was not defined' % transformation)
      return S_ERROR('Type for transformation %d was not defined' % transformation)
    for step in workflow.step_instances:
      param = step.findParameter('listoutput')
      if not param:
        continue
      olist.extend(param.value)
    expectedlfns = []
    contactfailed = []
    fileprocessed = []
    files = []
    tasks_to_be_checked = {}
    for files in jobFileDict.values():
      for f in files:
        if f in filedict:
          tasks_to_be_checked[f] = filedict[f]  # get the tasks that need to be checked
    for filep, task in tasks_to_be_checked.items():
      commons = {}
      commons['outputList'] = olist
      commons['PRODUCTION_ID'] = transformation
      commons['JOB_ID'] = task
      commons['JobType'] = jtype
      out = constructProductionLFNs(commons)
      expectedlfns = out['Value']['ProductionOutputData']
      res = self.replicaManager.getCatalogFileMetadata(expectedlfns)
      if not res['OK']:
        self.log.error('Getting metadata failed')
        contactfailed.append(filep)
        continue
      if filep not in files:
        files.append(filep)
      success = res['Value']['Successful'].keys()
      failed = res['Value']['Failed'].keys()
      if len(success) and not len(failed):
        fileprocessed.append(filep)

    final_list_unused = files
    for file_all in files:
      if file_all in fileprocessed:
        try:
          final_list_unused.remove(filep)
        except BaseException:
          self.log.warn("Item not in list anymore")

    result = {'filesprocessed': fileprocessed, 'filesToMarkUnused': final_list_unused}
    return S_OK(result)

  #############################################################################
  def updateFileStatus(self, transformation, fileList, fileStatus):
    """ Update file list to specified status.
    """
    if not self.enableFlag:
      self.log.info(
          'Enable flag is False, would update  %s files to "%s" status for %s' %
          (len(fileList), fileStatus, transformation))
      return S_OK()

    self.log.info('Updating %s files to "%s" status for %s' % (len(fileList), fileStatus, transformation))
    result = self.prodDB.setFileStatusForTransformation(int(transformation), fileStatus, fileList, force=True)
    self.log.debug(result)
    if not result['OK']:
      self.log.error(result)
      return result
    if result['Value']['Failed']:
      self.log.error(result['Value']['Failed'])
      return result

    msg = result['Value']['Successful']
    for lfn, message in msg.items():
      self.log.info('%s => %s' % (lfn, message))

    return S_OK()
