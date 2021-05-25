########################################################################
# File : Utilities.py
# Author : Federico Stagni
########################################################################

"""
Utilities for Transformation system
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ast
import random

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
# from DIRAC.Core.Utilities.Time import timeThis
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

__RCSID__ = "$Id$"


class PluginUtilities(object):
  """
  Utility class used by plugins
  """

  def __init__(self, plugin='Standard', transClient=None, dataManager=None, fc=None,
               debug=False, transID=None):
    """
    c'tor

    Setting defaults
    """
    # clients
    if transClient is None:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient
    if dataManager is None:
      self.dm = DataManager()
    else:
      self.dm = dataManager
    if fc is None:
      self.fc = FileCatalog()
    else:
      self.fc = fc

    self.dmsHelper = DMSHelpers()

    self.plugin = plugin
    self.transID = str(transID) if transID else 'None'
    self.params = {}
    self.groupSize = 0
    self.maxFiles = 0
    self.cachedLFNSize = {}
    self.transString = ''
    self.debug = debug

    self.log = gLogger.getSubLogger(self.plugin + self.transID)
    # FIXME: This doesn't work (yet) but should soon, will allow scripts to get the context
    self.log.showHeaders(True)

  def logVerbose(self, message, param=''):
    """ logger helper """
    if self.debug:
      log = gLogger.getSubLogger(self.plugin + ' (V)' + self.transID)
      log.info(message, param)
    else:
      self.log.verbose(message, param)

  def logDebug(self, message, param=''):
    """ logger helper """
    self.log.debug(message, param)

  def logInfo(self, message, param=''):
    """ logger helper """
    self.log.info(message, param)

  def logWarn(self, message, param=''):
    """ logger helper """
    self.log.warn(message, param)

  def logError(self, message, param=''):
    """ logger helper """
    self.log.error(message, param)

  def logException(self, message, param='', lException=False):
    """ logger helper """
    self.log.exception(message, param, lException)

  def setParameters(self, params):
    """ Set the transformation parameters and extract transID """
    self.params = params
    self.transID = str(params['TransformationID'])
    self.log = gLogger.getSubLogger(self.plugin + self.transID)

  # @timeThis
  def groupByReplicas(self, files, status):
    """
    Generates tasks based on the location of the input data

   :param dict fileReplicas:
              {'/this/is/at.1': ['SE1'],
               '/this/is/at.12': ['SE1', 'SE2'],
               '/this/is/at.2': ['SE2'],
               '/this/is/at_123': ['SE1', 'SE2', 'SE3'],
               '/this/is/at_23': ['SE2', 'SE3'],
               '/this/is/at_4': ['SE4']}

    """
    tasks = []
    nTasks = 0

    if not files:
      return S_OK(tasks)

    files = dict(files)

    # Parameters
    if not self.groupSize:
      self.groupSize = self.getPluginParam('GroupSize', 10)
    flush = (status == 'Flush')
    self.logVerbose(
        "groupByReplicas: %d files, groupSize %d, flush %s" %
        (len(files), self.groupSize, flush))

    # Consider files by groups of SEs, a file is only in one group
    # Then consider files site by site, but a file can now be at more than one site
    for groupSE in (True, False):
      if not files:
        break
      seFiles = getFileGroups(files, groupSE=groupSE)
      self.logDebug("fileGroups set: ", seFiles)

      for replicaSE in sortSEs(seFiles):
        lfns = seFiles[replicaSE]
        if lfns:
          tasksLfns = breakListIntoChunks(lfns, self.groupSize)
          lfnsInTasks = []
          for taskLfns in tasksLfns:
            if flush or (len(taskLfns) >= self.groupSize):
              tasks.append((replicaSE, taskLfns))
              lfnsInTasks += taskLfns
          # In case the file was at more than one site, remove it from the other sites' list
          # Remove files from global list
          for lfn in lfnsInTasks:
            files.pop(lfn)
          if not groupSE:
            # Remove files from other SEs
            for se in [se for se in seFiles if se != replicaSE]:
              seFiles[se] = [lfn for lfn in seFiles[se] if lfn not in lfnsInTasks]
      self.logVerbose(
          "groupByReplicas: %d tasks created (groupSE %s)" %
          (len(tasks) - nTasks, str(groupSE)), "%d files not included in tasks" %
          len(files))
      nTasks = len(tasks)

    return S_OK(tasks)

  def createTasksBySize(self, lfns, replicaSE, fileSizes=None, flush=False):
    """
    Split files in groups according to the size and create tasks for a given SE
    """
    tasks = []
    if fileSizes is None:
      fileSizes = self._getFileSize(lfns).get('Value')
    if fileSizes is None:
      self.logWarn('Error getting file sizes, no tasks created')
      return tasks
    taskLfns = []
    taskSize = 0
    if not self.groupSize:
      # input size in GB converted to bytes
      self.groupSize = float(self.getPluginParam('GroupSize', 1.)) * 1000 * 1000 * 1000
    if not self.maxFiles:
      # FIXME: prepare for chaging the name of the ambiguoug  CS option
      self.maxFiles = self.getPluginParam('MaxFilesPerTask', self.getPluginParam('MaxFiles', 100))
    lfns = sorted(lfns, key=fileSizes.get)
    for lfn in lfns:
      size = fileSizes.get(lfn, 0)
      if size:
        if size > self.groupSize:
          tasks.append((replicaSE, [lfn]))
        else:
          taskSize += size
          taskLfns.append(lfn)
          if (taskSize > self.groupSize) or (len(taskLfns) >= self.maxFiles):
            tasks.append((replicaSE, taskLfns))
            taskLfns = []
            taskSize = 0
    if flush and taskLfns:
      tasks.append((replicaSE, taskLfns))
    if not tasks and not flush and taskLfns:
      self.logVerbose(
          'Not enough data to create a task, and flush not set (%d bytes for groupSize %d)' %
          (taskSize, self.groupSize))
    return tasks

  # @timeThis
  def groupBySize(self, files, status):
    """
    Generate a task for a given amount of data
    """
    tasks = []
    nTasks = 0

    if not len(files):
      return S_OK(tasks)

    files = dict(files)
    # Parameters
    if not self.groupSize:
      # input size in GB converted to bytes
      self.groupSize = float(self.getPluginParam('GroupSize', 1)) * 1000 * 1000 * 1000
    flush = (status == 'Flush')
    self.logVerbose(
        "groupBySize: %d files, groupSize: %d, flush: %s" %
        (len(files), self.groupSize, flush))

    # Get the file sizes
    res = self._getFileSize(list(files))
    if not res['OK']:
      return res
    fileSizes = res['Value']

    for groupSE in (True, False):
      if not files:
        break
      seFiles = getFileGroups(files, groupSE=groupSE)

      for replicaSE in sorted(seFiles) if groupSE else sortSEs(seFiles):
        lfns = seFiles[replicaSE]
        newTasks = self.createTasksBySize(lfns, replicaSE, fileSizes=fileSizes, flush=flush)
        lfnsInTasks = []
        for task in newTasks:
          lfnsInTasks += task[1]
        tasks += newTasks

        # Remove the selected files from the size cache
        self.clearCachedFileSize(lfnsInTasks)
        if not groupSE:
          # Remove files from other SEs
          for se in [se for se in seFiles if se != replicaSE]:
            seFiles[se] = [lfn for lfn in seFiles[se] if lfn not in lfnsInTasks]
        # Remove files from global list
        for lfn in lfnsInTasks:
          files.pop(lfn)

      self.logVerbose(
          "groupBySize: %d tasks created with groupSE %s" %
          (len(tasks) - nTasks, str(groupSE)))
      self.logVerbose("groupBySize: %d files have not been included in tasks" % len(files))
      nTasks = len(tasks)

    self.logVerbose("Grouped %d files by size" % len(files))
    return S_OK(tasks)

  def getExistingCounters(self, normalise=False, requestedSites=[]):
    res = self.transClient.getCounters('TransformationFiles', ['UsedSE'],
                                       {'TransformationID': self.params['TransformationID']})
    if not res['OK']:
      return res
    usageDict = {}
    for usedDict, count in res['Value']:
      usedSE = usedDict['UsedSE']
      if usedSE != 'Unknown':
        usageDict[usedSE] = count
    if requestedSites:
      siteDict = {}
      for se, count in usageDict.items():
        res = getSitesForSE(se)
        if not res['OK']:
          return res
        for site in res['Value']:
          if site in requestedSites:
            siteDict[site] = count
      usageDict = siteDict.copy()
    if normalise:
      usageDict = self._normaliseShares(usageDict)
    return S_OK(usageDict)

  # @timeThis
  def _getFileSize(self, lfns):
    """ Get file size from a cache, if not from the catalog
    #FIXME: have to fill the cachedLFNSize!
    """
    lfns = list(lfns)
    cachedLFNSize = dict(self.cachedLFNSize)

    fileSizes = {}
    for lfn in [lfn for lfn in lfns if lfn in cachedLFNSize]:
      fileSizes[lfn] = cachedLFNSize[lfn]
    self.logDebug(
        "Found cache hit for File size for %d files out of %d" %
        (len(fileSizes), len(lfns)))
    lfns = [lfn for lfn in lfns if lfn not in cachedLFNSize]
    if lfns:
      fileSizes = self._getFileSizeFromCatalog(lfns, fileSizes)
      if not fileSizes['OK']:
        self.logError(fileSizes['Message'])
        return fileSizes
      fileSizes = fileSizes['Value']
    return S_OK(fileSizes)

  # @timeThis
  def _getFileSizeFromCatalog(self, lfns, fileSizes):
    """
    Get file size from the catalog
    """
    lfns = list(lfns)
    fileSizes = dict(fileSizes)

    res = self.fc.getFileSize(lfns)
    if not res['OK']:
      return S_ERROR("Failed to get sizes for all files: %s" % res['Message'])
    if res['Value']['Failed']:
      errorReason = sorted(set(res['Value']['Failed'].values()))
      self.logWarn("Failed to get sizes for %d files:" % len(res['Value']['Failed']), errorReason)
    fileSizes.update(res['Value']['Successful'])
    self.cachedLFNSize.update((res['Value']['Successful']))
    self.logVerbose("Got size of %d files from catalog" % len(lfns))
    return S_OK(fileSizes)

  def clearCachedFileSize(self, lfns):
    """ Utility function
    """
    for lfn in [lfn for lfn in lfns if lfn in self.cachedLFNSize]:
      self.cachedLFNSize.pop(lfn)

  def getPluginParam(self, name, default=None):
    """ Get plugin parameters using specific settings or settings defined in the CS
        Caution: the type returned is that of the default value
    """
    # get the value of a parameter looking 1st in the CS
    if default is not None:
      valueType = type(default)
    else:
      valueType = None
    # First look at a generic value...
    optionPath = "TransformationPlugins/%s" % (name)
    value = Operations().getValue(optionPath, None)
    self.logVerbose("Default plugin param %s: '%s'" % (optionPath, value))
    # Then look at a plugin-specific value
    optionPath = "TransformationPlugins/%s/%s" % (self.plugin, name)
    value = Operations().getValue(optionPath, value)
    self.logVerbose("Specific plugin param %s: '%s'" % (optionPath, value))
    if value is not None:
      default = value
    # Finally look at a transformation-specific parameter
    value = self.params.get(name, default)
    self.logVerbose(
        "Transformation plugin param %s: '%s'. Convert to %s" %
        (name, value, str(valueType)))
    if valueType and not isinstance(value, valueType):
      if valueType is list:
        try:
          value = ast.literal_eval(value) if value and value != 'None' else []
        # literal_eval('SE-DST') -> ValueError
        # literal_eval('SE_MC-DST') -> SyntaxError
        # Don't ask...
        except (ValueError, SyntaxError):
          value = [val for val in value.replace(' ', '').split(',') if val]

      elif valueType is int:
        value = int(value)
      elif valueType is float:
        value = float(value)
      elif valueType is bool:
        if value in ('False', 'No', 'None', None, 0):
          value = False
        else:
          value = bool(value)
      elif valueType is not str:
        self.logWarn(
            "Unknown parameter type (%s) for %s, passed as string" %
            (str(valueType), name))
    self.logVerbose("Final plugin param %s: '%s'" % (name, value))
    return value

  @staticmethod
  def _normaliseShares(originalShares):
    """ Normalize shares to 1 """
    total = sum(float(share) for share in originalShares.values())
    return dict([(site, 100. * float(share) / total if total else 0.)
                 for site, share in originalShares.items()])

  def uniqueSEs(self, ses):
    """ return a list of SEs that are not physically the same """
    newSEs = []
    for se in ses:
      if not self.isSameSEInList(se, newSEs):
        newSEs.append(se)
    return newSEs

  def isSameSE(self, se1, se2):
    """ Check if 2 SEs are indeed the same.

        :param se1: name of the first StorageElement
        :param se2: name of the second StorageElement

        :returns: True/False if they are considered the same.
                  See :py:mod:`~DIRAC.Resources.Storage.StorageElement.StorageElementItem.isSameSE`
    """
    if se1 == se2:
      return True

    return StorageElement(se1).isSameSE(StorageElement(se2))

  def isSameSEInList(self, se1, seList):
    """ Check if an SE is the same as any in a list """
    if se1 in seList:
      return True
    for se in seList:
      if self.isSameSE(se1, se):
        return True
    return False

  def closerSEs(self, existingSEs, targetSEs, local=False):
    """ Order the targetSEs such that the first ones are closer to existingSEs. Keep all elements in targetSEs
    """
    setTarget = set(targetSEs)
    sameSEs = set([se1 for se1 in setTarget for se2 in existingSEs if self.isSameSE(se1, se2)])
    targetSEs = setTarget - set(sameSEs)
    if targetSEs:
      # Some SEs are left, look for sites
      existingSites = [self.dmsHelper.getLocalSiteForSE(se).get('Value')
                       for se in existingSEs]
      existingSites = set([site for site in existingSites if site])
      closeSEs = set([se for se in targetSEs
                      if self.dmsHelper.getLocalSiteForSE(se).get('Value') in existingSites])
      # print existingSEs, existingSites, targetSEs, closeSEs
      otherSEs = targetSEs - closeSEs
      targetSEs = list(closeSEs)
      random.shuffle(targetSEs)
      if not local and otherSEs:
        otherSEs = list(otherSEs)
        random.shuffle(otherSEs)
        targetSEs += otherSEs
    else:
      targetSEs = []
    return (targetSEs + list(sameSEs)) if not local else targetSEs

  @staticmethod
  def seParamtoList(inputParam):
    """Transform ``inputParam`` to list.

    :param inputParam: can be string, list, or string representation of list
    :returns: list
    """
    if not inputParam:
      return []
    if inputParam.count('['):
      return eval(inputParam)  # pylint: disable=eval-used
    elif isinstance(inputParam, list):
      return inputParam
    return [inputParam]


def getFileGroups(fileReplicas, groupSE=True):
  """
  Group files by set of SEs

  :param dict fileReplicas:
              {'/this/is/at.1': ['SE1'],
               '/this/is/at.12': ['SE1', 'SE2'],
               '/this/is/at.2': ['SE2'],
               '/this/is/at_123': ['SE1', 'SE2', 'SE3'],
               '/this/is/at_23': ['SE2', 'SE3'],
               '/this/is/at_4': ['SE4']}

  If groupSE == False, group by SE, in which case a file can be in more than one element
  """
  fileGroups = {}
  for lfn, replicas in fileReplicas.items():
    if not replicas:
      continue
    replicas = sorted(list(set(replicas)))
    if not groupSE or len(replicas) == 1:
      for rep in replicas:
        fileGroups.setdefault(rep, []).append(lfn)
    else:
      replicaSEs = ','.join(replicas)
      fileGroups.setdefault(replicaSEs, []).append(lfn)
  return fileGroups


def sortSEs(ses):
  """ Returnes an ordered list of SEs, disk first """
  seSvcClass = {}
  for se in ses:
    if len(se.split(',')) != 1:
      return sorted(ses)
    if se not in seSvcClass:
      seSvcClass[se] = StorageElement(se).status()['DiskSE']
  diskSEs = [se for se in ses if seSvcClass[se]]
  tapeSEs = [se for se in ses if se not in diskSEs]
  return sorted(diskSEs) + sorted(tapeSEs)


def sortExistingSEs(lfnSEs, lfns=None):
  """ Sort SEs according to the number of files in each (most first)
  """
  seFrequency = {}
  archiveSEs = []
  if not lfns:
    lfns = list(lfnSEs)
  else:
    lfns = [lfn for lfn in lfns if lfn in lfnSEs]
  for lfn in lfns:
    existingSEs = lfnSEs[lfn]
    archiveSEs += [s for s in existingSEs if isArchive(s) and s not in archiveSEs]
    for se in [s for s in existingSEs if not isFailover(s) and s not in archiveSEs]:
      seFrequency[se] = seFrequency.setdefault(se, 0) + 1
  sortedSEs = list(seFrequency)
  # sort SEs in reverse order of frequency
  sortedSEs.sort(key=seFrequency.get, reverse=True)
  # add the archive SEs at the end
  return sortedSEs + archiveSEs


def isArchive(se):
  """ Is the SE an archive """
  return DMSHelpers().isSEArchive(se)


def isFailover(se):
  """ Is the SE a failover SE """
  return DMSHelpers().isSEFailover(se)


def getActiveSEs(seList, access='Write'):
  """ Utility function - uses the StorageElement cached status
  """
  return [se for se in seList if StorageElement(se).status().get(access, False)]
