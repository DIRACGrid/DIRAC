"""TransformationPlugin is a class wrapping the supported transformation plugins

New plugins can be created by defining a function.
The function name has to be ``_MyPlugin``, and the plugin is then call ``'MyPlugin'``. Do not forget to enable new
plugins in the ``Operations/Transformation/AllowedPlugins`` list.
The return value of the function has to be ``S_OK`` with a list of tuples. Each Tuple is a pair of a StorageElement
and list of LFNs to treat in given task::

  return S_OK([('SE_1', [lfn_1_1, lfn_1_2, ...]),
               ('SE_1', [lfn_2_1, lfn_2_2, ...]),
               # ...
               ('SE_I', [lfn_J_1, lfn_J_2, ...]),
               ])

Inside the plugin function, the relevant LFNs can be accessed in the ``self.data`` dictionary, and transformation
parameters are obtained contained in the ``self.params`` dictionary. See also the
:class:`~DIRAC.TransformationSystem.Client.Utilities.PluginUtilities` class.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import time

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE, getSEsForSite
from DIRAC.Core.Utilities.List import breakListIntoChunks

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.TransformationSystem.Client.PluginBase import PluginBase
from DIRAC.TransformationSystem.Client.Utilities import PluginUtilities, getFileGroups
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

__RCSID__ = "$Id$"


class TransformationPlugin(PluginBase):
  """ A TransformationPlugin object should be instantiated by every transformation.

  :param str plugin: A plugin name has to be passed in: it will then be executed as one of the functions below, e.g.
     plugin = 'BySize' will execute TransformationPlugin('BySize')._BySize()

  :param transClient: TransformationManagerClient instance
  :param dataManager: DataManager instance
  :param fc: FileCatalog instance
  """

  def __init__(self, plugin, transClient=None, dataManager=None, fc=None):
    """Constructor of the TransformationPlugin.

    Instantiate clients, if not given, and set up the PluginUtilities.
    """
    super(TransformationPlugin, self).__init__(plugin)

    self.data = {}
    self.files = False
    self.startTime = time.time()
    self.valid = False

    if transClient is None:
      transClient = TransformationClient()

    if dataManager is None:
      dataManager = DataManager()

    if fc is None:
      fc = FileCatalog()

    self.util = PluginUtilities(plugin,
                                transClient=transClient,
                                dataManager=dataManager,
                                fc=fc)

  def __del__(self):
    """ Destructor: print the elapsed time """
    self.util.logInfo("Execution finished, timing: %.3f seconds" % (time.time() - self.startTime))

  def isOK(self):
    """ Check if all information is present """
    self.valid = True
    if (not self.data) or (not self.params):
      self.valid = False
    return self.valid

  def setParameters(self, params):
    """ Need to pass parameters also to self.util
    """
    self.params = params
    self.util.setParameters(params)

  def setInputData(self, data):
    """ Set the replica information as data member """
    self.data = data

  def setTransformationFiles(self, files):
    """ Set the TS files as data member """
    self.files = files

  def _Standard(self):
    """ Simply group by replica location (if any)
    """
    return self.util.groupByReplicas(self.data, self.params['Status'])

  def _BySize(self):
    """ Alias for groupBySize
    """
    return self._groupBySize()

  def _groupBySize(self, files=None):
    """
    Generate a task for a given amount of data at a (set of) SE
    """
    if not files:
      files = self.data
    else:
      files = dict(zip(files, [self.data[lfn] for lfn in files]))
    return self.util.groupBySize(files, self.params['Status'])

  def _Broadcast(self):
    """ This plug-in takes files found at the sourceSE and broadcasts to all (or a selection of) targetSEs.

    Parameters used by this plugin:

    * SourceSE: Optional: only files at this location are treated
    * TargetSE: Where to broadcast files to
    * Destinations: Optional: integer, files are only broadcast to this number of TargetSEs, Destinations has to be
      larger than the number of TargetSEs
    * GroupSize: number of files per task
    """
    if not self.params:
      return S_ERROR("TransformationPlugin._Broadcast: The 'Broadcast' plugin requires additional parameters.")

    sourceSEs = set(self.util.seParamtoList(self.params.get('SourceSE', [])))
    targetSEs = self.util.seParamtoList(self.params['TargetSE'])
    destinations = int(self.params.get('Destinations', 0))
    if destinations and (destinations >= len(targetSEs)):
      destinations = 0

    status = self.params['Status']
    groupSize = self.params['GroupSize']  # Number of files per tasks

    fileGroups = getFileGroups(self.data)  # groups by SE
    targetSELfns = {}
    for replicaSE, lfns in fileGroups.items():
      ses = replicaSE.split(',')
      atSource = (not sourceSEs) or set(ses).intersection(sourceSEs)
      if not atSource:
        continue

      for lfn in lfns:
        targets = []
        sourceSites = self._getSitesForSEs(ses)
        random.shuffle(targetSEs)
        for targetSE in targetSEs:
          site = self._getSiteForSE(targetSE)['Value']
          if site not in sourceSites:
            if (destinations) and (len(targets) >= destinations):
              continue
            sourceSites.append(site)
          targets.append(targetSE)  # after all, if someone wants to copy to the source, it's his choice
        strTargetSEs = ','.join(sorted(targets))
        targetSELfns.setdefault(strTargetSEs, []).append(lfn)
    tasks = []
    for ses, lfns in targetSELfns.items():
      tasksLfns = breakListIntoChunks(lfns, groupSize)
      for taskLfns in tasksLfns:
        if (status == 'Flush') or (len(taskLfns) >= int(groupSize)):
          # do not allow groups smaller than the groupSize, except if transformation is in flush state
          tasks.append((ses, taskLfns))
    return S_OK(tasks)

  def _ByShare(self, shareType='CPU'):
    """ first get the shares from the CS, and then makes the grouping looking at the history
    """
    res = self._getShares(shareType, normalise=True)
    if not res['OK']:
      return res
    cpuShares = res['Value']
    self.util.logInfo("Obtained the following target shares (%):")
    for site in sorted(cpuShares):
      self.util.logInfo("%s: %.1f" % (site.ljust(15), cpuShares[site]))

    # Get the existing destinations from the transformationDB
    res = self.util.getExistingCounters(requestedSites=list(cpuShares))
    if not res['OK']:
      self.util.logError("Failed to get existing file share", res['Message'])
      return res
    existingCount = res['Value']
    if existingCount:
      self.util.logInfo("Existing site utilization (%):")
      normalisedExistingCount = self.util._normaliseShares(existingCount.copy())  # pylint: disable=protected-access
      for se in sorted(normalisedExistingCount):
        self.util.logInfo("%s: %.1f" % (se.ljust(15), normalisedExistingCount[se]))

    # Group the input files by their existing replicas
    res = self.util.groupByReplicas(self.data, self.params['Status'])
    if not res['OK']:
      return res
    replicaGroups = res['Value']

    tasks = []
    # For the replica groups
    for replicaSE, lfns in replicaGroups:
      possibleSEs = replicaSE.split(',')
      # Determine the next site based on requested shares, existing usage and candidate sites
      res = self._getNextSite(existingCount, cpuShares, candidates=self._getSitesForSEs(possibleSEs))
      if not res['OK']:
        self.util.logError("Failed to get next destination SE", res['Message'])
        continue
      targetSite = res['Value']
      # Resolve the ses for the target site
      res = getSEsForSite(targetSite)
      if not res['OK']:
        continue
      ses = res['Value']
      # Determine the selected SE and create the task
      for chosenSE in ses:
        if chosenSE in possibleSEs:
          tasks.append((chosenSE, lfns))
          existingCount[targetSite] = existingCount.setdefault(targetSite, 0) + len(lfns)
    return S_OK(tasks)

  def _getShares(self, shareType, normalise=False):
    """ Takes share from the CS, eventually normalize them
    """
    res = gConfig.getOptionsDict('/Resources/Shares/%s' % shareType)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("/Resources/Shares/%s option contains no shares" % shareType)
    shares = res['Value']
    for site, value in shares.items():
      shares[site] = float(value)
    if normalise:
      shares = self.util._normaliseShares(shares)  # pylint: disable=protected-access
    if not shares:
      return S_ERROR("No non-zero shares defined")
    return S_OK(shares)

  def _getNextSite(self, existingCount, targetShares, candidates=None):
    """
    Selects the most suitable site, i.e. further to targetShares in existingCount
    """
    if candidates is None:
      candidates = targetShares
    # normalise the existing counts
    existingShares = self.util._normaliseShares(existingCount)  # pylint: disable=protected-access
    # then fill the missing share values to 0
    for site in targetShares:
      existingShares.setdefault(site, 0.0)
    # determine which site is farthest from its share
    chosenSite = ''
    minShareShortFall = -float("inf")
    for site, targetShare in targetShares.items():
      if site not in candidates or not targetShare:
        continue
      existingShare = existingShares[site]
      shareShortFall = targetShare - existingShare
      if shareShortFall > minShareShortFall:
        minShareShortFall = shareShortFall
        chosenSite = site
    return S_OK(chosenSite)

  @classmethod
  def _getSiteForSE(cls, se):
    """ Get site name for the given SE
    """
    result = getSitesForSE(se)
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK(result['Value'][0])
    return S_OK('')

  @classmethod
  def _getSitesForSEs(cls, seList):
    """ Get all the sites for the given SE list
    """
    sites = []
    for se in seList:
      result = getSitesForSE(se)
      if result['OK']:
        sites += result['Value']
    return sites
