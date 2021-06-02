########################################################################
# File :    CPUNormalization.py
# Author :  Ricardo Graciani
########################################################################

""" DIRAC Workload Management System Client module that encapsulates all the
    methods necessary to handle CPU normalization
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from six.moves.urllib.request import urlopen

import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft
from DIRAC.WorkloadManagementSystem.Client.DIRACbenchmark import singleDiracBenchmark

__RCSID__ = "$Id$"

# TODO: This should come from some place in the configuration
NORMALIZATIONCONSTANT = 60. / 250.  # from minutes to seconds and from SI00 to HS06 (ie min * SI00 -> sec * HS06 )

UNITS = {'HS06': 1., 'SI00': 1. / 250.}

# TODO: This is still fetching directly from MJF rather than going through
# the MJF module and the values it saves in the local DIRAC configuration


def __getFeatures(envVariable, items):
  """ Extract features """
  features = {}
  featuresDir = os.environ.get(envVariable)
  if featuresDir is None:
    return features
  for item in items:
    fname = os.path.join(featuresDir, item)
    try:
      # Only keep features that do exist
      features[item] = urlopen(fname).read()
    except IOError:
      pass
  return features


def getMachineFeatures():
  """ This uses the _old_ MJF information """
  return __getFeatures("MACHINEFEATURES", ('hs06', 'jobslots', 'log_cores', 'phys_cores'))
# TODO: log_cores and phys_cores are deprecated and from old MJF specificationa and not collected
# by the MJF module!


def getJobFeatures():
  """ This uses the _new_ MJF information """
  return __getFeatures("JOBFEATURES", ('hs06_job', 'allocated_cpu'))


def getPowerFromMJF():
  """ Extracts the machine power from either JOBFEATURES or MACHINEFEATURES """
  try:
    features = getJobFeatures()
    hs06Job = features.get('hs06_job')
    # If the information is there and non zero, return, otherwise go to machine features
    if hs06Job:
      return round(float(hs06Job), 2)
    features = getMachineFeatures()
    totalPower = float(features.get('hs06', 0))
    logCores = float(features.get('log_cores', 0))
    physCores = float(features.get('phys_cores', 0))
    jobSlots = float(features.get('jobslots', 0))
    denom = min(max(logCores, physCores), jobSlots) if (logCores or physCores) and jobSlots else None
    if totalPower and denom:
      return round(totalPower / denom, 2)
    return None
  except ValueError as e:
    gLogger.exception("Exception getting MJF information", lException=e)
    return None


def queueNormalizedCPU(ceUniqueID):
  """ Report Normalized CPU length of queue
  """
  result = getQueueInfo(ceUniqueID)
  if not result['OK']:
    return result

  ceInfoDict = result['Value']
  siteCSSEction = ceInfoDict['SiteCSSEction']
  queueCSSection = ceInfoDict['QueueCSSection']

  benchmarkSI00 = __getQueueNormalization(queueCSSection, siteCSSEction)
  maxCPUTime = __getMaxCPUTime(queueCSSection)

  if maxCPUTime and benchmarkSI00:
    normCPUTime = NORMALIZATIONCONSTANT * maxCPUTime * benchmarkSI00
  else:
    if not benchmarkSI00:
      subClusterUniqueID = ceInfoDict['SubClusterUniqueID']
      return S_ERROR('benchmarkSI00 info not available for %s' % subClusterUniqueID)
    if not maxCPUTime:
      return S_ERROR('maxCPUTime info not available')

  return S_OK(normCPUTime)


def getQueueNormalization(ceUniqueID):
  """ Report Normalization Factor applied by Site to the given Queue
  """
  result = getQueueInfo(ceUniqueID)
  if not result['OK']:
    return result

  ceInfoDict = result['Value']
  siteCSSEction = ceInfoDict['SiteCSSEction']
  queueCSSection = ceInfoDict['QueueCSSection']
  subClusterUniqueID = ceInfoDict['SubClusterUniqueID']

  benchmarkSI00 = __getQueueNormalization(queueCSSection, siteCSSEction)

  if benchmarkSI00:
    return S_OK(benchmarkSI00)
  return S_ERROR('benchmarkSI00 info not available for %s' % subClusterUniqueID)
  # errorList.append( ( subClusterUniqueID , 'benchmarkSI00 info not available' ) )
  # exitCode = 3


def __getQueueNormalization(queueCSSection, siteCSSEction):
  """ Query the CS and return the Normalization
  """
  benchmarkSI00Option = '%s/%s' % (queueCSSection, 'SI00')
  benchmarkSI00 = gConfig.getValue(benchmarkSI00Option, 0.0)
  if not benchmarkSI00:
    benchmarkSI00Option = '%s/%s' % (siteCSSEction, 'SI00')
    benchmarkSI00 = gConfig.getValue(benchmarkSI00Option, 0.0)

  return benchmarkSI00


def __getMaxCPUTime(queueCSSection):
  """ Query the CS and return the maxCPUTime
  """
  maxCPUTimeOption = '%s/%s' % (queueCSSection, 'maxCPUTime')
  maxCPUTime = gConfig.getValue(maxCPUTimeOption, 0.0)
  # For some sites there are crazy values in the CS
  maxCPUTime = max(maxCPUTime, 0)
  maxCPUTime = min(maxCPUTime, 86400 * 12.5)

  return maxCPUTime


def getCPUNormalization(reference='HS06', iterations=1):
  """ Get Normalized Power of the current CPU in [reference] units
  """
  if reference not in UNITS:
    return S_ERROR('Unknown Normalization unit %s' % str(reference))
  try:
    max(min(int(iterations), 10), 1)
  except (TypeError, ValueError) as x:
    return S_ERROR(x)

  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  corr = Operations().getValue('JobScheduling/CPUNormalizationCorrection', 1.)

  result = singleDiracBenchmark(iterations)

  if result is None:
    return S_ERROR('Cannot get benchmark measurements')

  return S_OK({'CPU': result['CPU'],
               'WALL': result['WALL'],
               'NORM': result['NORM'] / corr,
               'UNIT': reference})


def getCPUTime(cpuNormalizationFactor):
  """ Trying to get CPUTime left for execution (in seconds).

      It will first look to get the work left looking for batch system information useing the TimeLeft utility.
      If it succeeds, it will convert it in real second, and return it.

      If it fails, it tries to get it from the static info found in CS.
      If it fails, it returns the default, which is a large 9999999, that we may consider as "Infinite".

      This is a generic method, independent from the middleware of the resource if TimeLeft doesn't return a value

      args:
        cpuNormalizationFactor (float): the CPU power of the current Worker Node.
        If not passed in, it's get from the local configuration

      returns:
        cpuTimeLeft (int): the CPU time left, in seconds
  """
  cpuTimeLeft = 0.
  cpuWorkLeft = gConfig.getValue('/LocalSite/CPUTimeLeft', 0)

  if not cpuWorkLeft:
    # Try and get the information from the CPU left utility
    result = TimeLeft().getTimeLeft()
    if result['OK']:
      cpuWorkLeft = result['Value']

  if cpuWorkLeft > 0:
    # This is in HS06sseconds
    # We need to convert in real seconds
    if not cpuNormalizationFactor:  # if cpuNormalizationFactor passed in is 0, try get it from the local cfg
      cpuNormalizationFactor = gConfig.getValue('/LocalSite/CPUNormalizationFactor', 0.0)
    if cpuNormalizationFactor:
      cpuTimeLeft = cpuWorkLeft / cpuNormalizationFactor

  if not cpuTimeLeft:
    # now we know that we have to find the CPUTimeLeft by looking in the CS
    # this is not granted to be correct as the CS units may not be real seconds
    gridCE = gConfig.getValue('/LocalSite/GridCE')
    ceQueue = gConfig.getValue('/LocalSite/CEQueue')
    if not ceQueue:
      # we have to look for a ceQueue in the CS
      # A bit hacky. We should better profit from something generic
      gLogger.warn("No CEQueue in local configuration, looking to find one in CS")
      siteName = DIRAC.siteName()
      queueSection = '/Resources/Sites/%s/%s/CEs/%s/Queues' % (siteName.split('.')[0], siteName, gridCE)
      res = gConfig.getSections(queueSection)
      if not res['OK']:
        raise RuntimeError(res['Message'])
      queues = res['Value']
      cpuTimes = [gConfig.getValue(queueSection + '/' + queue + '/maxCPUTime', 9999999.) for queue in queues]
      # These are (real, wall clock) minutes - damn BDII!
      cpuTimeLeft = min(cpuTimes) * 60
    else:
      queueInfo = getQueueInfo('%s/%s' % (gridCE, ceQueue))
      cpuTimeLeft = 9999999.
      if not queueInfo['OK'] or not queueInfo['Value']:
        gLogger.warn("Can't find a CE/queue, defaulting CPUTime to %d" % cpuTimeLeft)
      else:
        queueCSSection = queueInfo['Value']['QueueCSSection']
        # These are (real, wall clock) minutes - damn BDII!
        cpuTimeInMinutes = gConfig.getValue('%s/maxCPUTime' % queueCSSection, 0.)
        if cpuTimeInMinutes:
          cpuTimeLeft = cpuTimeInMinutes * 60.
          gLogger.info("CPUTime for %s: %f" % (queueCSSection, cpuTimeLeft))
        else:
          gLogger.warn("Can't find maxCPUTime for %s, defaulting CPUTime to %f" % (queueCSSection, cpuTimeLeft))

  return int(cpuTimeLeft)


def getQueueInfo(ceUniqueID, diracSiteName=''):
  """
    Extract information from full CE Name including associate DIRAC Site
  """
  try:
    subClusterUniqueID = ceUniqueID.split('/')[0].split(':')[0]
    queueID = ceUniqueID.split('/')[1]
  except IndexError:
    return S_ERROR('Wrong full queue Name')

  if not diracSiteName:
    gLogger.debug("SiteName not given, looking in /LocaSite/Site")
    diracSiteName = gConfig.getValue('/LocalSite/Site', '')

    if not diracSiteName:
      gLogger.debug("Can't find LocalSite name, looking in CS")
      result = getCESiteMapping(subClusterUniqueID)
      if not result['OK']:
        return result
      diracSiteName = result['Value'][subClusterUniqueID]

      if not diracSiteName:
        gLogger.error('Can not find corresponding Site in CS')
        return S_ERROR('Can not find corresponding Site in CS')

  gridType = diracSiteName.split('.')[0]

  siteCSSEction = '/Resources/Sites/%s/%s/CEs/%s' % (gridType, diracSiteName, subClusterUniqueID)
  queueCSSection = '%s/Queues/%s' % (siteCSSEction, queueID)

  resultDict = {'SubClusterUniqueID': subClusterUniqueID,
                'QueueID': queueID,
                'SiteName': diracSiteName,
                'Grid': gridType,
                'SiteCSSEction': siteCSSEction,
                'QueueCSSection': queueCSSection}

  return S_OK(resultDict)
