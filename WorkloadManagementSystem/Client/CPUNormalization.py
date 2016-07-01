########################################################################
# File :    CPUNormalization.py
# Author :  Ricardo Graciani
########################################################################

""" DIRAC Workload Management System Client module that encapsulates all the
    methods necessary to handle CPU normalization
"""

import os
import random
import urllib

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteCEMapping import getQueueInfo
from DIRAC.Core.Utilities.TimeLeft.TimeLeft import TimeLeft

__RCSID__ = "$Id$"

# TODO: This should come from some place in the configuration
NORMALIZATIONCONSTANT = 60. / 250.  # from minutes to seconds and from SI00 to HS06 (ie min * SI00 -> sec * HS06 )

UNITS = { 'HS06': 1. , 'SI00': 1. / 250. }

def getMachineFeatures():
  """ This uses the _old_ MJF information """
  features = {}
  featuresDir = os.environ.get( "MACHINEFEATURES" )
  if featuresDir is None:
    return features
  for item in ( 'hs06', 'jobslots', 'log_cores', 'phys_cores' ):
    fname = os.path.join( featuresDir, item )
    try:
      val = urllib.urlopen( fname ).read()
    except :
      val = 0
    features[item] = val
  return features

def getJobFeatures():
  """ This uses the _new_ MJF information """
  features = {}
  featuresDir = os.environ.get( "JOBFEATURES" )
  if featuresDir is None:
    return features
  for item in ( 'hs06_job', 'allocated_cpu' ):
    fname = os.path.join( featuresDir, item )
    try:
      val = urllib.urlopen( fname ).read()
    except IOError:
      val = 0
    features[item] = val
  return features


def getPowerFromMJF():
  """ Extracts the machine power from either JOBFEATURES or MACHINEFEATURES """
  try:
    features = getJobFeatures()
    if 'hs06_job' in features:
      return round( float( features['hs06_job'] ), 2 )
    features = getMachineFeatures()
    totalPower = float( features.get( 'hs06', 0 ) )
    logCores = float( features.get( 'log_cores', 0 ) )
    physCores = float( features.get( 'phys_cores', 0 ) )
    jobSlots = float( features.get( 'jobslots', 0 ) )
    denom = min( max( logCores, physCores ), jobSlots ) if ( logCores or physCores ) and jobSlots else None
    if totalPower and denom:
      return round( totalPower / denom , 2 )
    else:
      return None
  except ValueError as e:
    gLogger.exception( "Exception getting MJF information", lException = e )
    return None

def queueNormalizedCPU( ceUniqueID ):
  """ Report Normalized CPU length of queue
  """
  result = getQueueInfo( ceUniqueID )
  if not result['OK']:
    return result

  ceInfoDict = result['Value']
  siteCSSEction = ceInfoDict['SiteCSSEction']
  queueCSSection = ceInfoDict['QueueCSSection']

  benchmarkSI00 = __getQueueNormalization( queueCSSection, siteCSSEction )
  maxCPUTime = __getMaxCPUTime( queueCSSection )

  if maxCPUTime and benchmarkSI00:
    normCPUTime = NORMALIZATIONCONSTANT * maxCPUTime * benchmarkSI00
  else:
    if not benchmarkSI00:
      subClusterUniqueID = ceInfoDict['SubClusterUniqueID']
      return S_ERROR( 'benchmarkSI00 info not available for %s' % subClusterUniqueID )
    if not maxCPUTime:
      return S_ERROR( 'maxCPUTime info not available' )

  return S_OK( normCPUTime )

def getQueueNormalization( ceUniqueID ):
  """ Report Normalization Factor applied by Site to the given Queue
  """
  result = getQueueInfo( ceUniqueID )
  if not result['OK']:
    return result

  ceInfoDict = result['Value']
  siteCSSEction = ceInfoDict['SiteCSSEction']
  queueCSSection = ceInfoDict['QueueCSSection']
  subClusterUniqueID = ceInfoDict['SubClusterUniqueID']

  benchmarkSI00 = __getQueueNormalization( queueCSSection, siteCSSEction )

  if benchmarkSI00:
    return S_OK( benchmarkSI00 )
  else:
    return S_ERROR( 'benchmarkSI00 info not available for %s' % subClusterUniqueID )
    # errorList.append( ( subClusterUniqueID , 'benchmarkSI00 info not available' ) )
    # exitCode = 3

def __getQueueNormalization( queueCSSection, siteCSSEction ):
  """ Query the CS and return the Normalization
  """
  benchmarkSI00Option = '%s/%s' % ( queueCSSection, 'SI00' )
  benchmarkSI00 = gConfig.getValue( benchmarkSI00Option, 0.0 )
  if not benchmarkSI00:
    benchmarkSI00Option = '%s/%s' % ( siteCSSEction, 'SI00' )
    benchmarkSI00 = gConfig.getValue( benchmarkSI00Option, 0.0 )

  return benchmarkSI00

def __getMaxCPUTime( queueCSSection ):
  """ Query the CS and return the maxCPUTime
  """
  maxCPUTimeOption = '%s/%s' % ( queueCSSection, 'maxCPUTime' )
  maxCPUTime = gConfig.getValue( maxCPUTimeOption, 0.0 )
  # For some sites there are crazy values in the CS
  maxCPUTime = max( maxCPUTime, 0 )
  maxCPUTime = min( maxCPUTime, 86400 * 12.5 )

  return maxCPUTime

def getCPUNormalization( reference = 'HS06', iterations = 1 ):
  """ Get Normalized Power of the current CPU in [reference] units
  """
  if reference not in UNITS:
    return S_ERROR( 'Unknown Normalization unit %s' % str( reference ) )
  try:
    max( min( int( iterations ), 10 ), 1 )
  except ( TypeError, ValueError ), x :
    return S_ERROR( x )

  # This number of iterations corresponds to 1kHS2k.seconds, i.e. 250 HS06 seconds
  # 06.11.2015: fixed absolute normalization w.r.t. MJF at GRIDKA
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  corr = Operations().getValue( 'JobScheduling/CPUNormalizationCorrection', 1. )
  n = int( 1000 * 1000 * 12.5 )
  calib = 250.0 / UNITS[reference] / corr

  m = long( 0 )
  m2 = long( 0 )
  p = 0
  p2 = 0
  # Do one iteration extra to allow CPUs with variable speed
  for i in range( iterations + 1 ):
    if i == 1:
      start = os.times()
    # Now the iterations
    for _j in xrange( n ):
      t = random.normalvariate( 10, 1 )
      m += t
      m2 += t * t
      p += t
      p2 += t * t

  end = os.times()
  cput = sum( end[:4] ) - sum( start[:4] )
  wall = end[4] - start[4]

  if not cput:
    return S_ERROR( 'Can not get used CPU' )

  return S_OK( {'CPU': cput, 'WALL':wall, 'NORM': calib * iterations / cput, 'UNIT': reference } )


def getCPUTime( cpuNormalizationFactor ):
  """ Trying to get CPUTime left for execution (in seconds).

      It will first look to get the work left looking for batch system information useing the TimeLeft utility.
      If it succeeds, it will convert it in real second, and return it.

      If it fails, it tries to get it from the static info found in CS.
      If it fails, it returns the default, which is a large 9999999, that we may consider as "Infinite".

      This is a generic method, independent from the middleware of the resource if TimeLeft doesn't return a value

      args:
        cpuNormalizationFactor (float): the CPU power of the current Worker Node. If not passed in, it's get from the local configuration

      returns:
        cpuTimeLeft (int): the CPU time left, in seconds
  """
  cpuTimeLeft = 0.
  cpuWorkLeft = gConfig.getValue( '/LocalSite/CPUTimeLeft', 0 )

  if not cpuWorkLeft:
    # Try and get the information from the CPU left utility
    result = TimeLeft().getTimeLeft()
    if result['OK']:
      cpuWorkLeft = result['Value']

  if cpuWorkLeft:
    # This is in HS06sseconds
    # We need to convert in real seconds
    if not cpuNormalizationFactor:  # if cpuNormalizationFactor passed in is 0, try get it from the local cfg
      cpuNormalizationFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 0.0 )
    if cpuNormalizationFactor:
      cpuTimeLeft = cpuWorkLeft / cpuNormalizationFactor  # this is a float

  if not cpuTimeLeft:
    # now we know that we have to find the CPUTimeLeft by looking in the CS
    # this is not granted to be correct as the CS units may not be real seconds
    gridCE = gConfig.getValue( '/LocalSite/GridCE' )
    ceQueue = gConfig.getValue( '/LocalSite/CEQueue' )
    if not ceQueue:
      # we have to look for a ceQueue in the CS
      # A bit hacky. We should better profit from something generic
      gLogger.warn( "No CEQueue in local configuration, looking to find one in CS" )
      siteName = gConfig.getValue( '/LocalSite/Site' )
      queueSection = '/Resources/Sites/%s/%s/CEs/%s/Queues' % ( siteName.split( '.' )[0], siteName, gridCE )
      res = gConfig.getSections( queueSection )
      if not res['OK']:
        raise RuntimeError( res['Message'] )
      queues = res['Value']
      cpuTimes = [gConfig.getValue( queueSection + '/' + queue + '/maxCPUTime', 9999999. ) for queue in queues]
      # These are (real, wall clock) minutes - damn BDII!
      cpuTimeLeft = min( cpuTimes ) * 60
    else:
      queueInfo = getQueueInfo( '%s/%s' % ( gridCE, ceQueue ) )
      cpuTimeLeft = 9999999.
      if not queueInfo['OK'] or not queueInfo['Value']:
        gLogger.warn( "Can't find a CE/queue, defaulting CPUTime to %d" % cpuTimeLeft )
      else:
        queueCSSection = queueInfo['Value']['QueueCSSection']
        # These are (real, wall clock) minutes - damn BDII!
        cpuTimeInMinutes = gConfig.getValue( '%s/maxCPUTime' % queueCSSection, 0. )
        if cpuTimeInMinutes:
          cpuTimeLeft = cpuTimeInMinutes * 60.
          gLogger.info( "CPUTime for %s: %f" % ( queueCSSection, cpuTimeLeft ) )
        else:
          gLogger.warn( "Can't find maxCPUTime for %s, defaulting CPUTime to %f" % ( queueCSSection, cpuTimeLeft ) )

  return int( cpuTimeLeft )
