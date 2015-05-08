########################################################################
# File :    CPUNormalization.py
# Author :  Ricardo Graciani
########################################################################

""" DIRAC Workload Management System Client module that encapsulates all the
    methods necessary to handle CPU normalization
"""
__RCSID__ = "$Id$"

import os, random

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteCEMapping import getQueueInfo

# TODO: This should come from some place in the configuration
NORMALIZATIONCONSTANT = 60. / 250.  # from minutes to seconds and from SI00 to HS06 (ie min * SI00 -> sec * HS06 )

UNITS = { 'HS06': 1. , 'SI00': 1. / 250. }

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
    #errorList.append( ( subClusterUniqueID , 'benchmarkSI00 info not available' ) )
    #exitCode = 3

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

  # This number of iterations corresponds to 250 HS06 seconds
  n = int( 1000 * 1000 * 12.5 )
  calib = 250.0 / UNITS[reference]

  m = long( 0 )
  m2 = long( 0 )
  p = 0
  p2 = 0
  # Do one iteration extra to allow CPUs with variable speed
  for i in range( iterations + 1 ):
    if i == 1:
      start = os.times()
    # Now the iterations
    for _j in range( n ):
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


def getCPUTime( CPUNormalizationFactor ):
  """ Trying to get CPUTime (in seconds) from the CS. The default is a (low) 10000s.

      This is a generic method, independent from the middleware of the resource.
  """
  CPUTime = gConfig.getValue( '/LocalSite/CPUTimeLeft', 0 )

  if CPUTime:
    # This is in HS06sseconds
    # We need to convert in real seconds
    if not CPUNormalizationFactor:
      CPUNormalizationFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 0.0 )
    CPUTime = CPUTime / int( CPUNormalizationFactor )
  else:
    # now we know that we have to find the CPUTimeLeft by looking in the CS
    gridCE = gConfig.getValue( '/LocalSite/GridCE' )
    CEQueue = gConfig.getValue( '/LocalSite/CEQueue' )
    if not CEQueue:
      # we have to look for a CEQueue in the CS
      # A bit hacky. We should better profit from something generic
      gLogger.warn( "No CEQueue in local configuration, looking to find one in CS" )
      siteName = gConfig.getValue( '/LocalSite/Site' )
      queueSection = '/Resources/Sites/%s/%s/CEs/%s/Queues' % ( siteName.split( '.' )[0], siteName, gridCE )
      res = gConfig.getSections( queueSection )
      if not res['OK']:
        raise RuntimeError( res['Message'] )
      queues = res['Value']
      CPUTimes = []
      for queue in queues:
        CPUTimes.append( gConfig.getValue( queueSection + '/' + queue + '/maxCPUTime', 10000 ) )
      cpuTimeInMinutes = min( CPUTimes )
      # These are (real, wall clock) minutes - damn BDII!
      CPUTime = int( cpuTimeInMinutes ) * 60
    else:
      queueInfo = getQueueInfo( '%s/%s' % ( gridCE, CEQueue ) )
      CPUTime = 10000
      if not queueInfo['OK'] or not queueInfo['Value']:
        gLogger.warn( "Can't find a CE/queue, defaulting CPUTime to %d" % CPUTime )
      else:
        queueCSSection = queueInfo['Value']['QueueCSSection']
        # These are (real, wall clock) minutes - damn BDII!
        cpuTimeInMinutes = gConfig.getValue( '%s/maxCPUTime' % queueCSSection )
        if cpuTimeInMinutes:
          CPUTime = int( cpuTimeInMinutes ) * 60
          gLogger.info( "CPUTime for %s: %d" % ( queueCSSection, CPUTime ) )
        else:
          gLogger.warn( "Can't find maxCPUTime for %s, defaulting CPUTime to %d" % ( queueCSSection, CPUTime ) )

  return CPUTime
