########################################################################
# $HeadURL$
# File :    CPUNormalization.py
# Author :  Ricardo Graciani
########################################################################

""" DIRAC Workload Management System Client module that encapsulates all the
    methods necessary to handle CPU normalization
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.SiteCEMapping import getQueueInfo
from DIRAC import S_OK, S_ERROR
import os, random

# TODO: This should come from some place in the configuration
NORMALIZATIONCONSTANT = 60. / 250.  # from minutes to seconds and from SI00 to HS06 (ie min * SI00 -> sec * HS06 )

UNITS = { 'HS06': 1. , 'SI00': 1. / 250. }

def queueNormalizedCPU( ceUniqueID ):
  """
    Report Normalized CPU length of queue
  """
  result = getQueueInfo( ceUniqueID )
  if not result['OK']:
    return result

  ceInfoDict = result['Value']

  benchmarkSI00 = ceInfoDict['SI00']
  maxCPUTime = ceInfoDict['maxCPUTime']
  # For some sites there are crazy values in the CS
  maxCPUTime = max( maxCPUTime, 0 )
  maxCPUTime = min( maxCPUTime, 86400 * 12.5 )

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
  """
    Report Normalization Factor applied by Site to the given Queue
  """
  result = getQueueInfo( ceUniqueID )
  if not result['OK']:
    return result

  ceInfoDict = result['Value']
  subClusterUniqueID = ceInfoDict['SubClusterUniqueID']
  benchmarkSI00 = ceInfoDict['SI00']

  if benchmarkSI00:
    return S_OK( benchmarkSI00 )
  else:
    return S_ERROR( 'benchmarkSI00 info not available for %s' % subClusterUniqueID )
    #errorList.append( ( subClusterUniqueID , 'benchmarkSI00 info not available' ) )
    #exitCode = 3

def getCPUNormalization( reference = 'HS06', iterations = 1 ):
  """
    Get Normalized Power of the current CPU in [reference] units
  """
  if reference not in UNITS:
    return S_ERROR( 'Unknown Normalization unit %s' % str( reference ) )
  try:
    iter = max( min( int( iterations ), 10 ), 1 )
  except ( TypeError, ValueError ), x :
    return S_ERROR( x )

  # This number of iterations corresponds to 250 HS06 seconds
  n = int( 1000 * 1000 * 12.5 )
  calib = 250.0 / UNITS[reference]

  m = 0L
  m2 = 0L
  p = 0
  p2 = 0
  # Do one iteration extra to allow CPUs with variable speed
  for i in range( iterations + 1 ):
    if i == 1:
      start = os.times()
    # Now the iterations
    for j in range( n ):
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


