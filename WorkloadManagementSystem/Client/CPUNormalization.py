########################################################################
# $HeadURL$
# File :    CPUNormalization.py
# Author :  Ricardo Graciani
########################################################################

""" DIRAC Workload Management System Client module that encapsulates all the
    methods necessary to handle CPU normalization
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.SiteCEMapping import getSiteForCE, getQueueInfo
from DIRAC import gConfig, S_OK, S_ERROR

# TODO: This should come from some place in the configuration
NORMALIZATIONCONSTANT = 60. / 250.  # from minutes to seconds and from SI00 to HS06 (ie min * SI00 -> sec * HS06 )

def queueNormalizedCPU( ceUniqueID ):
  """
    Report Normalized CPU length of queue
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
    # To get to the Current LHCb 
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
  siteCSSEction = ceInfoDict['SiteCSSEction']
  queueCSSection = ceInfoDict['QueueCSSection']

  benchmarkSI00 = __getQueueNormalization( queueCSSection, siteCSSEction )

  if benchmarkSI00:
    return S_OK( benchmarkSI00 )
  else:
    return S_ERROR( 'benchmarkSI00 info not available for %s' % subClusterUniqueID )
    errorList.append( ( subClusterUniqueID , 'benchmarkSI00 info not available' ) )
    exitCode = 3

  pass

def __getQueueNormalization( queueCSSection, siteCSSEction ):
  """
    Query the CS and return the Normalization
  """
  benchmarkSI00Option = '%s/%s' % ( queueCSSection, 'SI00' )
  benchmarkSI00 = gConfig.getValue( benchmarkSI00Option, 0.0 )
  if not benchmarkSI00:
    benchmarkSI00Option = '%s/%s' % ( siteCSSEction, 'SI00' )
    benchmarkSI00 = gConfig.getValue( benchmarkSI00Option, 0.0 )

  return benchmarkSI00

def __getMaxCPUTime( queueCSSection ):
  """
    Query the CS and return the maxCPUTime 
  """
  maxCPUTimeOption = '%s/%s' % ( queueCSSection, 'maxCPUTime' )
  maxCPUTime = gConfig.getValue( maxCPUTimeOption, 0.0 )
  # For some sites there are crazy values in the CS
  maxCPUTime = max( maxCPUTime, 0 )
  maxCPUTime = min( maxCPUTime, 86400 * 12.5 )

  return maxCPUTime
