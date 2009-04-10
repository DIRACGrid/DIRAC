#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-wms-get-normalized-queue-length.py,v 1.3 2009/04/10 06:44:08 rgracian Exp $
# File :   dirac-wms-get-normalized-queue-length.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: dirac-wms-get-normalized-queue-length.py,v 1.3 2009/04/10 06:44:08 rgracian Exp $"
__VERSION__ = "$Revision: 1.3 $"

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.SiteCEMapping import getSiteForCE
from DIRAC import gConfig

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <GlueCEUniqueID> [<GlueCEUniqueID>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

exitCode = 0
errorList = []
resultList = {}

for ceUniqueID in args:

  try:
    subClusterUniqueID = ceUniqueID.split(':')[0]
    queueID = ceUniqueID.split('/')[1]
  except:
    errorList.append( (ceUniqueID, 'Wrong full queue Name') )
    exitCode = 1
    continue
    
  result = getSiteForCE( subClusterUniqueID )
  if not result['OK']:
    errorList.append( (ceUniqueID, result['Message']) )
    exitCode = 2
    continue
  diracSiteName = result['Value']
  if not diracSiteName:
    # getSiteForCE will return '' if not matching site is found
    errorList.append( (ceUniqueID, 'Can not find corresponding Site in CS' ) )
    exitCode = 2
    continue
  
  siteCSSEction = '/Resources/Sites/%s/%s/CEs/%s' %( 'LCG', diracSiteName, subClusterUniqueID )

  benchmarkSI00Option = '%s/%s' % ( siteCSSEction, 'SI00' )
  benchmarkSI00       = gConfig.getValue( benchmarkSI00Option, 0.0 )

  maxCPUTimeOption = '%s/Queues/%s/maxCPUTime' % ( siteCSSEction, queueID )
  maxCPUTime       = gConfig.getValue( maxCPUTimeOption, 0.0 )
  # For some sites there are crazy values in the CS
  maxCPUTime       = max( maxCPUTime, 0 )
  maxCPUTime       = min( maxCPUTime, 86400 * 10 )
  
  if maxCPUTime and benchmarkSI00:
    # To get to the Current LHCb 
    normCPUTime = 60. / 500. * maxCPUTime * benchmarkSI00
    resultList[ceUniqueID] = normCPUTime
    print ceUniqueID, normCPUTime
  else:
    if not benchmarkSI00:
      errorList.append( (subClusterUniqueID ,'benchmarkSI00 info not available' ) )
      exitCode = 3
    if not maxCPUTime:
      errorList.append( (ceUniqueID ,'maxCPUTime info not avalailable' ) )
      exitCode = 3


for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)
