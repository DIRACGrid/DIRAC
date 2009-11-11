#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-wms-get-queue-normalization.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"

import DIRAC
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

  if benchmarkSI00:
    resultList[ceUniqueID] = benchmarkSI00
    print ceUniqueID, benchmarkSI00
  else:
    errorList.append( (subClusterUniqueID ,'benchmarkSI00 info not available' ) )
    exitCode = 3


for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)
