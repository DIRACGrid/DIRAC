#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-wms-job-get-output
# Author : Stuart Paterson
########################################################################
"""
  Retrieve output sandbox for a DIRAC job
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

import os

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JobID:    DIRAC Job ID or a name of the file with JobID per line' ] ) )
Script.registerSwitch( "D:", "Dir=", "Store the output in this directory" )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.Dirac  import Dirac

if len( args ) < 1:
  Script.showHelp()

dirac = Dirac()
exitCode = 0
errorList = []

outputDir = None
for sw, v in Script.getUnprocessedSwitches():
  if sw in ( 'D', 'Dir' ):
    outputDir = v

jobs = []
for arg in args:
  if os.path.exists(arg):
    jFile = open(arg)
    jobs += jFile.read().split()
    jFile.close()
  else:
    jobs.append(arg)

for job in jobs:

  result = dirac.getOutputSandbox( job, outputDir = outputDir )
  if result['OK']:
    if os.path.exists( '%s' % job ):
      print 'Job output sandbox retrieved in %s/' % ( job )
  else:
    errorList.append( ( job, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
