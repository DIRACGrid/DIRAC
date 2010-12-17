#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-wms-job-submit
# Author :  Stuart Paterson
########################################################################
"""
  Submit jobs to DIRAC WMS
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JDL ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JDL:      Path to JDL file' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac                              import Dirac
dirac = Dirac()
exitCode = 0
errorList = []

for jdl in args:

  result = dirac.submit( jdl )
  if result['OK']:
    print 'JobID = %s' % ( result['Value'] )
  else:
    errorList.append( ( jdl, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
