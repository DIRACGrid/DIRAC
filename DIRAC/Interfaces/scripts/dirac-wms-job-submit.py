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

import os

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JDL ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JDL:      Path to JDL file' ] ) )

Script.registerSwitch( "f:", "File=", "Writes job ids to file <value>" )
Script.registerSwitch( "r:", "UseJobRepo=", "Use the job repository")
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac                              import Dirac
unprocessed_switches = Script.getUnprocessedSwitches()
use_repo = False
repo_name = ""
for sw, value in unprocessed_switches:
  if sw.lower() in ["r", "usejobrepo"]:
    use_repo = True
    repo_name = value
    repo_name = repo_name.replace(".cfg", ".repo")
dirac = Dirac(use_repo, repo_name)
exitCode = 0
errorList = []

jFile = None
for sw, value in unprocessed_switches:
  if sw.lower() in ( 'f', 'file' ):
    if os.path.isfile( value ):
      print 'Appending job ids to existing logfile: %s' %value
      if not os.access( value , os.W_OK ):
        print 'Existing logfile %s must be writable by user.' %value
    jFile = open( value, 'a' )

for jdl in args:

  result = dirac.submitJob( jdl )
  if result['OK']:
    print 'JobID = %s' % ( result['Value'] )
    if jFile != None:
      # parametric jobs
      if isinstance( result['Value'], list ):
        jFile.write( '\n'.join(str(p) for p in result['Value']) )
        jFile.write( '\n' )
      else:  
        jFile.write( str( result['Value'] )+'\n' )
  else:
    errorList.append( ( jdl, result['Message'] ) )
    exitCode = 2

if jFile != None:
  jFile.close()

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
