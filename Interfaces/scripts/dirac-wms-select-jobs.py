#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-wms-select-jobs
# Author :  Stuart Paterson
########################################################################
"""
  Select DIRAC jobs matching the given conditions
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "Status=", "Primary status" )
Script.registerSwitch( "", "MinorStatus=", "Secondary status" )
Script.registerSwitch( "", "ApplicationStatus=", "Application status" )
Script.registerSwitch( "", "Site=", "Execution site" )
Script.registerSwitch( "", "Owner=", "Owner (DIRAC nickname)" )
Script.registerSwitch( "", "JobGroup=", "Select jobs for specified job group" )
Script.registerSwitch( "", "Date=", "Date in YYYY-MM-DD format, if not specified default is today" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JobID ...' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

#Default values
status = None
minorStatus = None
appStatus = None
site = None
owner = None
jobGroup = None
date = None

if args:
  Script.showHelp()

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "status":
    status = switch[1]
  elif switch[0].lower() == "minorstatus":
    minorStatus = switch[1]
  elif switch[0].lower() == "applicationstatus":
    appStatus = switch[1]
  elif switch[0].lower() == "site":
    site = switch[1]
  elif switch[0].lower() == "owner":
    owner = switch[1]
  elif switch[0].lower() == "jobgroup":
    jobGroup = switch[1]
  elif switch[0].lower() == "date":
    date = switch[1]

selDate = date
if not date:
  selDate = 'Today'
conditions = { 'Status':status,
               'MinorStatus':minorStatus,
               'ApplicationStatus':appStatus,
               'Owner':owner,
               'JobGroup':jobGroup,
               'Date':selDate }

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()
result = dirac.selectJobs( status = status,
                           minorStatus = minorStatus,
                           applicationStatus = appStatus,
                           site = site,
                           owner = owner,
                           jobGroup = jobGroup,
                           date = date )
if not result['OK']:
  print 'ERROR %s' % result['Message']
  exitCode = 2
else:
  conds = []
  for n, v in conditions.items():
    if v:
      conds.append( '%s = %s' % ( n, v ) )
  jobs = result['Value']
  constrained = ' '
  if len( jobs ) > 100:
    jobs = jobs[:100]
    constrained = ' (first 100 shown) '
  print '==> Selected %s jobs%swith conditions: %s\n%s' % ( len( result['Value'] ),
                                                            constrained,
                                                            ', '.join( conds ),
                                                            ', '.join( jobs ) )

DIRAC.exit( exitCode )
