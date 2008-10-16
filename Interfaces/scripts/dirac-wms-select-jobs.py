#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-wms-select-jobs.py,v 1.1 2008/10/16 09:28:34 paterson Exp $
# File :   dirac-wms-select-jobs
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-wms-select-jobs.py,v 1.1 2008/10/16 09:28:34 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
import sys,string
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "Status=", "Primary status" )
Script.registerSwitch( "", "MinorStatus=", "Secondary status" )
Script.registerSwitch( "", "ApplicationStatus=", "Application status" )
Script.registerSwitch( "", "Site=", "Execution site" )
Script.registerSwitch( "", "Owner=", "Owner (DIRAC nickname)" )
Script.registerSwitch( "", "JobGroup=", "Select jobs for specified job group" )
Script.registerSwitch( "", "Date=", "Date in YYYY-MM-DD format, if not specified default is today" )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac import Dirac

args = Script.getPositionalArgs()

#Default values
status=None
minorStatus=None
appStatus=None
site=None
owner=None
jobGroup=None
date=None

def usage():
  print 'Usage: %s [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)

if args:
  usage()

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="status":
    status=switch[1]
  elif switch[0].lower()=="minorstatus":
    minorStatus=switch[1]
  elif switch[0].lower()=="applicationstatus":
    appStatus=switch[1]
  elif switch[0].lower()=="site":
    site=switch[1]
  elif switch[0].lower()=="owner":
    owner=switch[1]
  elif switch[0].lower()=="jobgroup":
    jobGroup=switch[1]
  elif switch[0].lower()=="date":
    date=switch[1]

selDate = date
if not date:
  selDate='Today'
conditions = {'Status':status,'MinorStatus':minorStatus,'ApplicationStatus':appStatus,'Owner':owner,'JobGroup':jobGroup,'Date':selDate}
dirac = Dirac()
result = dirac.selectJobs(Status=status,MinorStatus=minorStatus,ApplicationStatus=appStatus,Site=site,Owner=owner,JobGroup=jobGroup,Date=date)
if not result['OK']:
  print 'ERROR %s' %result['Message']
  exitCode = 2
else:
  conds = []
  for n,v in conditions.items():
    if v:
      conds.append('%s = %s' %(n,v))
  jobs = result['Value']
  constrained = ' '
  if len(jobs)>100:
    jobs = jobs[:100]
    constrained = ' (first 100 shown) '
  print '==> Selected %s jobs%swith conditions: %s\n%s' %(len(result['Value']),constrained,string.join(conds,', '),string.join(jobs,', '))

DIRAC.exit(exitCode)