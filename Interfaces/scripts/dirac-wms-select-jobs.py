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
from DIRAC import gLogger
from DIRAC.Core.Base import Script

maxJobs = 100
Script.registerSwitch("", "Status=", "Primary status")
Script.registerSwitch("", "MinorStatus=", "Secondary status")
Script.registerSwitch("", "ApplicationStatus=", "Application status")
Script.registerSwitch("", "Site=", "Execution site")
Script.registerSwitch("", "Owner=", "Owner (DIRAC nickname)")
Script.registerSwitch("", "JobGroup=", "Select jobs for specified job group")
Script.registerSwitch("", "Date=", "Date in YYYY-MM-DD format, if not specified default is today")
Script.registerSwitch("", "Maximum=", "Maximum number of jobs shown (default %d, 0 means all)" % maxJobs)
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... JobID ...' % Script.scriptName]))
Script.parseCommandLine(ignoreErrors=True)

args = Script.getPositionalArgs()

# Default values
status = None
minorStatus = None
appStatus = None
site = None
owner = None
jobGroups = []
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
    for xx in switch[1].split(','):
      if xx.isdigit():
        jobGroups.append('%08d' % int(xx))
      else:
        jobGroups.append(xx)
  elif switch[0].lower() == "date":
    date = switch[1]
  elif switch[0] == 'Maximum':
    try:
      maxJobs = int(switch[1])
    except TypeError:
      gLogger.fatal("Invalid max number of jobs", switch[1])
      DIRAC.exit(1)

selDate = date
if not date:
  selDate = 'Today'
conditions = {'Status': status,
              'MinorStatus': minorStatus,
              'ApplicationStatus': appStatus,
              'Owner': owner,
              'JobGroup': ','.join(str(xx) for xx in jobGroups),
              'Date': selDate}

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()
jobs = []
for jobGroup in jobGroups:
  res = dirac.selectJobs(status=status,
                         minorStatus=minorStatus,
                         applicationStatus=appStatus,
                         site=site,
                         owner=owner,
                         jobGroup=jobGroup,
                         date=date,
                         printErrors=False)
  if res['OK']:
    jobs.extend(res['Value'])

conds = ['%s = %s' % (n, v) for n, v in conditions.iteritems() if v]
if maxJobs and len(jobs) > maxJobs:
  jobs = jobs[:maxJobs]
  constrained = ' (first %d shown) ' % maxJobs
else:
  constrained = ' '

if jobs:
  gLogger.notice('==> Selected %s jobs%swith conditions: %s\n%s' % (len(jobs),
                                                                    constrained,
                                                                    ', '.join(conds),
                                                                    ','.join(jobs)))
else:
  gLogger.notice('No jobs were selected with conditions:', ', '.join(conds))

DIRAC.exit(exitCode)
