#! /usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-wms-jobs-select-output-search.py,v 1.3 2009/04/18 18:26:59 rgracian Exp $
# File :   dirac-wms-jobs-select-output-search
# Author : Vladimir Romanovsky
########################################################################
__RCSID__   = "$Id: dirac-wms-jobs-select-output-search.py,v 1.3 2009/04/18 18:26:59 rgracian Exp $"
__VERSION__ = "$Revision: 1.3 $"
import os, sys, popen2
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "Status=", "Primary status" )
Script.registerSwitch( "", "MinorStatus=", "Secondary status" )
Script.registerSwitch( "", "ApplicationStatus=", "Application status" )
Script.registerSwitch( "", "Site=", "Execution site" )
Script.registerSwitch( "", "Owner=", "Owner (DIRAC nickname)" )
Script.registerSwitch( "", "JobGroup=", "Select jobs for specified job group" )
Script.registerSwitch( "", "Date=", "Date in YYYY-MM-DD format, if not specified default is today" )
Script.registerSwitch( "", "File=", "File name,if not specified default is std.out " )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac                              import Dirac
from shutil import rmtree

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

#Default values
status=None
minorStatus=None
appStatus=None
site=None
owner=None
jobGroup=None
date=None
filename = 'std.out'

def usage():
  print 'Usage: %s string to seearch '%(Script.scriptName)
  DIRAC.exit(2)

if len(args)!=1:
  usage()
  
searchstring = str(args[0])

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
  elif switch[0].lower()=="file":
    filename=switch[1]

selDate = date
if not date:
  selDate='Today'
   
dirac=Dirac()
exitCode = 0
errorList = []
resultDict = {}

result = dirac.selectJobs(Status=status,MinorStatus=minorStatus,ApplicationStatus=appStatus,Site=site,Owner=owner,JobGroup=jobGroup,Date=date)
if result['OK']:
  jobs = result['Value']
else:
  print "Error in selectJob",result['Message']
  DIRAC.exit(2)
  
for job in jobs:

  result = dirac.getOutputSandbox(job)
  if result['OK']:
    if os.path.exists('%s' %job):

      lines = []
      try:
        lines = open(os.path.join(job,filename)).readlines()
      except Exception,x:
        errorList.append( (job, x) )
      for line in lines:
        if line.count(searchstring):
          resultDict[job]= line
    rmtree("%s" %(job))
  else:
    errorList.append( (job, result['Message']) )
    exitCode = 2

for result in resultDict.iteritems():
  print result


DIRAC.exit(exitCode)
