#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-select-requests
# Author :  Stuart Paterson
########################################################################
"""
  Select requests from the request management system
"""
__RCSID__ = "$Id$"
import sys, string
import DIRAC
from DIRAC.Core.Base import Script


Script.registerSwitch( "", "JobID=", "WMS JobID for the request (if applicable)" )
Script.registerSwitch( "", "RequestID=", "ID assigned during submission of the request" )
Script.registerSwitch( "", "RequestName=", "XML request file name" )
Script.registerSwitch( "", "RequestType=", "Type of the request e.g. 'transfer'" )
Script.registerSwitch( "", "Status=", "Request status" )
Script.registerSwitch( "", "Operation=", "Request operation e.g. 'replicateAndRegister'" )
Script.registerSwitch( "", "RequestStart=", "First request to consider (start from 0 by default)" )
Script.registerSwitch( "", "Limit=", "Selection limit (default 100)" )
Script.registerSwitch( "", "OwnerDN=", "DN of owner (in double quotes)" )
Script.registerSwitch( "", "OwnerGroup=", "Owner group" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

args = Script.getPositionalArgs()

#Default values
jobID = None
requestID = None
requestName = None
requestType = None
status = None
operation = None
ownerDN = None
ownerGroup = None
requestStart = 0
limit = 100

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "jobid":
    jobID = switch[1]
  elif switch[0].lower() == "requestid":
    requestID = switch[1]
  elif switch[0].lower() == "requestname":
    requestName = switch[1]
  elif switch[0].lower() == "requesttype":
    requestType = switch[1]
  elif switch[0].lower() == "status":
    status = switch[1]
  elif switch[0].lower() == "operation":
    operation = switch[1]
  elif switch[0].lower() == "requeststart":
    requestStart = switch[1]
  elif switch[0].lower() == "limit":
    limit = switch[1]
  elif switch[0].lower() == "ownerDN":
    ownerDN = switch[1]
  elif switch[0].lower() == "ownerGroup":
    ownerGroup = switch[1]

diracAdmin = DiracAdmin()
result = diracAdmin.selectRequests( jobID = jobID, requestID = requestID, requestName = requestName, 
                                    requestType = requestType, status = status, operation = operation, 
                                    ownerDN = ownerDN, ownerGroup = ownerGroup, requestStart = requestStart, 
                                    limit = limit, printOutput = True )
if not result['OK']:
  print 'ERROR %s' % result['Message']
  exitCode = 2

DIRAC.exit( exitCode )
