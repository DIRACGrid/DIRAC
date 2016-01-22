#!/bin/env python
""" Reset failed requests and operations therein """
__RCSID__ = "$Id: $"

import sys
from DIRAC.Core.Base import Script
maxReset = 100
Script.registerSwitch( '', 'Job=', '   jobID: reset requests for jobID' )
Script.registerSwitch( '', 'Failed', '  reset Failed requests' )
Script.registerSwitch( '', 'Maximum=', '   max number of requests to reset' )
Script.registerSwitch( '', 'All', '   reset requests even if irrecoverable' )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] requestID' % Script.scriptName,
                                     'Arguments:',
                                     ' requestID: a request ID' ] ) )
# # execution
if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger
  resetFailed = False
  requests = []
  jobs = []
  allR = False
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  reqClient = ReqClient()
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Failed':
      resetFailed = True
    elif switch[0] == 'All':
      allR = True
    elif switch[0] == 'Maximum':
      try:
        maxReset = int( switch[1] )
      except:
        pass
    elif switch[0] == 'Job':
      try:
        jobs = [int( job ) for job in switch[1].split( ',' )]
      except:
        print "Invalid jobID", switch[1]

  if not jobs:
    args = Script.getPositionalArgs()

    requests = list()
    if len( args ) == 1:
      requestsSplit = args[0].split( ',' )
      for reqID in requestsSplit:
        try:
          requestID = int( reqID )
        except ValueError:
          requestID = reqClient.getRequestIDForName( reqID )
          if not requestID['OK']:
            gLogger.always( requestID['Message'] )
            continue
          requestID = requestID['Value']
        requests.append( requestID )

  else:
    res = reqClient.getRequestIDsForJobs( jobs )
    if not res['OK']:
      gLogger.fatal( "Error getting request for jobs", res['Message'] )
      DIRAC.exit( 2 )
    if res['Value']['Failed']:
      gLogger.error( "No request found for jobs %s" % str( res['Value']['Failed'].keys() ) )
    requests = sorted( res['Value']['Successful'].values() )

  if resetFailed:
    allR = False
    res = reqClient.getRequestIDsList( ['Failed'], maxReset );
    if not res['OK']:
        print "Error", res['Message'];
    elif res['Value']:
      requests = [reqID for reqID, _x, _y in res['Value']]

  if not requests:
    print "No requests to reset"
    Script.showHelp()
  else:
    reset = 0
    notReset = 0
    if len( requests ) > 1:
      gLogger.always( "Resetting now %d requests" % len( requests ) )
    for reqID in requests:
      if len( requests ) > 1:
        gLogger.always( '============ Request %s =============' % reqID )
      ret = reqClient.resetFailedRequest( reqID, allR = allR )
      if not ret['OK']:
        notReset += 1
        print "Error", ret['Message']
      else:
        if ret['Value'] != 'Not reset':
          reset += 1
        else:
          notReset += 1
    if reset:
      print "Reset", reset, 'Requests'
    if notReset:
      print "Not reset (doesn't exist or irrecoverable) %d requests" % notReset
