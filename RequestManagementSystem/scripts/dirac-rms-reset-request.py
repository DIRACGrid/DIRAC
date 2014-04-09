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
                                     ' %s [option|cfgfile] [requestName|requestID]' % Script.scriptName,
                                     'Arguments:',
                                     ' requestName: a request name' ] ) )
# # execution
if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger
  resetFailed = False
  requests = []
  job = None
  all = False
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  reqClient = ReqClient()
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Failed':
      resetFailed = True
    elif switch[0] == 'All':
      all = True
    elif switch[0] == 'Maximum':
      try:
        maxReset = int( switch[1] )
      except:
        pass
    elif switch[0] == 'Job':
      try:
        job = int( switch[1] )
      except:
        print "Invalid jobID", switch[1]

  if not job:
    args = Script.getPositionalArgs()

    if len( args ) == 1:
      requests = args[0].split( ',' )
  else:
    from DIRAC.Interfaces.API.Dirac                              import Dirac
    dirac = Dirac()
    res = dirac.attributes( job )
    if not res['OK']:
      print "Error getting job parameters", res['Message']
    else:
      jobName = res['Value'].get( 'JobName' )
      if not jobName:
        print 'Job %d not found' % job
      else:
        requests = [jobName + '_job_%d' % job]

  if resetFailed:
    all = False
    res = reqClient.getRequestNamesList( ['Failed'], maxReset );
    if not res['OK']:
        print "Error", res['Message'];
    elif res['Value']:
      requests = [reqName for reqName, _x, _y in res['Value']]

  if not requests:
    print "No requests to reset"
    Script.showHelp()
  else:
    reset = 0
    notReset = 0
    if len( requests ) > 1:
      gLogger.always( "Resetting now %d requests" % len( requests ) )
    for reqName in requests:
      if len( requests ) > 1:
        gLogger.always( '============ Request %s =============' % reqName )
      ret = reqClient.resetFailedRequest( reqName, all = all )
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
