#!/bin/env python
""" Reset failed requests and operations therein """
__RCSID__ = "$Id: $"

import sys
from DIRAC.Core.Base import Script
maxReset = 100
Script.registerSwitch( '', 'Job=', '   jobID: reset requests for jobID' )
Script.registerSwitch( '', 'Failed', '  reset Failed requests' )
Script.registerSwitch( '', 'Maximum=', '   max number of requests to reset' )
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
  resetFailed = False
  requestName = ''
  job = None
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  reqClient = ReqClient()
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Failed':
      resetFailed = True
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
      requestName = args[0]
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
        requestName = jobName + '_job_%d' % job

  requests = []
  if requestName:
    requests = requestName.split( ',' )
    force = True
  elif resetFailed:
    force = False
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
    freq = 10
    if len( requests ) > freq:
      print "Resetting now %d requests (. each %d requests)" % ( len( requests ), freq )
    else:
      freq = 0
    for n, reqName in enumerate( requests ):
      if freq and ( n % freq ) == 0:
        sys.stdout.write( '.' )
        sys.stdout.flush()
      if len( requests ) > 1:
        gLogger.always( '============ Request %s =============' % reqName )
      ret = reqClient.resetFailedRequest( reqName, force = force )
      if not ret['OK']:
        notReset += 1
        print "Error", ret['Message']
      else:
        if ret['Value'] != 'Not reset':
          reset += 1
        else:
          notReset += 1
    if freq:
      print ""
    if reset:
      print "Reset", reset, 'Requests'
    if notReset:
      print "Not reset (Request doesn't exist or really Failed) %d requests" % notReset
