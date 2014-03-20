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
        requestName = jobname + '_job_%d' % job

  requests = []
  if requestName:
    requests = [requestName]
  elif resetFailed:
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
    for reqName in requests:
      toReset = True
      if len( requests ) < maxReset:
        req = reqClient.peekRequest( reqName ).get( 'Value' )
        if not req:
          continue
        for op in req:
          if op.Status == 'Failed':
            if not op.Type.startswith( 'Remove' ):
              for f in op:
                if f.Status == 'Failed' and f.Error == 'No such file or directory':
                  toReset = False
                  notReset += 1
                  break
            break
      if toReset:
        ret = reqClient.resetFailedRequest( reqName )
        if not ret['OK']:
          print "Error", ret['Message']
        else:
          if freq and ( reset % freq ) == 0:
            sys.stdout.write( '.' )
            sys.stdout.flush()
          reset += 1
    if reset:
      print "\nReset", reset, 'Requests'
    if notReset:
      print "Not reset (File doesn't exist) %d requests" % notReset
