#!/bin/env python
""" Cancel a request """
__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] <RequestID list>' % Script.scriptName ] ) )

if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  requests = []

  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  reqClient = ReqClient()


  args = Script.getPositionalArgs()
  if len( args ) == 1:
    requests = [requestID for requestID in args[0].split( ',' ) if requestID]

  if not requests:
    DIRAC.gLogger.fatal( "Need at least one request name" )
    Script.showHelp()
    DIRAC.exit( 1 )

  for requestID in requests:
    requestID = requestID.strip()
    try:
      reqID = int( requestID )
    except ValueError:
      reqID = reqClient.getRequestIDForName( requestID )
      if not reqID['OK']:
        gLogger.always( reqID['Message'] )
        continue
      reqID = reqID['Value']
    res = reqClient.cancelRequest( reqID )
    if res['OK']:
      DIRAC.gLogger.always( "Request %s canceled" % reqID )
    else:
      DIRAC.gLogger.error( "Error canceling request %s" % reqID, res['Message'] )

  DIRAC.exit( 0 )


