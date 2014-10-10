#!/bin/env python
""" Cancel a request """
__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] <Request list>' % Script.scriptName ] ) )

if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  requests = []

  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  reqClient = ReqClient()


  args = Script.getPositionalArgs()
  if len( args ) == 1:
    requests = [reqName for reqName in args[0].split( ',' ) if reqName]

  if not requests:
    DIRAC.gLogger.fatal( "Need at least one request name" )
    Script.showHelp()
    DIRAC.exit( 1 )

  for reqName in requests:
    reqName = reqName.strip()
    res = reqClient.cancelRequest( reqName )
    if res['OK']:
      DIRAC.gLogger.always( "Request %s canceled" % reqName )
    else:
      DIRAC.gLogger.error( "Error canceling request %s" % reqName, res['Message'] )

  DIRAC.exit( 0 )


