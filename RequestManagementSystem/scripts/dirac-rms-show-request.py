#!/bin/env python  
""" show request """
__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] requestName' % Script.scriptName,
                                     'Arguments:',
                                     ' requestName: a request name' ] ) )

if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC

  args = Script.getPositionalArgs()

  requestName = ""
  if not len(args) == 1:
    Script.showHelp()
  else:
    requestName = args[0]

  if requestName:
    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
    reqClient = ReqClient()
  
    request = reqClient.peekRequest( requestName )
    if not request["OK"]:
      DIRAC.gLogger.error( request["Message"] )
      DIRAC.exit(-1)

    print request

    request = request["Value"]

    if not request:
      DIRAC.gLogger.info("no such request")
      DIRAC.exit(0)

    print request.RequestName, request.Status, request.Error
    for op in request:
      print op.Type, op.Status, op.Error
      for f in op:
        print f.LFN, f.PFN, f.Status, f.Error



