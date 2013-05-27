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

    request = request["Value"]
    if not request:
      DIRAC.gLogger.info("no such request")
      DIRAC.exit(0)

    DIRAC.gLogger.always( "Request name='%s' ID=%s Status='%s' %s" % ( request.RequestName,
                                                                     request.RequestID,
                                                                     request.Status,
                                                                     "error=%s" % request.Error if request.Error else "" ) )
    for i, op in enumerate( request ):
      DIRAC.gLogger.always( "  [%s] Operation Type='%s' ID=%s Order=%s Status='%s' %s" % ( i, op.Type, op.OperationID,
                                                                                           op.Order, op.Status,
                                                                                           "error=%s" % op.Error if op.Error else "" ) )
      for j, f in enumerate( op ):
        DIRAC.gLogger.always( "    [%02d] ID=%s LFN='%s' Status='%s' %s" % ( j + 1, f.FileID, f.LFN, f.Status,
                                                                             "error=%s" % f.Error if f.Error else "" ) )



