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

    self.log.info( "requestName=%s requestID=%s status=%s %s" % ( request.RequestName,
                                                                  request.RequestID,
                                                                  requst.Status,
                                                                  "error=%s" % request.Error if request.Error else "" ) )
    for op in request:
      self.log.info( "  operation type=%s operationID=%s order=%s status=%s %s" % ( op.Type,
                                                                                    op.OperationID,
                                                                                    op.Order,
                                                                                    op.Status,
                                                                                    "error=%s" % op.Error if op.Error else "" ) )
      for f in op:
        self.log.info( "   file fileID=%s LFN=%s status=%s %s" % ( f.FileID,
                                                                   f.LFN,
                                                                   f.Status,
                                                                   "error=%s" % f.Error if f.Error else "" ) )



