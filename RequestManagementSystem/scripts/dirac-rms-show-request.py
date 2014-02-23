#!/bin/env python
""" Show request given its name or a jobID """
__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.registerSwitch( '', 'Job=', '   = JobID' )
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

  job = None
  requestName = ""
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Job':
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


  if requestName:
    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
    reqClient = ReqClient()

    try:
      requestName = reqClient.getRequestName( int( requestName ) )
      if requestName['OK']:
        requestName = requestName['Value']
    except ValueError:
      pass

    request = reqClient.peekRequest( requestName )
    if not request["OK"]:
      DIRAC.gLogger.error( request["Message"] )
      DIRAC.exit( -1 )

    request = request["Value"]
    if not request:
      DIRAC.gLogger.info( "no such request" )
      DIRAC.exit( 0 )

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
  else:
    Script.showHelp()
    DIRAC.exit( 2 )



