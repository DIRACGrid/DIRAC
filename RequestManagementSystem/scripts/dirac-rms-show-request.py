#!/bin/env python
""" Show request given its name, a jobID or a transformation and a task """
__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.registerSwitch( '', 'Job=', '   = JobID' )
Script.registerSwitch( '', 'Transformation=', '   = transID' )
Script.registerSwitch( '', 'Tasks=', '   = list of taskIDs' )
Script.registerSwitch( '', 'Verbose', '   Print more information' )
Script.registerSwitch( '', 'Full', '   Print full request' )
Script.registerSwitch( '', 'Status=', '   Select all requests in a given status' )
Script.registerSwitch( '', 'Since=', '   Associated to --Status, start date (default= 24h ago' )
Script.registerSwitch( '', 'Until=', '   Associated to --Status, end date (default= now' )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] [requestName|requestID]' % Script.scriptName,
                                     'Arguments:',
                                     ' requestName: a request name' ] ) )


# # execution
if __name__ == "__main__":
  from DIRAC.Core.Base.Script import parseCommandLine
  import datetime
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger

  job = None
  requestName = ""
  transID = None
  tasks = None
  requests = None
  full = False
  verbose = False
  status = None
  until = None
  since = None
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Job':
      try:
        job = int( switch[1] )
      except:
        print "Invalid jobID", switch[1]
    elif switch[0] == 'Transformation':
      try:
        transID = int( switch[1] )
      except:
        print 'Invalid transID', switch[1]
    elif switch[0] == 'Tasks':
      try:
        taskIDs = [int( task ) for task in switch[1].split( ',' )]
      except:
        print 'Invalid tasks', switch[1]
    elif switch[0] == 'Full':
      full = True
    elif switch[0] == 'Verbose':
      verbose = True
    elif switch[0] == 'Status':
      status = switch[1].capitalize()
    elif switch[0] == 'Since':
      try:
        since = datetime.datetime.strptime( switch[1], '%Y-%m-%d' )
      except:
        print "Invalid --Since date"
        DIRAC.exit( 2 )
    elif switch[0] == 'Until':
      try:
        until = datetime.datetime.strptime( switch[1], '%Y-%m-%d' )
      except:
        print "Invalid --Until date"
        DIRAC.exit( 2 )


  if status:
    if not until:
      until = datetime.datetime.now()
    if not since:
      since = until - datetime.timedelta( hours = 24 )
  if transID:
    if not taskIDs:
      Script.showHelp()
      DIRAC.exit( 2 )
    requests = ['%08d_%08d' % ( transID, task ) for task in taskIDs]

  elif not job:
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

  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  from DIRAC.RequestManagementSystem.Client.ReqClient import printRequest
  reqClient = ReqClient()

  if status:
    res = reqClient.getRequestNamesList( [status], limit = 999999999 )
    if not res['OK']:
      print "Error getting requests:", res['Message']
      DIRAC.exit( 2 )
    requests = [reqName for reqName, _st, updTime in res['Value'] if updTime > since and updTime <= until]
    print 'Obtained %d requests %s between %s and %s' % ( len( requests ), status, since, until )
  if not requests:
    Script.showHelp()
    DIRAC.exit( 2 )
  for requestName in [reqN for reqN in requests if reqN]:
    if len( requests ) > 1:
      print '\n==================================='
    try:
      requestName = reqClient.getRequestName( int( requestName ) )
      if requestName['OK']:
        requestName = requestName['Value']
    except ValueError:
      pass

    request = reqClient.peekRequest( requestName )
    if not request["OK"]:
      gLogger.error( request["Message"] )
      DIRAC.exit( -1 )

    request = request["Value"]
    if not request:
      gLogger.info( "no such request" )
      DIRAC.exit( 0 )

    status = reqClient.getRequestStatus( requestName ).get( 'Value', 'Unknown' )

    printRequest( request, status = status, full = full, verbose = verbose )



