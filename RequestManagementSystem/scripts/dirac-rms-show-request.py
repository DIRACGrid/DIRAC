#!/bin/env python
""" Show request given its name, a jobID or a transformation and a task """
__RCSID__ = "$Id: $"

import datetime
def convertDate( date ):
  try:
    value = datetime.datetime.strptime( date, '%Y-%m-%d' )
  except:
    pass
  try:
    value = datetime.datetime.utcnow() - datetime.timedelta( hours = int( 24 * float( date ) ) )
  except:
    gLogger.fatal( "Invalid date", date )
    value = None
  return value


from DIRAC.Core.Base import Script
Script.registerSwitch( '', 'Job=', '   = JobID' )
Script.registerSwitch( '', 'Transformation=', '   = transID' )
Script.registerSwitch( '', 'Tasks=', '   = list of taskIDs' )
Script.registerSwitch( '', 'NotVerbose', '   Print more information' )
Script.registerSwitch( '', 'Terse', '   Only print request status' )
Script.registerSwitch( '', 'Full', '   Print full request' )
Script.registerSwitch( '', 'Status=', '   Select all requests in a given status' )
Script.registerSwitch( '', 'Since=', '      Associated to --Status, start date yyyy-mm-dd or nb of days (default= -one day' )
Script.registerSwitch( '', 'Until=', '      Associated to --Status, end date (default= now' )
Script.registerSwitch( '', 'All', '      Show all requests with given status (otherwise exclude File does not exist' )
Script.registerSwitch( '', 'Reset', '      Reset Failed files to Waiting' )
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

  job = None
  requestName = ""
  transID = None
  tasks = None
  requests = None
  full = False
  verbose = True
  status = None
  until = None
  since = None
  terse = False
  all = False
  reset = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Job':
      try:
        job = int( switch[1] )
      except:
        gLogger.fatal( "Invalid jobID", switch[1] )
    elif switch[0] == 'Transformation':
      try:
        transID = int( switch[1] )
      except:
        gLogger.fatal( 'Invalid transID', switch[1] )
    elif switch[0] == 'Tasks':
      try:
        taskIDs = [int( task ) for task in switch[1].split( ',' )]
      except:
        gLogger.fatal( 'Invalid tasks', switch[1] )
    elif switch[0] == 'Full':
      full = True
    elif switch[0] == 'NotVerbose':
      verbose = False
    elif switch[0] == 'Terse':
      terse = True
    elif switch[0] == 'All':
      all = True
    elif switch[0] == 'Reset':
      reset = True
    elif switch[0] == 'Status':
      status = switch[1].capitalize()
    elif switch[0] == 'Since':
      since = convertDate( switch[1] )
    elif switch[0] == 'Until':
      until = convertDate( switch[1] )

  if reset:
    status = 'Failed'
  if terse:
    verbose = True
  if status:
    if not until:
      until = datetime.datetime.utcnow()
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
      all = True
      requests = [reqName for reqName in args[0].split( ',' ) if reqName]
  else:
    from DIRAC.Interfaces.API.Dirac                              import Dirac
    dirac = Dirac()
    res = dirac.attributes( job )
    if not res['OK']:
      gLogger.error( "Error getting job parameters", res['Message'] )
    else:
      jobName = res['Value'].get( 'JobName' )
      if not jobName:
        gLogger.warn( 'Job %d not found' % job )
      else:
        requests = [jobName + '_job_%d' % job]
        all = True

  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  from DIRAC.RequestManagementSystem.Client.ReqClient import printRequest, recoverableRequest
  reqClient = ReqClient()

  if status:
    all = all or status != 'Failed'
    res = reqClient.getRequestNamesList( [status], limit = 999999999 )
    if not res['OK']:
      gLogger.error( "Error getting requests:", res['Message'] )
      DIRAC.exit( 2 )
    requests = [reqName for reqName, _st, updTime in res['Value'] if updTime > since and updTime <= until and reqName]
    gLogger.always( 'Obtained %d requests %s between %s and %s' % ( len( requests ), status, since, until ) )
  elif not requests:
    gLogger.always( 'No request give....' )
    Script.showHelp()
    DIRAC.exit( 2 )
  okRequests = []
  for requestName in requests:
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
      gLogger.error( "no such request %s" % requestName )
      continue

    if all or recoverableRequest( request ):
      okRequests.append( requestName )
      if reset:
        gLogger.always( '============ Request %s =============' % requestName )
        ret = reqClient.resetFailedRequest( requestName )
        if not ret['OK']:
          gLogger.error( "Error resetting request %s" % requestName, ret['Message'] )
      else:
        if len( requests ) > 1:
          gLogger.always( '\n===================================' )
        dbStatus = reqClient.getRequestStatus( requestName ).get( 'Value', 'Unknown' )

        printRequest( request, status = dbStatus, full = full, verbose = verbose, terse = terse )
  if status and okRequests:
    from DIRAC.Core.Utilities.List import breakListIntoChunks
    gLogger.always( '\nList of %d selected requests:' % len( okRequests ) )
    for reqs in breakListIntoChunks( okRequests, 100 ):
      gLogger.always( ','.join( reqs ) )



