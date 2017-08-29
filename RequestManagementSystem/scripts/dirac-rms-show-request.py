#!/bin/env python
""" Show request given its ID, a jobID or a transformation and a task """
__RCSID__ = "$Id: $"

import datetime
def convertDate( date ):
  try:
    value = datetime.datetime.strptime( date, '%Y-%m-%d' )
    return value
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
Script.registerSwitch( '', 'Tasks=', '      Associated to --Transformation, list of taskIDs' )
Script.registerSwitch( '', 'Verbose', '   Print more information' )
Script.registerSwitch( '', 'Terse', '   Only print request status' )
Script.registerSwitch( '', 'Full', '   Print full request' )
Script.registerSwitch( '', 'Status=', '   Select all requests in a given status' )
Script.registerSwitch( '', 'Since=', '      Associated to --Status, start date yyyy-mm-dd or nb of days (default= -one day' )
Script.registerSwitch( '', 'Until=', '      Associated to --Status, end date (default= now' )
Script.registerSwitch( '', 'All', '      (if --Status Failed) all requests, otherwise exclude irrecoverable failures' )
Script.registerSwitch( '', 'Reset', '   Reset Failed files to Waiting if any' )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] requestID/requestName(if unique)' % Script.scriptName,
                                     'Arguments:',
                                     ' requestID: a request ID' ] ) )

# # execution
if __name__ == "__main__":
  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger

  jobs = []
  requestID = 0
  transID = None
  taskIDs = None
  tasks = None
  requests = []
  full = False
  verbose = False
  status = None
  until = None
  since = None
  terse = False
  allR = False
  reset = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Job':
      try:
        jobs = [int( job ) for job in switch[1].split( ',' )]
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
    elif switch[0] == 'Verbose':
      verbose = True
    elif switch[0] == 'Terse':
      terse = True
    elif switch[0] == 'All':
      allR = True
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
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  from DIRAC.RequestManagementSystem.Client.ReqClient import printRequest, recoverableRequest
  reqClient = ReqClient()
  if transID:
    if not taskIDs:
      gLogger.fatal( "If Transformation is set, a list of Tasks should also be set" )
      Script.showHelp()
      DIRAC.exit( 2 )
    # In principle, the task name is unique, so the request name should be unique as well
    # If ever this would not work anymore, we would need to use the transformationClient
    # to fetch the ExternalID
    requests = ['%08d_%08d' % ( transID, task ) for task in taskIDs]
    allR = True

  elif not jobs:
    args = Script.getPositionalArgs()
    if len( args ) == 1:
      allR = True
      requests = [reqID for reqID in args[0].split( ',' ) if reqID]
  else:
    res = reqClient.getRequestIDsForJobs( jobs )
    if not res['OK']:
      gLogger.fatal( "Error getting request for jobs", res['Message'] )
      DIRAC.exit( 2 )
    if res['Value']['Failed']:
      gLogger.error( "No request found for jobs %s" % ','.join( sorted( str( job ) for job in res['Value']['Failed'] ) ) )
    requests = sorted( res['Value']['Successful'].values() )
    if requests:
      allR = True


  if status and not requests:
    allR = allR or status != 'Failed'
    res = reqClient.getRequestIDsList( [status], limit = 999999999, since = since, until = until )

    if not res['OK']:
      gLogger.error( "Error getting requests:", res['Message'] )
      DIRAC.exit( 2 )
    requests = [reqID for reqID, _st, updTime in res['Value'] if updTime > since and updTime <= until and reqID]
    gLogger.always( 'Obtained %d requests %s between %s and %s' % ( len( requests ), status, since, until ) )
  if not requests:
    gLogger.always( 'No request selected....' )
    Script.showHelp()
    DIRAC.exit( 2 )
  okRequests = []
  warningPrinted = False
  for reqID in requests:
    # We allow reqID to be the requestName if it is unique
    try:
      requestID = int( reqID )
    except ValueError:
      requestID = reqClient.getRequestIDForName( reqID )
      if not requestID['OK']:
        gLogger.always( requestID['Message'] )
        continue
      requestID = requestID['Value']

    request = reqClient.peekRequest( requestID )
    if not request["OK"]:
      gLogger.error( request["Message"] )
      DIRAC.exit( -1 )

    request = request["Value"]
    if not request:
      gLogger.error( "no such request %s" % requestID )
      continue
    if status and request.Status != status:
      if not warningPrinted:
        gLogger.always( "Some requests are not in status %s" % status )
        warningPrinted = True
      continue

    if allR or recoverableRequest( request ):
      okRequests.append( str( requestID ) )
      if reset:
        gLogger.always( '============ Request %s =============' % requestID )
        ret = reqClient.resetFailedRequest( requestID, allR = allR )
        if not ret['OK']:
          gLogger.error( "Error resetting request %s" % requestID, ret['Message'] )
      else:
        if len( requests ) > 1:
          gLogger.always( '\n===================================' )
        dbStatus = reqClient.getRequestStatus( requestID ).get( 'Value', 'Unknown' )

        printRequest( request, status = dbStatus, full = full, verbose = verbose, terse = terse )
  if status and okRequests:
    from DIRAC.Core.Utilities.List import breakListIntoChunks
    gLogger.always( '\nList of %d selected requests:' % len( okRequests ) )
    for reqs in breakListIntoChunks( okRequests, 100 ):
      gLogger.always( ','.join( reqs ) )



