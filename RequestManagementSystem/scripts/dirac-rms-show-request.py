#!/bin/env python
""" Show request given its name, a jobID or a transformation and a task """
__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.registerSwitch( '', 'Job=', '   = JobID' )
Script.registerSwitch( '', 'Transformation=', '   = transID' )
Script.registerSwitch( '', 'Tasks=', '   = list of taskIDs' )
Script.registerSwitch( '', 'Verbose', '   Print more information' )
Script.registerSwitch( '', 'Full', '   Print full request' )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] [requestName|requestID]' % Script.scriptName,
                                     'Arguments:',
                                     ' requestName: a request name' ] ) )

output = ''
def prettyPrint( mainItem, key = '', offset = 0 ):
  global output
  if key:
    key += ': '
  blanks = offset * ' '
  if mainItem and type( mainItem ) == type( {} ):
    output += "%s%s%s\n" % ( blanks, key, '{' ) if blanks or key else ''
    for key in sorted( mainItem ):
      prettyPrint( mainItem[key], key = key, offset = offset )
    output += "%s%s\n" % ( blanks, '}' ) if blanks else ''
  elif mainItem and type( mainItem ) == type( [] ):
    output += "%s%s%s\n" % ( blanks, key, '[' )
    for item in mainItem:
      prettyPrint( item, offset = offset + 2 )
    output += "%s%s\n" % ( blanks, ']' )
  elif type( mainItem ) == type( '' ):
    output += "%s%s'%s'\n" % ( blanks, key, str( mainItem ) )
  else:
    output += "%s%s%s\n" % ( blanks, key, str( mainItem ) )
  output = output.replace( '[\n%s{' % blanks, '[{' ).replace( '}\n%s]' % blanks, '}]' )


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
  verbose = False
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

  if not requests:
    Script.showHelp()
    DIRAC.exit( 2 )

  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  from DIRAC.DataManagementSystem.Client.FTSClient                                  import FTSClient
  reqClient = ReqClient()
  ftsClient = FTSClient()
  for requestName in requests:
    if requestName:

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

      if full:
        output = ''
        prettyPrint( request.toJSON()['Value'] )
        print output
      else:
        anyReplication = False
        gLogger.always( "Request name='%s' ID=%s Status='%s'%s%s" % ( request.RequestName,
                                                                         request.RequestID,
                                                                         request.Status,
                                                                         ( " Error=%s" % request.Error ) if request.Error and request.Error.strip() else "" ,
                                                                         ( " Job=%s" % request.JobID ) if request.JobID else "" ) )
        if verbose:
          gLogger.always( "Created %s, Updated %s" % ( request.CreationTime, request.LastUpdate ) )
          if request.OwnerDN:
            gLogger.always( "Owner: '%s', Group: %s" % ( request.OwnerDN, request.OwnerGroup ) )
        for i, op in enumerate( request ):
          prStr = ''
          if 'Replicate' in op.Type:
            anyReplication = True
          if verbose:
            if op.SourceSE:
              prStr += 'SourceSE: %s' % op.SourceSE
            if op.TargetSE:
              prStr += ( ' - ' if prStr else '' ) + 'TargetSE: %s' % op.TargetSE
            if prStr:
              prStr += ' - '
            prStr += 'Created %s, Updated %s' % ( op.CreationTime, op.LastUpdate )
          gLogger.always( "  [%s] Operation Type='%s' ID=%s Order=%s Status='%s'%s%s" % ( i, op.Type, op.OperationID,
                                                                                               op.Order, op.Status,
                                                                                               ( " Error=%s" % op.Error ) if op.Error and op.Error.strip() else "",
                                                                                               ( " Catalog=%s" % op.Catalog ) if op.Catalog else "" ) )
          if prStr:
            gLogger.always( "      %s" % prStr )
          for j, f in enumerate( op ):
            gLogger.always( "    [%02d] ID=%s LFN='%s' Status='%s'%s%s" % ( j + 1, f.FileID, f.LFN, f.Status,
                                                                                 ( " Error=%s" % f.Error ) if f.Error and f.Error.strip() else "",
                                                                                 ( " Attempts=%d" % f.Attempt ) if f.Attempt > 1 else "" ) )
      # Check if FTS job exists
      if anyReplication:
        res = ftsClient.getFTSJobsForRequest( request.RequestID )
        if res['OK']:
          ftsJobs = res['Value']
          if ftsJobs:
            gLogger.always( '         FTS jobs associated: %s' % ','.join( ['%s (%s)' % ( job.FTSGUID, job.Status ) \
                                                                     for job in ftsJobs] ) )
          else:
            print '         No FTS jobs found for that request'




