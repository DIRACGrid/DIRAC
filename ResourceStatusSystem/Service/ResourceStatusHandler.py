################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC                                             import gConfig, S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler                   import RequestHandler

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB    import ResourceStatusDB
from DIRAC.ResourceStatusSystem.Utilities              import Utils
db = False

def initializeResourceStatusHandler( _serviceInfo ):

  global db
  db = ResourceStatusDB()

# Publisher is on boxes right now
#
#  rmDB = ResourceStatusDB()
#  cc = CommandCaller()
#  global VOExtension
#  VOExtension = getExt()
#  ig = InfoGetter( VOExtension )
#  WMSAdmin = RPCClient( "WorkloadStatus/WMSAdministrator" )
#  global publisher
#  publisher = Publisher( VOExtension, dbIn = db, commandCallerIn = cc,
#                         infoGetterIn = ig, WMSAdminIn = WMSAdmin )

  SyncModule = Utils.voimport("DIRAC.ResourceStatusSystem.Utilities.Synchronizer")
  sync_O = SyncModule.Synchronizer()
  gConfig.addListenerToNewVersionEvent( sync_O.sync )
  return S_OK()

################################################################################

class ResourceStatusHandler( RequestHandler ):
  '''
  The ResourceStatusHandler exposes the DB front-end functions through a XML-RPC
  server, functionalities inherited from
  :class:`DIRAC.Core.DISET.RequestHandler.RequestHandler`

  According to the ResourceStatusDB philosophy, only functions of the type:
  - insert
  - update
  - get
  - delete

  are exposed. If you need anything more complicated, either look for it on the
  :class:`ResourceStatusClient`, or code it yourself. This way the DB and the
  Service are kept clean and tidied.

  To can use this service on this way, but you MUST NOT DO IT. Use it through the
  :class:`ResourceStatusClient`. If offers in the worst case as good performance
  as the :class:`ResourceStatusHandler`, if not better.

   >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
   >>> server = RPCCLient("ResourceStatus/ResourceStatus")
  '''

  def setDatabase( self, database ):
    '''
    This method let us inherit from this class and overwrite the database object
    without having problems with the global variables.

    :Parameters:
      **database** - `MySQL`
        database used by this handler

    :return: None
    '''
    global db
    db = database

  types_insert = [ dict, dict ]
  def export_insert( self, params, meta ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).

      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''

    gLogger.info( 'insert: %s %s' % ( params, meta ) )
    
    try:
      res = db.insert( params, meta )
      gLogger.debug( 'insert %s' % res )
    except Exception, e:
      _msg = 'Exception calling db.insert: \n %s' % e
      gLogger.exception( _msg )
      res = S_ERROR( _msg )
    
    return res   

  types_update = [ dict, dict ]
  def export_update( self, params, meta ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).

      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''

    gLogger.info( 'update: %s %s' % ( params, meta ) )
    
    try:
      res = db.update( params, meta )
      gLogger.debug( 'update %s' % res )
    except Exception, e:
      _msg = 'Exception calling db.update: \n %s' % e
      gLogger.exception( _msg )
      res = S_ERROR( _msg )
    
    return res   

  types_get = [ dict, dict ]
  def export_get( self, params, meta ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It \
    does not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).

      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''

    gLogger.info( 'get: %s %s' % ( params, meta ) )
    
    try:
      res = db.get( params, meta )
      gLogger.debug( 'get %s' % res )
    except Exception, e:
      _msg = 'Exception calling db.get: \n %s' % e
      gLogger.exception( _msg )
      res = S_ERROR( _msg )
    
    return res   

  types_delete = [ dict, dict ]
  def export_delete( self, params, meta ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).

      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''

    gLogger.info( 'delete: %s %s' % ( params, meta ) )
    
    try:
      res = db.delete( params, meta )
      gLogger.debug( 'delete %s' % res )
    except Exception, e:
      _msg = 'Exception calling db.delete: \n %s' % e
      gLogger.exception( _msg )
      res = S_ERROR( _msg )
    
    return res   

#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

#################################################################################
##
##  Cleaning ongoing
##
#################################################################################
##
##  types_getPeriods = [ str, str, str, int ]
##  def export_getPeriods( self, granularity, name, status, hours ):
##    """ get periods of time when name was in status (for a total of hours hours)
##    """
##
##    gLogger.info( "ResourceStatusHandler.getPeriods: Attempting to get %s periods when it was in %s" % ( name, status ) )
##
##    try:
##      resQuery = rsDB.getPeriods( granularity, name, status, int( hours ) )
##      gLogger.info( "ResourceStatusHandler.getPeriods: got %s periods" % name )
##      return resQuery
##    except RSSDBException, x:
##      errorStr = whoRaised( x )
##    except RSSException, x:
##      errorStr = whoRaised( x )
##    except Exception, x:
##      errorStr = whoRaised( x )
##
##    errorStr += '\n ' + where( self, self.export_getPeriods )
##    return S_ERROR( errorStr )
##
##############################################################################
##
##  types_getDownTimesWeb = [dict, list, int, int]
##  def export_getDownTimesWeb(self, selectDict, sortList, startItem, maxItems):
##    """ get down times as registered with the policies.
##        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getDownTimesWeb`
##
##        :Parameters:
##          `selectDict`
##            {
##              'Granularity':'Site', 'Resource', or a list with both
##              'Severity':'OUTAGE', 'AT_RISK', or a list with both
##            }
##
##          `sortList`
##            [] (no sorting provided)
##
##          `startItem`
##
##          `maxItems`
##
##        :return:
##        {
##          'OK': XX,
##
##          'rpcStub': XX, 'getDownTimesWeb', ({}, [], X, X)),
##
##          Value':
##          {
##
##            'ParameterNames': ['Granularity', 'Name', 'Severity', 'When'],
##
##            'Records': [[], [], ...]
##
##            'TotalRecords': X,
##
##            'Extras': {},
##          }
##        }
##    """
##    try:
##      gLogger.info("ResourceStatusHandler.getDownTimesWeb: Attempting to get down times list")
##      try:
##        try:
##          granularity = selectDict['Granularity']
##        except KeyError:
##          granularity = []
##
##        if not isinstance(granularity, list):
##          granularity = [granularity]
##        commands = []
##        if granularity == []:
##          commands = ['DTEverySites', 'DTEveryResources']
##        elif 'Site' in granularity:
##          commands.append('DTEverySites')
##        elif 'Resource' in granularity:
##          commands.append('DTEveryResources')
##
##        try:
##          severity = selectDict['Severity']
##        except KeyError:
##          severity = []
##        if not isinstance(severity, list):
##          severity = [severity]
##        if severity == []:
##          severity = ['AT_RISK', 'OUTAGE']
##
##        res = rsDB.getClientsCacheStuff(['Name', 'Opt_ID', 'Value', 'Result', 'CommandName'],
##                                        commandName = commands)
##        records = []
##
##        if not ( res == () ):
##          made_IDs = []
##
##          for dt_tuple in res:
##            considered_ID = dt_tuple[1]
##            if considered_ID not in made_IDs:
##              name = dt_tuple[0]
##              if dt_tuple[4] == 'DTEverySites':
##                granularity = 'Site'
##              elif dt_tuple[4] == 'DTEveryResources':
##                granularity = 'Resource'
##              toTake = ['Severity', 'StartDate', 'EndDate', 'Description']
##
##              for dt_t in res:
##                if considered_ID == dt_t[1]:
##                  if toTake != []:
##                    if dt_t[2] in toTake:
##                      if dt_t[2] == 'Severity':
##                        sev = dt_t[3]
##                        toTake.remove('Severity')
##                      if dt_t[2] == 'StartDate':
##                        startDate = dt_t[3]
##                        toTake.remove('StartDate')
##                      if dt_t[2] == 'EndDate':
##                        endDate = dt_t[3]
##                        toTake.remove('EndDate')
##                      if dt_t[2] == 'Description':
##                        description = dt_t[3]
##                        toTake.remove('Description')
##
##              now = datetime.datetime.utcnow().replace(microsecond = 0, second = 0)
##              startDate_datetime = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M')
##              endDate_datetime = datetime.datetime.strptime(endDate, '%Y-%m-%d %H:%M')
##
##              if endDate_datetime < now:
##                when = 'Finished'
##              else:
##                if startDate_datetime < now:
##                  when = 'OnGoing'
##                else:
##                  hours = str(convertTime(startDate_datetime - now, 'hours'))
##                  when = 'In ' + hours + ' hours.'
##
##              if sev in severity:
##                records.append([ considered_ID, granularity, name, sev,
##                                when, startDate, endDate, description ])
##
##              made_IDs.append(considered_ID)
##
##        # adding downtime links to the GOC DB page in Extras
##        DT_links = []
##        for record in records:
##          DT_link = rsDB.getClientsCacheStuff(['Result'], opt_ID = record[0], value = 'Link')
##          DT_link = DT_link[0][0]
##          DT_links.append({ record[0] : DT_link } )
##
##        paramNames = ['ID', 'Granularity', 'Name', 'Severity', 'When', 'Start', 'End', 'Description']
##
##        finalDict = {}
##        finalDict['TotalRecords'] = len(records)
##        finalDict['ParameterNames'] = paramNames
##
##        # Return all the records if maxItems == 0 or the specified number otherwise
##        if maxItems:
##          finalDict['Records'] = records[startItem:startItem+maxItems]
##        else:
##          finalDict['Records'] = records
##
##        finalDict['Extras'] = DT_links
##
##
##      except RSSDBException, x:
##        gLogger.error(whoRaised(x))
##      except RSSException, x:
##        gLogger.error(whoRaised(x))
##      gLogger.info("ResourceStatusHandler.getDownTimesWeb: got DT list")
##      return S_OK(finalDict)
##    except Exception:
##      errorStr = where(self, self.export_getDownTimesWeb)
##      gLogger.exception(errorStr)
##      return S_ERROR(errorStr)
##
##############################################################################
##
##  types_enforcePolicies = [str, str, BooleanType]
##  def export_enforcePolicies(self, granularity, name, useNewRes = True):
##    """ Enforce all the policies. If `useNewRes` is False, use cached results only (where available).
##    """
##    try:
##      gLogger.info("ResourceStatusHandler.enforcePolicies: Attempting to enforce policies for %s %s" % (granularity, name))
##      try:
##        reason = serviceType = resourceType = None
##
##        res = rsDB.getStuffToCheck(granularity, name = name)[0]
##        status = res[1]
##        formerStatus = res[2]
##        siteType = res[3]
##        tokenOwner = res[len(res)-1]
##        if granularity == 'Resource':
##          resourceType = res[4]
##        elif granularity == 'Service':
##          serviceType = res[4]
##
##        from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
##        pep = PEP(VOExtension, granularity, name, status, formerStatus, reason, siteType,
##                  serviceType, resourceType, tokenOwner, useNewRes)
##        pep.enforce(rsDBIn = rsDB)
##
##      except RSSDBException, x:
##        gLogger.error(whoRaised(x))
##      except RSSException, x:
##        gLogger.error(whoRaised(x))
##      gLogger.info("ResourceStatusHandler.enforcePolicies: enforced for %s: %s" % (granularity, name))
##      return S_OK("ResourceStatusHandler.enforcePolicies: enforced for %s: %s" % (granularity, name))
##    except Exception:
##      errorStr = where(self, self.export_getCachedResult)
##      gLogger.exception(errorStr)
##      return S_ERROR(errorStr)
##
##############################################################################
##
##  types_publisher = [str, str, BooleanType]
##  def export_publisher(self, granularity, name, useNewRes = False):
##    """ get a view
##
##    :Parameters:
##      `granularity`
##        string - a ValidRes
##
##      `name`
##        string - name of the res
##
##      `useNewRes`
##        boolean. When set to true, will get new results,
##        otherwise it will get cached results (where available).
##    """
##    try:
##      gLogger.info("ResourceStatusHandler.publisher: Attempting to get info for %s: %s" % (granularity, name))
##      try:
##        if useNewRes == True:
##          from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
##          gLogger.info("ResourceStatusHandler.publisher: Recalculating policies for %s: %s" % (granularity, name))
##          if granularity in ('Site', 'Sites'):
##            res = rsDB.getStuffToCheck(granularity, name = name)[0]
##            status = res[1]
##            formerStatus = res[2]
##            siteType = res[3]
##            tokenOwner = res[4]
##
##            pep = PEP(VOExtension, granularity, name, status, formerStatus, None, siteType,
##                      None, None, tokenOwner, useNewRes)
##            pep.enforce(rsDBIn = rsDB)
##
##            res = rsDB.getMonitoredsList('Service', paramsList = ['ServiceName'], siteName = name)
##            services = [x[0] for x in res]
##            for s in services:
##              res = rsDB.getStuffToCheck('Service', name = s)[0]
##              status = res[1]
##              formerStatus = res[2]
##              siteType = res[3]
##              serviceType = res[4]
##
##              pep = PEP(VOExtension, 'Service', s, status, formerStatus, None, siteType,
##                        serviceType, None, tokenOwner, useNewRes)
##              pep.enforce(rsDBIn = rsDB)
##          else:
##            reason = serviceType = resourceType = None
##
##            res = rsDB.getStuffToCheck(granularity, name = name)[0]
##            status = res[1]
##            formerStatus = res[2]
##            siteType = res[3]
##            tokenOwner = res[len(res)-1]
##            if granularity == 'Resource':
##              resourceType = res[4]
##            elif granularity == 'Service':
##              serviceType = res[4]
##
##            from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
##            pep = PEP(VOExtension, granularity, name, status, formerStatus, reason, siteType,
##                      serviceType, resourceType, tokenOwner, useNewRes)
##            pep.enforce(rsDBIn = rsDB)
##
##        res = publisher.getInfo(granularity, name, useNewRes)
##      except InvalidRes, x:
##        errorStr = "Invalid granularity"
##        gLogger.exception(whoRaised(x) + errorStr)
##        return S_ERROR(errorStr)
##      except RSSException, x:
##        errorStr = "RSSException"
##        gLogger.exception(whoRaised(x) + errorStr)
##      gLogger.info("ResourceStatusHandler.publisher: got info for %s: %s" % (granularity, name))
##      return S_OK(res)
##    except Exception:
##      errorStr = where(self, self.export_publisher)
##      gLogger.exception(errorStr)
##      return S_ERROR(errorStr)
##
##############################################################################
##
##  types_reAssignToken = [ str, str, str ]
##  def export_reAssignToken( self, granularity, name, requester ):
##    """
##    Re-assign a token: if it was assigned to a human, assign it to 'RS_SVC' and viceversa.
##    """
##
##    str_ = "ResourceStatusHandler.reAssignToken: attempting to re-assign token "
##    str_ = str_ + "%s: %s: %s" % ( granularity, name, requester )
##    gLogger.info( str_ )
##
##    try:
##      token      = rsDB.getTokens( granularity, name = name )
##      tokenOwner = token[ 0 ][ 1 ]
##      if tokenOwner == 'RS_SVC':
##        if requester != 'RS_SVC':
##          rsDB.setToken( granularity, name, requester, datetime.utcnow() + timedelta( hours = 24 ) )
##      else:
##        rsDB.setToken( granularity, name, 'RS_SVC', datetime( 9999, 12, 31, 23, 59, 59 ) )
##
##      gLogger.info( "ResourceStatusHandler.reAssignToken: re-assigned token %s: %s: %s" % ( granularity, name, requester ) )
##      return S_OK()
##    except RSSDBException, x:
##      errorStr = whoRaised( x )
##    except RSSException, x:
##      errorStr = whoRaised( x )
##    except Exception, x:
##      errorStr = whoRaised( x )
##
##    errorStr += '\n ' + where( self, self.export_reAssignToken )
##    return S_ERROR( errorStr )
##
##############################################################################
##
##  types_extendToken = [ str, str, int ]
##  def export_extendToken( self, granularity, name, hrs ):
##    """
##    Extend the duration of token by the number of provided hours.
##    """
##
##    str_ = "ResourceStatusHandler.extendToken: attempting to extend token "
##    str_ = str_ + "%s: %s for %i hours" % ( granularity, name, hrs )
##    gLogger.info( str_ )
##
##    try:
##      token              = rsDB.getTokens( granularity, name )
##      tokenOwner         = token[ 0 ][ 1 ]
##      tokenExpiration    = token[ 0 ][ 2 ]
##      tokenNewExpiration = tokenExpiration
##      try:
##        tokenNewExpiration = tokenExpiration + timedelta( hours = hrs )
##      except OverflowError:
##        pass
##      rsDB.setToken( granularity, name, tokenOwner, tokenNewExpiration )
##      gLogger.info( "ResourceStatusHandler.extendToken: extended token %s: %s for %i hours" % ( granularity, name, hrs ) )
##      return S_OK()
##    except RSSDBException, x:
##      errorStr = whoRaised( x )
##    except RSSException, x:
##      errorStr = whoRaised( x )
##    except Exception, x:
##      errorStr = whoRaised( x )
##
##    errorStr += '\n ' + where( self, self.export_extendToken )
##    return S_ERROR( errorStr )
#