################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC                                              import S_OK
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Utilities.Decorators    import HandlerDec

db = False

def initializeResourceManagementHandler( _serviceInfo ):

  global db
  db = ResourceManagementDB()

  return S_OK()

################################################################################

class ResourceManagementHandler( RequestHandler ):
  '''
  The ResourceManagementHandler exposes the DB front-end functions through a
  XML-RPC server, functionalities inherited from :class:`DIRAC.Core.DISET.Reques\
  tHandler.RequestHandler`
  
  According to the ResourceManagementDB philosophy, only functions of the type:
  - insert
  - update
  - get
  - delete 
  
  are exposed. If you need anything more complicated, either look for it on the 
  :class:`ResourceManagementClient`, or code it yourself. This way the DB and the 
  Service are kept clean and tidied.

  To can use this service on this way, but you MUST NOT DO IT. Use it through the
  :class:`ResourceManagementClient`. If offers in the worst case as good perfor\
  mance as the :class:`ResourceManagementHandler`, if not better.

   >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
   >>> server = RPCCLient("ResourceStatus/ResourceManagement")
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
  @HandlerDec
  def export_insert( self, params, meta ):
    '''   
    This method is a bridge to access :class:`ResourceManagementDB` remotely. It 
    does not add neither processing nor validation. If you need to know more 
    about this method, you must keep reading on the database documentation.     
      
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    
      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # It returns a db object, which is picked by the decorator and return whatever
    # the insert method returns ( db.insert )    
    return db

  types_update = [ dict, dict ]
  @HandlerDec
  def export_update( self, params, meta ):
    '''   
    This method is a bridge to access :class:`ResourceManagementDB` remotely. It 
    does not add neither processing nor validation. If you need to know more 
    about this method, you must keep reading on the database documentation.     
      
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    
      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''      
    # It returns a db object, which is picked by the decorator and return whatever
    # the update method returns ( db.update )    
    return db

  types_get = [ dict, dict ]
  @HandlerDec
  def export_get( self, params, meta ):
    '''
    This method is a bridge to access :class:`ResourceManagementDB` remotely. 
    It does not add neither processing nor validation. If you need to know more\ 
    about this method, you must keep reading on the database documentation.     
      
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    
      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''      
    # It returns a db object, which is picked by the decorator and return whatever
    # the get method returns ( db.get )    
    return db

  types_delete = [ dict, dict ]
  @HandlerDec
  def export_delete( self, params, meta ):
    '''   
    This method is a bridge to access :class:`ResourceManagementDB` remotely.\
    It does not add neither processing nor validation. If you need to know more \
    about this method, you must keep reading on the database documentation.     
      
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    
      **meta** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''         
    # It returns a db object, which is picked by the decorator and return whatever
    # the delete method returns ( db.delete )    
    return db
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

################################################################################
#
# Cleaning ongoing
#
################################################################################

##############################################################################
#
#  types_getDownTimesWeb = [ DictType, ListType, IntType, IntType ]
#  def export_getDownTimesWeb( self, selectDict, _sortList, startItem, maxItems ):
#    """ get down times as registered with the policies.
#        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getDownTimesWeb`
#
#        :Parameters:
#          `selectDict`
#            {
#              'Granularity':'Site', 'Resource', or a list with both
#              'Severity':'OUTAGE', 'AT_RISK', or a list with both
#            }
#
#          `sortList`
#            [] (no sorting provided)
#
#          `startItem`
#
#          `maxItems`
#
#        :return:
#        {
#          'OK': XX,
#
#          'rpcStub': XX, 'getDownTimesWeb', ({}, [], X, X)),
#
#          Value':
#          {
#
#            'ParameterNames': ['Granularity', 'Name', 'Severity', 'When'],
#
#            'Records': [[], [], ...]
#
#            'TotalRecords': X,
#
#            'Extras': {},
#          }
#        }
#    """
#
#    gLogger.info( "ResourceManagementHandler.getDownTimesWeb: Attempting to get down times list" )
#
#    try:
#
#      finalDict = {}
#
#      try:
#        try:
#          granularity = selectDict[ 'Granularity' ]
#        except KeyError:
#          granularity = []
#
#        if not isinstance( granularity, list ):
#          granularity = [ granularity ]
#        commands = []
#        if granularity == []:
#          commands = [ 'DTEverySites', 'DTEveryResources' ]
#        elif 'Site' in granularity:
#          commands.append( 'DTEverySites' )
#        elif 'Resource' in granularity:
#          commands.append( 'DTEveryResources' )
#
#        try:
#          severity = selectDict[ 'Severity' ]
#        except KeyError:
#          severity = []
#        if not isinstance( severity, list ):
#          severity = [ severity ]
#        if severity == []:
#          severity = [ 'AT_RISK', 'OUTAGE' ]
#
#        res = db.getClientsCacheStuff( [ 'Name', 'Opt_ID', 'Value', 'Result', 'CommandName' ],
#                                         commandName = commands )
#        records = []
#
#        if not ( res == () ):
#          made_IDs = []
#
#          for dt_tuple in res:
#            considered_ID = dt_tuple[ 1 ]
#            if considered_ID not in made_IDs:
#              name = dt_tuple[ 0 ]
#              if dt_tuple[ 4 ] == 'DTEverySites':
#                granularity = 'Site'
#              elif dt_tuple[ 4 ] == 'DTEveryResources':
#                granularity = 'Resource'
#              toTake = [ 'Severity', 'StartDate', 'EndDate', 'Description' ]
#
#              for dt_t in res:
#                if considered_ID == dt_t[ 1 ]:
#                  if toTake != []:
#                    if dt_t[ 2 ] in toTake:
#                      if dt_t[ 2 ] == 'Severity':
#                        sev = dt_t[ 3 ]
#                        toTake.remove( 'Severity' )
#                      if dt_t[ 2 ] == 'StartDate':
#                        startDate = dt_t[ 3 ]
#                        toTake.remove( 'StartDate' )
#                      if dt_t[ 2 ] == 'EndDate':
#                        endDate = dt_t[ 3 ]
#                        toTake.remove( 'EndDate' )
#                      if dt_t[ 2 ] == 'Description':
#                        description = dt_t[ 3 ]
#                        toTake.remove( 'Description' )
#
#              now                = datetime.datetime.utcnow().replace( microsecond = 0, second = 0 )
#              startDate_datetime = datetime.datetime.strptime( startDate, '%Y-%m-%d %H:%M' )
#              endDate_datetime   = datetime.datetime.strptime( endDate, '%Y-%m-%d %H:%M' )
#
#              if endDate_datetime < now:
#                when = 'Finished'
#              else:
#                if startDate_datetime < now:
#                  when = 'OnGoing'
#                else:
#                  hours = str( convertTime( startDate_datetime - now, 'hours' ) )
#                  when = 'In ' + hours + ' hours.'
#
#              if sev in severity:
#                records.append( [ considered_ID, granularity, name, sev,
#                                 when, startDate, endDate, description ])
#
#              made_IDs.append( considered_ID )
#
#        # adding downtime links to the GOC DB page in Extras
#        DT_links = []
#        for record in records:
#          DT_link = db.getClientsCacheStuff( [ 'Result' ], opt_ID = record[ 0 ], value = 'Link' )
#          DT_link = DT_link[ 0 ][ 0 ]
#          DT_links.append( { record[ 0 ] : DT_link } )
#
#        paramNames = [ 'ID', 'Granularity', 'Name', 'Severity', 'When', 'Start', 'End', 'Description' ]
#
#        finalDict[ 'TotalRecords' ]   = len( records )
#        finalDict[ 'ParameterNames' ] = paramNames
#
#        # Return all the records if maxItems == 0 or the specified number otherwise
#        if maxItems:
#          finalDict[ 'Records' ] = records[ startItem:startItem+maxItems ]
#        else:
#          finalDict[ 'Records' ] = records
#
#        finalDict[ 'Extras' ] = DT_links
#
#      except RSSDBException, x:
#        gLogger.error( whoRaised( x ) )
#      except RSSException, x:
#        gLogger.error( whoRaised( x ) )
#
#      gLogger.info( "ResourceManagementHandler.getDownTimesWeb: got DT list" )
#      return S_OK( finalDict )
#
#    except Exception:
#      errorStr = where( self, self.export_getDownTimesWeb )
#      gLogger.exception( errorStr )
#      return S_ERROR( errorStr )
#
##############################################################################
#
#  types_enforcePolicies = [ StringType, StringType, BooleanType ]
#  def export_enforcePolicies( self, granularity, name, useNewRes = True ):
#    """ Enforce all the policies. If `useNewRes` is False, use cached results only (where available).
#    """
#
#    gLogger.info( "ResourceManagementHandler.enforcePolicies: Attempting to enforce policies for %s %s" % ( granularity, name ) )
#
#    try:
#
#      try:
#        reason = serviceType = resourceType = None
#
#        res          = rsDB.getStuffToCheck( granularity, name = name )[ 0 ]
#        status       = res[ 1 ]
#        formerStatus = res[ 2 ]
#        siteType     = res[ 3 ]
#        tokenOwner   = res[ len( res ) - 1 ]
#
#        if granularity == 'Resource':
#          resourceType = res[ 4 ]
#        elif granularity == 'Service':
#          serviceType = res[ 4 ]
#
#        from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
#        pep = PEP( VOExtension, granularity, name, status, formerStatus, reason, siteType,
#                   serviceType, resourceType, tokenOwner, useNewRes )
#        pep.enforce( rsDBIn = rsDB, dbIn = db )
#
#      except RSSDBException, x:
#        gLogger.error( whoRaised( x ) )
#      except RSSException, x:
#        gLogger.error( whoRaised( x ) )
#
#      msg = "ResourceManagementHandler.enforcePolicies: enforced for %s: %s" % ( granularity, name )
#      gLogger.info( msg )
#      return S_OK( msg )
#
#    except Exception:
#      errorStr = where( self, self.export_getCachedResult )
#      gLogger.exception( errorStr )
#      return S_ERROR( errorStr )
#
##############################################################################
#
##   types_publisher = [ StringType, StringType, BooleanType ]
##   def export_publisher( self, granularity, name, useNewRes = False ):
##     """ get a view
#
##     :Parameters:
##       `granularity`
##         string - a ValidRes
#
##       `name`
##         string - name of the res
#
##       `useNewRes`
##         boolean. When set to true, will get new results,
##         otherwise it will get cached results (where available).
##     """
#
##     gLogger.info( "ResourceManagementHandler.publisher: Attempting to get info for %s: %s" % ( granularity, name ) )
#
##     try:
#
##       res = []
#
##       try:
##         if useNewRes == True:
##           from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
##           gLogger.info( "ResourceManagementHandler.publisher: Recalculating policies for %s: %s" % ( granularity, name ) )
##           if granularity in ( 'Site', 'Sites' ):
##             res = rsDB.getStuffToCheck( granularity, name = name )[ 0 ]
##             status       = res[ 1 ]
##             formerStatus = res[ 2 ]
##             siteType     = res[ 3 ]
##             tokenOwner   = res[ 4 ]
#
##             pep = PEP( VOExtension, granularity, name, status, formerStatus, None, siteType,
##                        None, None, tokenOwner, useNewRes )
##             pep.enforce( rsDBIn = rsDB, dbIn = db )
#
##             res = rsDB.getMonitoredsList( 'Service', paramsList = [ 'ServiceName' ], siteName = name )
##             services = [ x[ 0 ] for x in res ]
##             for s in services:
##               res = rsDB.getStuffToCheck( 'Service', name = s )[ 0 ]
##               status       = res[ 1 ]
##               formerStatus = res[ 2 ]
##               siteType     = res[ 3 ]
##               serviceType  = res[ 4 ]
#
##               pep = PEP( VOExtension, 'Service', s, status, formerStatus, None, siteType,
##                          serviceType, None, tokenOwner, useNewRes )
##               pep.enforce( rsDBIn = rsDB, db = db )
##           else:
##             reason = serviceType = resourceType = None
#
##             res = rsDB.getStuffToCheck( granularity, name = name )[ 0 ]
##             status       = res[ 1 ]
##             formerStatus = res[ 2 ]
##             siteType     = res[ 3 ]
##             tokenOwner   = res[ len( res ) - 1 ]
#
##             if granularity == 'Resource':
##               resourceType = res[ 4 ]
##             elif granularity == 'Service':
##               serviceType = res[ 4 ]
#
## #            from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
##             pep = PEP( VOExtension, granularity, name, status, formerStatus, reason, siteType,
##                        serviceType, resourceType, tokenOwner, useNewRes )
##             pep.enforce( rsDBIn = rsDB, dbIn = db )
#
##         res = publisher.getInfo( granularity, name, useNewRes )
##       except InvalidRes, x:
##         errorStr = "Invalid granularity"
##         gLogger.exception( whoRaised( x ) + errorStr )
##         return S_ERROR( errorStr )
##       except RSSException, x:
##         errorStr = "RSSException"
##         gLogger.exception( whoRaised( x ) + errorStr )
#
##       gLogger.info( "ResourceManagementHandler.publisher: got info for %s: %s" % ( granularity, name ) )
##       return S_OK( res )
#
##     except Exception:
##       errorStr = where( self, self.export_publisher )
##       gLogger.exception( errorStr )
##       return S_ERROR( errorStr )
#
##############################################################################
#
## User Registry Functions
#
#  types_registryAddUser = [str, str, str]
#  def export_registryAddUser(self, login, name, email):
#    gLogger.info( "ResourceManagementHandler.registryAddUser: Attempting to add user on registry cache" )
#
#    try:
#      return S_OK(db.registryAddUser(login, name, email))
#
#    except RSSManagementDBException:
#      errorStr = where( self, self.export_registryAddUser )
#      gLogger.exception( errorStr )
#      return S_ERROR( errorStr )
#
#
#  types_registryDelUser = [str]
#  def export_registryDelUser(self, _name):
#    return S_ERROR("Not implemented.")
#
#  types_registryGetMailFromLogin = [list]
#  def export_registryGetMailFromLogin(self, logins):
#    gLogger.info( "ResourceManagementHandler.registryGetMailFromLogin" )
#    try:
#      S_OK([db.registryGetMailFromLogin(l) for l in logins])
#
#    except RSSManagementDBException:
#      errorStr = where( self, self.export_registryAddUser )
#      gLogger.exception( errorStr )
#      return S_ERROR( errorStr )
#
#  types_registryGetMailFromName = [list]
#  def export_registryGetMailFromName(self, names):
#    gLogger.info( "ResourceManagementHandler.registryGetMailFromName" )
#    try:
#      S_OK([db.registryGetMailFromName(n) for n in names])
#
#    except RSSManagementDBException:
#      errorStr = where( self, self.export_registryAddUser )
#      gLogger.exception( errorStr )
#      return S_ERROR( errorStr )