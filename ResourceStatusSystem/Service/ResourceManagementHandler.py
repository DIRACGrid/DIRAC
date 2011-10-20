################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from datetime                                           import datetime
from types                                              import NoneType

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
  XML-RPC server.
  
  According to the ResourceManagementDB philosophy, only functions of the type:
    o insert
    o update
    o get
    o delete 
  
  are exposed. If you need anything more complicated, either look for it on the 
  ResourceManagementClient, or code it yourself. This way the DB and the service 
  keep clean and tidied.

  To can use this service on this way, but you MUST NOT DO IT. Use it through the
  ResourceManagementClient. If offers in the worst case as good performance as the 
  ResourceManagementHandler, if not better.

   >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
   >>> server = RPCCLient("ResourceStatus/ResourceManagement")
   
  If you want to know more about ResourceManagementHandler, scroll down to the 
  end of the file.  
  '''
  
  def initialize( self ):
    pass

  def setDatabase( self, oDatabase ):
    '''
    Needed to inherit without messing up global variables, and get the
    extended DB object
    '''
    global db
    db = oDatabase

################################################################################
################################################################################

  '''
  ##############################################################################
  # ENVIRONMENT CACHE FUNCTIONS
  ##############################################################################
  '''
  __envCache_IU = [ str, str, str ]
  __envCache_GD = [ ( t, list, NoneType ) for t in __envCache_IU ] + [ dict ]
  
  types_insertEnvironmentCache = __envCache_IU
  @HandlerDec
  def export_insertEnvironmentCache( self, hashEnv, siteName, environment ):
    return db
  
  types_updateEnvironmentCache = __envCache_IU
  @HandlerDec
  def export_updateEnvironmentCache( self, hashEnv, siteName, environment ):
    return db
  
  types_getEnvironmentCache = __envCache_GD
  @HandlerDec 
  def export_getEnvironmentCache( self, hashEnv, siteName, environment, kwargs ):
    return db

  types_deleteEnvironmentCache = __envCache_GD   
  @HandlerDec
  def export_deleteEnvironmentCache( self, hashEnv, siteName, environment, kwargs ):
    return db
  
################################################################################
################################################################################
  
  '''
  ##############################################################################
  # POLICY RESULT FUNCTIONS
  ##############################################################################
  '''
  __polRes_IU = [ str, str, str, str, str, str, datetime, datetime ]
  __polRes_GD = [ ( t, list, NoneType ) for t in __polRes_IU ] + [ dict ]
  
  types_insertPolicyResult = __polRes_IU
  @HandlerDec
  def export_insertPolicyResult( self, granularity, name, policyName, statusType,
                                 status, reason, dateEffective, lastCheckTime ):
    return db

  types_updatePolicyResult = __polRes_IU 
  @HandlerDec
  def export_updatePolicyResult( self, granularity, name, policyName, statusType,
                                 status, reason, dateEffective, lastCheckTime ):
    return db
  
  types_getPolicyResult = __polRes_GD
  @HandlerDec
  def export_getPolicyResult( self, granularity, name, policyName, statusType, 
                              status, reason, dateEffective, lastCheckTime, 
                              kwargs ):
    return db

  types_deletePolicyResult = __polRes_GD  
  @HandlerDec
  def export_deletePolicyResult( self, granularity, name, policyName, statusType, 
                                 status, reason, dateEffective, lastCheckTime, 
                                 kwargs ):
    return db

################################################################################
################################################################################
  
  '''
  ##############################################################################
  # CLIENT CACHE FUNCTIONS
  ##############################################################################
  '''  
  __clienCache_IU = [ str, str, str, str, str, datetime, datetime ]
  __clienCache_GD = [ ( t, list, NoneType ) for t in __clienCache_IU ] + [ dict ]
  
  types_insertClientCache = __clienCache_IU
  @HandlerDec
  def export_insertClientCache( self, name, commandName, opt_ID, value, result,
                                dateEffective, lastCheckTime ):
    return db

  types_updateClientCache = __clienCache_IU
  @HandlerDec
  def export_updateClientCache( self, name, commandName, opt_ID, value, result,
                                dateEffective, lastCheckTime ):
    return db
  
  types_getClientCache = __clienCache_GD
  @HandlerDec
  def export_getClientCache( self, name, commandName, opt_ID, value, result,
                             dateEffective, lastCheckTime, kwargs ):
    return db              

  types_deleteClientCache = __clienCache_GD
  @HandlerDec
  def export_deleteClientCache( self, name, commandName, opt_ID, value, result,
                                dateEffective, lastCheckTime, kwargs ):
    return db  

################################################################################
################################################################################

  '''
  ##############################################################################
  # ACCOUNTING CACHE FUNCTIONS
  ##############################################################################
  '''  
  __acCache_IU = [ str, str, str, str, datetime, datetime ]
  __acCache_GD = [ ( t, list, NoneType ) for t in __acCache_IU ] + [ dict ]
  
  types_insertAccountingCache = __acCache_IU 
  @HandlerDec
  def export_insertAccountingCache( self, name, plotType, plotName, result, 
                                    dateEffective, lastCheckTime ):
    return db

  types_updateAccountingCache = __acCache_IU
  @HandlerDec
  def export_updateAccountingCache( self, name, plotType, plotName, result, 
                                    dateEffective, lastCheckTime ):
    return db
  
  types_getAccountingCache = __acCache_GD
  @HandlerDec
  def export_getAccountingCache( self, name, plotType, plotName, result, 
                                 dateEffective, lastCheckTime, kwargs ):
    return db  

  types_deleteAccountingCache = __acCache_GD
  @HandlerDec
  def export_deleteAccountingCache( self, name, plotType, plotName, result, 
                                    dateEffective, lastCheckTime, kwargs ):
    return db

################################################################################
################################################################################

  '''
  ##############################################################################
  # USER REGISTRY FUNCTIONS
  ##############################################################################
  '''  
  __usrReg_IU = [ str, str, str ]
  __usrReg_GD = [ ( t, list, NoneType ) for t in __usrReg_IU ] + [ dict ]
  
  types_insertUserRegistryCache = __usrReg_IU
  @HandlerDec
  def export_insertUserRegistryCache( self, login, name, email ):
    return db
  
  types_updateUserRegistryCache = __usrReg_IU
  @HandlerDec
  def export_updateUserRegistryCache( self, login, name, email ):
    return db
  
  types_getUserRegistryCache = __usrReg_GD
  @HandlerDec
  def export_getUserRegistryCache( self, login, name, email, kwargs ):
    return db

  types_deleteUserRegistryCache = __usrReg_GD
  @HandlerDec
  def export_deleteUserRegistryCache( self, login, name, email, kwargs ):      
    return db

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
  
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