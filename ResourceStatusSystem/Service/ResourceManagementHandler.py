""" ``ResourceManagementHandler`` exposes the service of the Resource Management System.
    It uses :mod:`DIRAC.ResourceStatusSystem.DB.ResourceManagementDB` for database persistence.

    To use this service

    >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
    >>> server = RPCCLient("ResourceStatus/ResourceManagement")

"""
__RCSID__ = "$Id$"

# it crashes epydoc
# __docformat__ = "restructuredtext en"

import datetime

from types import StringType, BooleanType, DictType, ListType, IntType
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger, gConfig

#from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
#from DIRAC.Core.Utilities import Time

from DIRAC.ResourceStatusSystem.Utilities.CS import getExt

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB, RSSManagementDBException
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB, RSSDBException
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException, InvalidRes
from DIRAC.ResourceStatusSystem.Utilities.Utils import whoRaised, where, convertTime
from DIRAC.ResourceStatusSystem.Utilities.Publisher import Publisher
from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter
from DIRAC.ResourceStatusSystem.Utilities.Synchronizer import Synchronizer


rmDB = False
rsDB = False

def initializeResourceManagementHandler( _serviceInfo ):

  global rsDB
  rsDB = ResourceStatusDB()
  global rmDB
  rmDB = ResourceManagementDB()

  cc = CommandCaller()

  global VOExtension
  VOExtension = getExt()

  ig = InfoGetter( VOExtension )

  WMSAdmin = RPCClient( "WorkloadManagement/WMSAdministrator" )

  global publisher
  publisher = Publisher( VOExtension, rsDBIn = rsDB, commandCallerIn = cc,
                         infoGetterIn = ig, WMSAdminIn = WMSAdmin )

  sync_O = Synchronizer(rsDBin=rsDB, rmDBin=rmDB)
  gConfig.addListenerToNewVersionEvent( sync_O.sync )

  return S_OK()

class ResourceManagementHandler( RequestHandler ):

  def initialize( self ):
    pass

##
# Methods needed to inherit without messing up global variables
##  
  def setResourceStatusDatabase( self, oDatabase ):
    global rsDB
    rsDB = oDatabase

  def setResourceManagementDatabase( self, oDatabase ):
    global rmDB
    rmDB = oDatabase

#############################################################################

#############################################################################
# Mixed functions
#############################################################################

#############################################################################

  types_getStatusList = []
  def export_getStatusList( self ):
    """
    Get status list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getStatusList`
    """

    gLogger.info( "ResourceManagementHandler.getStatusList: Attempting to get status list" )

    try:

      res = []

      try:
        res = rmDB.getStatusList()
      except RSSDBException, x:
        gLogger.error( whoRaised( x ) )
      except RSSException, x:
        gLogger.error( whoRaised( x ) )

      gLogger.info( "ResourceManagementHandler.getStatusList: got status list" )
      return S_OK( res )

    except Exception:
      errorStr = where( self, self.export_getStatusList )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  types_getEnvironmentCache = [StringType, StringType]
  def export_getEnvironmentCache( self, hash_, siteName ):
    """ get Policy Result
    """
    try:
      gLogger.info("ResourceManagementHandler.getEnvironmentCache: Attempting to get environment of %s for %s" % ( siteName, hash_ ))
      try:
        res = rmDB.getEnvironmentCache( hash_, siteName )
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceManagementHandler.getEnvironmentCache: got environment of %s for %s" % ( siteName, hash_ ))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getEnvironmentCache)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_addOrModifyEnvironmentCache = [StringType, StringType, StringType]
  def export_addOrModifyEnvironmentCache( self, hash_, siteName, environment ):
    """ get Policy Result
    """

    try:
      gLogger.info("ResourceManagementHandler.addOrModifyEnvironmentCache: Attempting to add/modify environment of %s for %s" % ( siteName, hash_ ))
      try:
        res = rmDB.addOrModifyEnvironmentCache( hash_, siteName, environment )
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceManagementHandler.addOrModifyEnvironmentCache: add/modify environment of %s for %s" % ( siteName, hash_ ))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_addOrModifyEnvironmentCache)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getPolicyRes = [ StringType, StringType, BooleanType ]
  def export_getPolicyRes( self, name, policyName, lastCheckTime ):
    """ get Policy Result
    """

    gLogger.info( "ResourceManagementHandler.getPolicyRes: Attempting to get result of %s for %s" % (policyName, name) )

    try:

      res = []

      try:
        res = rmDB.getPolicyRes( name, policyName, lastCheckTime )
      except RSSDBException, x:
        gLogger.error( whoRaised( x ) )
      except RSSException, x:
        gLogger.error( whoRaised( x ) )

      gLogger.info( "ResourceManagementHandler.getPolicyRes: got result of %s for %s" % (policyName, name) )
      return S_OK( res )

    except Exception:
      errorStr = where( self, self.export_getPolicyRes )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  types_getCachedAccountingResult = [ StringType, StringType, StringType ]
  def export_getCachedAccountingResult( self, name, plotType, plotName ):
    """ get a cached accounting result
    """
    gLogger.info( "ResourceManagementHandler.getCachedAccountingResult: Attempting to get %s: %s, %s accounting cached result" % ( name, plotType, plotName ) )

    try:

      res = []

      try:
        res = rmDB.getAccountingCacheStuff( [ 'Result' ], name = name, plotType = plotType,
                                            plotName = plotName )
        if not ( res == [] ):
          res = res[ 0 ]
      except RSSDBException, x:
        gLogger.error( whoRaised( x ) )
      except RSSException, x:
        gLogger.error( whoRaised( x ) )

      gLogger.info( "ResourceManagementHandler.getCachedAccountingResult: got %s: %s %s cached result" % ( name, plotType, plotName ) )
      return S_OK( res )

    except Exception:
      errorStr = where( self, self.export_getCachedAccountingResult )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  types_getCachedResult = [ StringType, StringType, StringType, StringType ]
  def export_getCachedResult( self, name, command, value, opt_ID ):
    """ get a cached result
    """

    gLogger.info( "ResourceManagementHandler.getCachedResult: Attempting to get %s: %s, %s cached result" % ( name, value, command ) )

    try:

      res = []

      try:
        if opt_ID == 'NULL':
          opt_ID = None
        res = rmDB.getClientsCacheStuff( [ 'Result' ], name = name, commandName = command,
                                         value = value, opt_ID = opt_ID )
        if not ( res == [] ):
          res = res[ 0 ]
      except RSSDBException, x:
        gLogger.error( whoRaised( x ) )
      except RSSException, x:
        gLogger.error( whoRaised( x ) )

      gLogger.info( "ResourceManagementHandler.getCachedResult: got %s: %s %s cached result" % ( name, value, command ) )
      return S_OK( res )

    except Exception:
      errorStr = where( self, self.export_getCachedResult )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  types_getCachedIDs = [ StringType, StringType ]
  def export_getCachedIDs( self, name, command ):
    """ get a cached IDs
    """

    gLogger.info( "ResourceManagementHandler.getCachedIDs: Attempting to get %s: %s cached IDs" % ( name, command ) )

    try:

      dt_ID = []

      try:
        res = rmDB.getClientsCacheStuff( 'opt_ID', name = name, commandName = command )
        for tuple_dt_ID in res:
          if tuple_dt_ID[ 0 ] not in dt_ID:
            dt_ID.append( tuple_dt_ID[ 0 ] )
      except RSSDBException, x:
        gLogger.error( whoRaised( x ) )
      except RSSException, x:
        gLogger.error( whoRaised( x ) )

      gLogger.info( "ResourceManagementHandler.getCachedIDs: got %s: %s cached result" % ( name, command ) )
      return S_OK( dt_ID )

    except Exception:
      errorStr = where( self, self.export_getCachedIDs )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  types_getDownTimesWeb = [ DictType, ListType, IntType, IntType ]
  def export_getDownTimesWeb( self, selectDict, _sortList, startItem, maxItems ):
    """ get down times as registered with the policies.
        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getDownTimesWeb`

        :Parameters:
          `selectDict`
            {
              'Granularity':'Site', 'Resource', or a list with both
              'Severity':'OUTAGE', 'AT_RISK', or a list with both
            }

          `sortList`
            [] (no sorting provided)

          `startItem`

          `maxItems`

        :return:
        {
          'OK': XX,

          'rpcStub': XX, 'getDownTimesWeb', ({}, [], X, X)),

          Value':
          {

            'ParameterNames': ['Granularity', 'Name', 'Severity', 'When'],

            'Records': [[], [], ...]

            'TotalRecords': X,

            'Extras': {},
          }
        }
    """

    gLogger.info( "ResourceManagementHandler.getDownTimesWeb: Attempting to get down times list" )

    try:

      finalDict = {}

      try:
        try:
          granularity = selectDict[ 'Granularity' ]
        except KeyError:
          granularity = []

        if not isinstance( granularity, list ):
          granularity = [ granularity ]
        commands = []
        if granularity == []:
          commands = [ 'DTEverySites', 'DTEveryResources' ]
        elif 'Site' in granularity:
          commands.append( 'DTEverySites' )
        elif 'Resource' in granularity:
          commands.append( 'DTEveryResources' )

        try:
          severity = selectDict[ 'Severity' ]
        except KeyError:
          severity = []
        if not isinstance( severity, list ):
          severity = [ severity ]
        if severity == []:
          severity = [ 'AT_RISK', 'OUTAGE' ]

        res = rmDB.getClientsCacheStuff( [ 'Name', 'Opt_ID', 'Value', 'Result', 'CommandName' ],
                                         commandName = commands )
        records = []

        if not ( res == () ):
          made_IDs = []

          for dt_tuple in res:
            considered_ID = dt_tuple[ 1 ]
            if considered_ID not in made_IDs:
              name = dt_tuple[ 0 ]
              if dt_tuple[ 4 ] == 'DTEverySites':
                granularity = 'Site'
              elif dt_tuple[ 4 ] == 'DTEveryResources':
                granularity = 'Resource'
              toTake = [ 'Severity', 'StartDate', 'EndDate', 'Description' ]

              for dt_t in res:
                if considered_ID == dt_t[ 1 ]:
                  if toTake != []:
                    if dt_t[ 2 ] in toTake:
                      if dt_t[ 2 ] == 'Severity':
                        sev = dt_t[ 3 ]
                        toTake.remove( 'Severity' )
                      if dt_t[ 2 ] == 'StartDate':
                        startDate = dt_t[ 3 ]
                        toTake.remove( 'StartDate' )
                      if dt_t[ 2 ] == 'EndDate':
                        endDate = dt_t[ 3 ]
                        toTake.remove( 'EndDate' )
                      if dt_t[ 2 ] == 'Description':
                        description = dt_t[ 3 ]
                        toTake.remove( 'Description' )

              now                = datetime.datetime.utcnow().replace( microsecond = 0, second = 0 )
              startDate_datetime = datetime.datetime.strptime( startDate, '%Y-%m-%d %H:%M' )
              endDate_datetime   = datetime.datetime.strptime( endDate, '%Y-%m-%d %H:%M' )

              if endDate_datetime < now:
                when = 'Finished'
              else:
                if startDate_datetime < now:
                  when = 'OnGoing'
                else:
                  hours = str( convertTime( startDate_datetime - now, 'hours' ) )
                  when = 'In ' + hours + ' hours.'

              if sev in severity:
                records.append( [ considered_ID, granularity, name, sev,
                                 when, startDate, endDate, description ])

              made_IDs.append( considered_ID )

        # adding downtime links to the GOC DB page in Extras
        DT_links = []
        for record in records:
          DT_link = rmDB.getClientsCacheStuff( [ 'Result' ], opt_ID = record[ 0 ], value = 'Link' )
          DT_link = DT_link[ 0 ][ 0 ]
          DT_links.append( { record[ 0 ] : DT_link } )

        paramNames = [ 'ID', 'Granularity', 'Name', 'Severity', 'When', 'Start', 'End', 'Description' ]

        finalDict[ 'TotalRecords' ]   = len( records )
        finalDict[ 'ParameterNames' ] = paramNames

        # Return all the records if maxItems == 0 or the specified number otherwise
        if maxItems:
          finalDict[ 'Records' ] = records[ startItem:startItem+maxItems ]
        else:
          finalDict[ 'Records' ] = records

        finalDict[ 'Extras' ] = DT_links

      except RSSDBException, x:
        gLogger.error( whoRaised( x ) )
      except RSSException, x:
        gLogger.error( whoRaised( x ) )

      gLogger.info( "ResourceManagementHandler.getDownTimesWeb: got DT list" )
      return S_OK( finalDict )

    except Exception:
      errorStr = where( self, self.export_getDownTimesWeb )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  types_enforcePolicies = [ StringType, StringType, BooleanType ]
  def export_enforcePolicies( self, granularity, name, useNewRes = True ):
    """ Enforce all the policies. If `useNewRes` is False, use cached results only (where available).
    """

    gLogger.info( "ResourceManagementHandler.enforcePolicies: Attempting to enforce policies for %s %s" % ( granularity, name ) )

    try:

      try:
        reason = serviceType = resourceType = None

        res          = rsDB.getStuffToCheck( granularity, name = name )[ 0 ]
        status       = res[ 1 ]
        formerStatus = res[ 2 ]
        siteType     = res[ 3 ]
        tokenOwner   = res[ len( res ) - 1 ]

        if granularity == 'Resource':
          resourceType = res[ 4 ]
        elif granularity == 'Service':
          serviceType = res[ 4 ]

        from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
        pep = PEP( VOExtension, granularity, name, status, formerStatus, reason, siteType,
                   serviceType, resourceType, tokenOwner, useNewRes )
        pep.enforce( rsDBIn = rsDB, rmDBIn = rmDB )

      except RSSDBException, x:
        gLogger.error( whoRaised( x ) )
      except RSSException, x:
        gLogger.error( whoRaised( x ) )

      msg = "ResourceManagementHandler.enforcePolicies: enforced for %s: %s" % ( granularity, name )
      gLogger.info( msg )
      return S_OK( msg )

    except Exception:
      errorStr = where( self, self.export_getCachedResult )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

#   types_publisher = [ StringType, StringType, BooleanType ]
#   def export_publisher( self, granularity, name, useNewRes = False ):
#     """ get a view

#     :Parameters:
#       `granularity`
#         string - a ValidRes

#       `name`
#         string - name of the res

#       `useNewRes`
#         boolean. When set to true, will get new results,
#         otherwise it will get cached results (where available).
#     """

#     gLogger.info( "ResourceManagementHandler.publisher: Attempting to get info for %s: %s" % ( granularity, name ) )

#     try:

#       res = []

#       try:
#         if useNewRes == True:
#           from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
#           gLogger.info( "ResourceManagementHandler.publisher: Recalculating policies for %s: %s" % ( granularity, name ) )
#           if granularity in ( 'Site', 'Sites' ):
#             res = rsDB.getStuffToCheck( granularity, name = name )[ 0 ]
#             status       = res[ 1 ]
#             formerStatus = res[ 2 ]
#             siteType     = res[ 3 ]
#             tokenOwner   = res[ 4 ]

#             pep = PEP( VOExtension, granularity, name, status, formerStatus, None, siteType,
#                        None, None, tokenOwner, useNewRes )
#             pep.enforce( rsDBIn = rsDB, rmDBIn = rmDB )

#             res = rsDB.getMonitoredsList( 'Service', paramsList = [ 'ServiceName' ], siteName = name )
#             services = [ x[ 0 ] for x in res ]
#             for s in services:
#               res = rsDB.getStuffToCheck( 'Service', name = s )[ 0 ]
#               status       = res[ 1 ]
#               formerStatus = res[ 2 ]
#               siteType     = res[ 3 ]
#               serviceType  = res[ 4 ]

#               pep = PEP( VOExtension, 'Service', s, status, formerStatus, None, siteType,
#                          serviceType, None, tokenOwner, useNewRes )
#               pep.enforce( rsDBIn = rsDB, rmDB = rmDB )
#           else:
#             reason = serviceType = resourceType = None

#             res = rsDB.getStuffToCheck( granularity, name = name )[ 0 ]
#             status       = res[ 1 ]
#             formerStatus = res[ 2 ]
#             siteType     = res[ 3 ]
#             tokenOwner   = res[ len( res ) - 1 ]

#             if granularity == 'Resource':
#               resourceType = res[ 4 ]
#             elif granularity == 'Service':
#               serviceType = res[ 4 ]

# #            from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
#             pep = PEP( VOExtension, granularity, name, status, formerStatus, reason, siteType,
#                        serviceType, resourceType, tokenOwner, useNewRes )
#             pep.enforce( rsDBIn = rsDB, rmDBIn = rmDB )

#         res = publisher.getInfo( granularity, name, useNewRes )
#       except InvalidRes, x:
#         errorStr = "Invalid granularity"
#         gLogger.exception( whoRaised( x ) + errorStr )
#         return S_ERROR( errorStr )
#       except RSSException, x:
#         errorStr = "RSSException"
#         gLogger.exception( whoRaised( x ) + errorStr )

#       gLogger.info( "ResourceManagementHandler.publisher: got info for %s: %s" % ( granularity, name ) )
#       return S_OK( res )

#     except Exception:
#       errorStr = where( self, self.export_publisher )
#       gLogger.exception( errorStr )
#       return S_ERROR( errorStr )

#############################################################################

# User Registry Functions

  types_registryAddUser = [str, str, str]
  def export_registryAddUser(self, login, name, email):
    gLogger.info( "ResourceManagementHandler.registryAddUser: Attempting to add user on registry cache" )

    try:
      return S_OK(rmDB.registryAddUser(login, name, email))

    except RSSManagementDBException:
      errorStr = where( self, self.export_registryAddUser )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )


  types_registryDelUser = [str]
  def export_registryDelUser(self, _name):
    return S_ERROR("Not implemented.")

  types_registryGetMailFromLogin = [list]
  def export_registryGetMailFromLogin(self, logins):
    gLogger.info( "ResourceManagementHandler.registryGetMailFromLogin" )
    try:
      S_OK([rmDB.registryGetMailFromLogin(l) for l in logins])

    except RSSManagementDBException:
      errorStr = where( self, self.export_registryAddUser )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

  types_registryGetMailFromName = [list]
  def export_registryGetMailFromName(self, names):
    gLogger.info( "ResourceManagementHandler.registryGetMailFromName" )
    try:
      S_OK([rmDB.registryGetMailFromName(n) for n in names])

    except RSSManagementDBException:
      errorStr = where( self, self.export_registryAddUser )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )
