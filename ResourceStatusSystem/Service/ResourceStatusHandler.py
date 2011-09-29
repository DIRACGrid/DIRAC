""" ``ResourceStatusHandler`` exposes the service of the Resource Status System.
    It uses :mod:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB` for database persistence.

    To use this service

    >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
    >>> server = RPCCLient("ResourceStatus/ResourceStatus")

"""

__RCSID__ = "$Id:  $"

from datetime import datetime
from types import NoneType

from DIRAC import gLogger, gConfig, S_OK#, S_ERROR

#from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName
from DIRAC.Core.DISET.RequestHandler             import RequestHandler

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB     import ResourceStatusDB#RSSDBException, ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
#from DIRAC.ResourceStatusSystem.Utilities.CS            import getExt
#from DIRAC.ResourceStatusSystem.Utilities.Exceptions    import RSSException
#from DIRAC.ResourceStatusSystem.Utilities.Utils         import whoRaised, where

#from DIRAC.ResourceStatusSystem                        import ValidSiteType, \
#      ValidServiceType, ValidResourceType, ValidStatus
#from DIRAC.ResourceStatusSystem.Utilities.Publisher    import Publisher
#from DIRAC.ResourceStatusSystem.Command.CommandCaller  import CommandCaller
#from DIRAC.Core.DISET.RPCClient                        import RPCClient
#from DIRAC.ResourceStatusSystem.Utilities.InfoGetter   import InfoGetter
from DIRAC.ResourceStatusSystem.Utilities.Synchronizer import Synchronizer

rsDB = False

def initializeResourceStatusHandler( _serviceInfo ):

  global rsDB
  rsDB = ResourceStatusDB()

  rmDB = ResourceManagementDB()

#  cc = CommandCaller()

#  global VOExtension
#  VOExtension = getExt()

#  ig = InfoGetter( VOExtension )

#  WMSAdmin = RPCClient( "WorkloadManagement/WMSAdministrator" )

#  global publisher
#  publisher = Publisher( VOExtension, rsDBIn = rsDB, commandCallerIn = cc,
#                         infoGetterIn = ig, WMSAdminIn = WMSAdmin )

  sync_O = Synchronizer( rsDBin = rsDB, rmDBin = rmDB )
  gConfig.addListenerToNewVersionEvent( sync_O.sync )

  return S_OK()

class ResourceStatusHandler( RequestHandler ):

  def initialize( self ):
    pass

################################################################################

################################################################################
# Sites functions
################################################################################

################################################################################

  types_addOrModifySite = [ str, str, str ]

  def export_addOrModifySite( self, siteName, siteType, gridSiteName ):
    """
    Add or modify a site to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifySite`

    :Parameters
      `siteName`
        string - name of the site (DIRAC name)

      `siteType`
        string - ValidSiteType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      `gridSiteName`
        string - name of the site in the GOC DB
    """

    gLogger.info( "addOrModifySite: Attempting to add or modify site %s" % siteName )
    resQuery = rsDB.addOrModifySite( siteName, siteType, gridSiteName )
    gLogger.info( "addOrModifySite: Added (or modified) site %s." % siteName )
    return resQuery

################################################################################

  types_setSiteStatus = [ str, str, str, str, str, ( datetime, NoneType ),
                          ( datetime, NoneType ),( datetime, NoneType ),
                          ( datetime, NoneType ),( datetime, NoneType ) ]    

  def export_setSiteStatus( self, siteName, statusType, status, reason, tokenOwner,
                            tokenExpiration, dateCreated, dateEffective, dateEnd,
                            lastCheckTime ):

    gLogger.info( "setSiteStatus: Attempting to set site %s status" % siteName )
    resQuery = rsDB.setSiteStatus( siteName, statusType, status, reason, tokenOwner, 
                        tokenExpiration, dateCreated, dateEffective, dateEnd,
                        lastCheckTime )
    gLogger.info( "setSiteStatus: Set site %s status." % siteName )
    return resQuery

################################################################################
    
  types_setSiteScheduledStatus = [ str, str, str, str, str, ( datetime, NoneType ),
                                  ( datetime, NoneType ), ( datetime, NoneType ),
                                  ( datetime, NoneType ), ( datetime, NoneType ) ]
    
  def export_setSiteScheduledStatus( self, siteName, statusType, status, reason, 
                                     tokenOwner, tokenExpiration, dateCreated, 
                                     dateEffective, dateEnd, lastCheckTime ):

    gLogger.info( "setSiteScheduledStatus: Attempting to set site %s scheduledStatus" % siteName )
    resQuery = rsDB.setSiteStatus( siteName, statusType, status, reason, tokenOwner, 
                        tokenExpiration, dateCreated, dateEffective, dateEnd,
                        lastCheckTime )
    gLogger.info( "setSiteScheduledStatus: Set site %s scheduledStatus." % siteName )
    return resQuery

################################################################################
      
  types_updateSiteStatus = [ str, ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                            ( str, NoneType ),( datetime, NoneType ),( datetime, NoneType ),
                            ( datetime, NoneType ), ( datetime, NoneType ),
                            ( datetime, NoneType ) ]  
    
  def export_updateSiteStatus( self, siteName, statusType, status, reason, 
                               tokenOwner, tokenExpiration, dateCreated, 
                               dateEffective, dateEnd, lastCheckTime):

    gLogger.info( "updateSiteStatus_1" )
    resQuery = rsDB.updateSiteStatus( siteName, statusType, status, reason, 
                                      tokenOwner, tokenExpiration, dateCreated, 
                                      dateEffective, dateEnd, lastCheckTime )
    gLogger.info( "updateSiteStatus_2" )
    return resQuery
      
################################################################################

  types_getSites = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ), dict ]
  
  def export_getSites( self, siteName, siteType, gridSiteName, kwargs ):
    
    gLogger.info( "getSites_1" )
    resQuery = rsDB.getSites( siteName, siteType, gridSiteName, **kwargs )
    gLogger.info( "getSites_2" )
    return resQuery

################################################################################

  types_getSitesStatus = [ str, str, str, str, str, ( datetime, NoneType ),
                          ( datetime, NoneType ), ( datetime, NoneType ),
                          ( datetime, NoneType ), ( datetime, NoneType ), dict ]

  def export_getSitesStatus( self, siteName, statusType, status, reason, 
                             tokenOwner, tokenExpiration, dateCreated, 
                             dateEffective, dateEnd, lastCheckTime, kwargs ):

    gLogger.info( "getSitesStatus_1" )
    resQuery = rsDB.getSitesStatus( siteName, statusType, status, reason, tokenOwner, 
                                    tokenExpiration, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, **kwargs )
    gLogger.info( "getSitesStatus_2" )
    return resQuery
  
################################################################################  
  
  types_getSitesHistory = [ str, str, str, str, str,( datetime, NoneType ),
                          ( datetime, NoneType ), ( datetime, NoneType ),
                          ( datetime, NoneType ), ( datetime, NoneType ), dict ]

  def export_getSitesHistory( self, siteName, statusType, status, reason, 
                              tokenOwner, tokenExpiration, dateCreated, 
                              dateEffective, dateEnd, lastCheckTime, kwargs ):

    gLogger.info( "getSitesHistory_1" )
    resQuery = rsDB.getSitesHistory( siteName, statusType, status, reason, tokenOwner, 
                                     tokenExpiration, dateCreated, dateEffective, 
                                     dateEnd, lastCheckTime, **kwargs )
    gLogger.info( "getSitesHistory_2" )
    return resQuery

################################################################################

  types_getSitesScheduledStatus = [ str, str, str, str, str,( datetime, NoneType ),
                                    ( datetime, NoneType ), ( datetime, NoneType ),
                                    ( datetime, NoneType ), ( datetime, NoneType ), 
                                    dict ]

  def export_getSitesScheduledStatus( self, siteName, statusType, status, reason, 
                                      tokenOwner, tokenExpiration, dateCreated, 
                                      dateEffective, dateEnd, lastCheckTime, 
                                      kwargs ):

    gLogger.info( "getSitesScheduledStatus_1" )
    resQuery = rsDB.getSitesScheduledStatus( siteName, statusType, status, reason, 
                                             tokenOwner, tokenExpiration, dateCreated, 
                                             dateEffective, dateEnd, lastCheckTime, 
                                             **kwargs )
    gLogger.info( "getSitesScheduledStatus_2" )
    return resQuery

################################################################################

  types_getSitesPresent = [ str, str, str, str, str, str, ( datetime, NoneType ),
                            str, ( datetime, NoneType ), str, ( datetime, NoneType ),
                            str, dict ]

  def export_getSitesPresent( self, siteName, siteType, gridSiteName, gridTier, 
                              statusType, status, dateEffective, reason, 
                              lastCheckTime, tokenOwner, tokenExpiration, 
                              formerStatus, kwargs ):

    gLogger.info( "getSitesPresent_1" )
    resQuery = rsDB.getSitesPresent( siteName, siteType, gridSiteName, gridTier, 
                                     statusType, status, dateEffective, reason, 
                                     lastCheckTime, tokenOwner, tokenExpiration, 
                                     formerStatus, **kwargs )
    gLogger.info( "getSitesPresent_2" )
    return resQuery

################################################################################    

  types_deleteSites = [ str ]

  def export_deleteSites( self, siteName ):
    
    gLogger.info( "deleteSites_1" )
    resQuery = rsDB.deleteSites( siteName )
    gLogger.info( "deleteSites_2" )
    return resQuery

################################################################################

  types_deleteSitesScheduledStatus = [ str, str, str, str, str, ( datetime, NoneType ),
                                      ( datetime, NoneType ),( datetime, NoneType ),
                                      ( datetime, NoneType ),( datetime, NoneType )]

  def export_deleteSitesScheduledStatus( self, siteName, statusType, status, 
                                         reason, tokenOwner, tokenExpiration, 
                                         dateCreated, dateEffective, dateEnd, 
                                         lastCheckTime ):

    gLogger.info( "deleteSitesScheduledStatus_1" )
    resQuery = rsDB.deleteSitesScheduledStatus( siteName, statusType, status, 
                                                reason, tokenOwner, tokenExpiration, 
                                                dateCreated, dateEffective, dateEnd, 
                                                lastCheckTime )
    gLogger.info( "deleteSitesScheduledStatus_2" )
    return resQuery

################################################################################

  types_deleteSitesHistory = [ str, str, str, str, str, ( datetime, NoneType ),
                              ( datetime, NoneType ),( datetime, NoneType ),
                              ( datetime, NoneType ),( datetime, NoneType ), dict ]

  def export_deleteSitesHistory( self, siteName, statusType, status, reason, 
                                 tokenOwner, tokenExpiration, dateCreated, 
                                 dateEffective, dateEnd, lastCheckTime, kwargs ):

    gLogger.info( "deleteSitesHistory_1" )
    resQuery = rsDB.deleteSitesHistory( siteName, statusType, status, reason, 
                                        tokenOwner, tokenExpiration, dateCreated, 
                                        dateEffective, dateEnd, lastCheckTime, 
                                        **kwargs )
    gLogger.info( "deleteSitesHistory_2" )
    return resQuery

################################################################################

################################################################################
# Services functions
################################################################################

################################################################################

  types_addOrModifyService = [ str, str, str ]
  
  def export_addOrModifyService( self, serviceName, serviceType, siteName ):
    
    gLogger.info( "addOrModifyService_1" )
    resQuery = rsDB.addOrModifyService( serviceName, serviceType, siteName )
    gLogger.info( "addOrModifyService_2" )  
    return resQuery
  
################################################################################  
  
  types_setServiceStatus = [ str, str, str, str, str, ( datetime, NoneType ),
                            ( datetime, NoneType ),( datetime, NoneType ),
                            ( datetime, NoneType ),( datetime, NoneType ) ]  
    
  def export_setServiceStatus( self, serviceName, statusType, status, reason, 
                               tokenOwner, tokenExpiration, dateCreated, 
                               dateEffective, dateEnd, lastCheckTime ):
    
    gLogger.info( "setServiceStatus_1" )
    resQuery = rsDB.setServiceStatus( serviceName, statusType, status, reason, 
                                      tokenOwner, tokenExpiration, dateCreated, 
                                      dateEffective, dateEnd, lastCheckTime )
    gLogger.info( "setServiceStatus_2" )  
    return resQuery
   
################################################################################   
    
  types_setServiceScheduledStatus = [ str, str, str, str, str, ( datetime, NoneType ),
                                      ( datetime, NoneType ),( datetime, NoneType ),
                                      ( datetime, NoneType ),( datetime, NoneType ) ]  
          
  def export_setServiceScheduledStatus( self, serviceName, statusType, status, 
                                        reason, tokenOwner, tokenExpiration, 
                                        dateCreated, dateEffective, dateEnd, 
                                        lastCheckTime ):
    gLogger.info( "setServiceScheduledStatus_1" )
    resQuery = rsDB.setServiceScheduledStatus( serviceName, statusType, status, 
                                               reason, tokenOwner, tokenExpiration, 
                                               dateCreated, dateEffective, dateEnd, 
                                               lastCheckTime )
    gLogger.info( "setServiceScheduledStatus_2" )  
    return resQuery  
  
################################################################################  
    
  types_updateServiceStatus = [ str, ( str, NoneType ), ( str, NoneType ),
                                ( str, NoneType ), ( str, NoneType ), ( str, NoneType ), 
                                ( datetime, NoneType ), ( datetime, NoneType ),
                                ( datetime, NoneType ),( datetime, NoneType ),
                                ( datetime, NoneType ) ]  
    
  def export_updateServiceStatus( self, serviceName, statusType, status, reason, 
                                  tokenOwner, tokenExpiration, dateCreated, 
                                  dateEffective, dateEnd, lastCheckTime ):
    
    gLogger.info( "updateServiceStatus_1" )
    resQuery = rsDB.updateServiceStatus( serviceName, statusType, status, reason, 
                                         tokenOwner, tokenExpiration, dateCreated, 
                                         dateEffective, dateEnd, lastCheckTime )
    gLogger.info( "updateServiceStatus_2" )  
    return resQuery
  
################################################################################  
  
  types_getServices = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                        dict ]
  
  def export_getServices( self, serviceName, serviceType, siteName, kwargs ):
    
    gLogger.info( "getServices_1" )
    resQuery = rsDB.getServices( serviceName, serviceType, siteName, **kwargs )
    gLogger.info( "getServices_2" )  
    return resQuery
  
################################################################################  
    
  types_getServicesStatus = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                              ( str, NoneType ), ( str, NoneType ), ( datetime, NoneType ),
                              ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                              ( str, NoneType ), dict ]  
    
  def export_getServicesStatus( self, serviceName, statusType, status, reason, 
                                tokenOwner, tokenExpiration, dateCreated, 
                                dateEffective, dateEnd, lastCheckTime, kwargs ):

    gLogger.info( "getServicesStatus_1" )
    resQuery = rsDB.getServicesStatus( serviceName, statusType, status, reason, 
                                       tokenOwner, tokenExpiration, dateCreated, 
                                       dateEffective, dateEnd, lastCheckTime, 
                                       **kwargs )
    gLogger.info( "getServicesStatus_2" )  
    return resQuery
  
################################################################################  

  types_getServicesHistory = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                               ( str, NoneType ), ( str, NoneType ), ( datetime, NoneType ),
                               ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                               ( str, NoneType ), dict ] 
      
  def export_getServicesHistory( self, serviceName, statusType, status, reason, 
                                 tokenOwner, tokenExpiration, dateCreated, 
                                 dateEffective, dateEnd, lastCheckTime, kwargs ):

    gLogger.info( "getServicesHistory_1" )
    resQuery = rsDB.getServicesStatus( serviceName, statusType, status, reason, 
                                       tokenOwner, tokenExpiration, dateCreated, 
                                       dateEffective, dateEnd, lastCheckTime, 
                                       **kwargs )
    gLogger.info( "getServicesHistory_2" )  
    return resQuery

################################################################################  

  types_getServicesScheduledStatus = [ ( str, NoneType ), ( str, NoneType ), 
                                       ( str, NoneType ), ( str, NoneType ), 
                                       ( str, NoneType ), ( datetime, NoneType ),
                                       ( str, NoneType ), ( str, NoneType ), 
                                       ( str, NoneType ), ( str, NoneType ), dict ] 
    
  def export_getServicesScheduledStatus( self, serviceName, statusType, status, 
                                         reason, tokenOwner, tokenExpiration, 
                                         dateCreated, dateEffective, dateEnd, 
                                         lastCheckTime, kwargs ):

    gLogger.info( "getServicesScheduledStatus_1" )
    resQuery = rsDB.getServicesScheduledStatus( serviceName, statusType, status, 
                                                reason, tokenOwner, tokenExpiration, 
                                                dateCreated, dateEffective, dateEnd, 
                                                lastCheckTime, **kwargs )
    gLogger.info( "getServicesScheduledStatus_2" )  
    return resQuery
    
################################################################################  
    
  types_getServicesPresent = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                               ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                               ( datetime, NoneType ), ( str, NoneType ), 
                               ( datetime, NoneType ), ( str, NoneType ), 
                               ( datetime, NoneType ), ( str, NoneType ), dict ]  
    
  def export_getServicesPresent( self, serviceName, siteName, siteType, serviceType, 
                                 statusType, status, dateEffective, reason, 
                                 lastCheckTime, tokenOwner, tokenExpiration, 
                                 formerStatus, kwargs ):

    gLogger.info( "getServicesPresent_1" )
    resQuery = rsDB.getServicesPresent( serviceName, siteName, siteType, serviceType, 
                                        statusType, status, dateEffective, reason, 
                                        lastCheckTime, tokenOwner, tokenExpiration, 
                                        formerStatus, **kwargs )
    gLogger.info( "getServicesPresent_2" )  
    return resQuery
  
################################################################################  
    
  types_deleteServices = [ str ]  
    
  def export_deleteServices( self, serviceName ):
    
    gLogger.info( "deleteServices_1" )
    resQuery = rsDB.deleteServices( serviceName )
    gLogger.info( "deleteServices_2" )  
    return resQuery  
  
################################################################################  
    
  types_deleteServicesScheduledStatus = [ ( str, NoneType ), ( str, NoneType ), 
                                          ( str, NoneType ), ( str, NoneType ), 
                                          ( str, NoneType ), ( datetime, NoneType ),
                                          ( str, NoneType ), ( str, NoneType ), 
                                          ( str, NoneType ), ( str, NoneType ) ]  
    
  def export_deleteServicesScheduledStatus( self, serviceName, statusType, status, 
                                            reason, tokenOwner, tokenExpiration, 
                                            dateCreated, dateEffective, dateEnd, 
                                            lastCheckTime ):

    gLogger.info( "deleteServicesScheduledStatus_1" )
    resQuery = rsDB.deleteServicesScheduledStatus( serviceName, statusType, status, 
                                                   reason, tokenOwner, tokenExpiration, 
                                                   dateCreated, dateEffective, dateEnd, 
                                                   lastCheckTime )
    gLogger.info( "deleteServicesScheduledStatus_2" )  
    return resQuery  

################################################################################
    
  types_deleteServicesHistory = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ), 
                                  ( str, NoneType ), ( str, NoneType ), ( datetime, NoneType ),
                                  ( str, NoneType ), ( str, NoneType ), ( str, NoneType ), 
                                  ( str, NoneType ), dict ]  
    
  def export_deleteServicesHistory( self, serviceName, statusType, status, reason, 
                                    tokenOwner, tokenExpiration, dateCreated, 
                                    dateEffective, dateEnd, lastCheckTime, kwargs ):                                              

    gLogger.info( "deleteServicesHistory_1" )
    resQuery = rsDB.deleteServicesHistory( serviceName, statusType, status, reason, 
                                           tokenOwner, tokenExpiration, dateCreated, 
                                           dateEffective, dateEnd, lastCheckTime, 
                                           **kwargs )
    gLogger.info( "deleteServicesHistory_2" )  
    return resQuery  

################################################################################

################################################################################
# Resources functions
################################################################################

################################################################################

  types_addOrModifyResource = [ str, str, str, str, str ]

  def export_addOrModifyResource( self, resourceName, resourceType, serviceType, 
                                  siteName, gridSiteName ):

    gLogger.info( "addOrModifyResource_1" )
    resQuery = rsDB.addOrModifyResource( resourceName, resourceType, serviceType, 
                                         siteName, gridSiteName )
    gLogger.info( "addOrModifyResource_2" )  
    return resQuery  
  
################################################################################
  
  types_setResourceStatus = [ str, str, str, str, str, ( datetime, NoneType ),
                              ( datetime, NoneType ), ( datetime, NoneType ),
                              ( datetime, NoneType ), ( datetime, NoneType ) ]
      
  def export_setResourceStatus( self, resourceName, statusType, status, reason, 
                                tokenOwner, tokenExpiration, dateCreated, 
                                dateEffective, dateEnd, lastCheckTime ):

    gLogger.info( "setResourceStatus_1" )
    resQuery = rsDB.setResourceStatus( resourceName, statusType, status, reason, 
                                       tokenOwner, tokenExpiration, dateCreated, 
                                       dateEffective, dateEnd, lastCheckTime )
    gLogger.info( "setResourceStatus_2" )  
    return resQuery  
  
################################################################################
   
  types_setResourceScheduledStatus = [ str, str, str, str, str, 
                                      ( datetime, NoneType ), ( datetime, NoneType ), 
                                      ( datetime, NoneType ), ( datetime, NoneType ), 
                                      ( datetime, NoneType ) ]    
       
  def export_setResourceScheduledStatus( self, resourceName, statusType, status, 
                                         reason, tokenOwner, tokenExpiration, 
                                         dateCreated, dateEffective, dateEnd, 
                                         lastCheckTime ):

    gLogger.info( "setResourceScheduledStatus_1" )
    resQuery = rsDB.setResourceScheduledStatus( resourceName, statusType, status, 
                                                reason, tokenOwner, tokenExpiration, 
                                                dateCreated, dateEffective, dateEnd, 
                                                lastCheckTime )
    gLogger.info( "setResourceScheduledStatus_2" )  
    return resQuery  
         
################################################################################         
         
  types_updateResourceStatus = [ str, ( str, NoneType ), ( str, NoneType ), 
                                ( str, NoneType ), ( str, NoneType ), 
                                ( datetime, NoneType ), ( datetime, NoneType ), 
                                ( datetime, NoneType ), ( datetime, NoneType ), 
                                ( datetime, NoneType ) ]   
         
  def export_updateResourceStatus( self, resourceName, statusType, status, reason, 
                                   tokenOwner, tokenExpiration, dateCreated, 
                                   dateEffective, dateEnd, lastCheckTime ):

    gLogger.info( "updateResourceStatus_1" )
    resQuery = rsDB.updateResourceStatus( resourceName, statusType, status, reason, 
                                          tokenOwner, tokenExpiration, dateCreated, 
                                          dateEffective, dateEnd, lastCheckTime )
    gLogger.info( "updateResourceStatus_2" )  
    return resQuery  
  
################################################################################
        
  types_getResources = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                         ( str, NoneType ), ( str, NoneType ), dict ]      
        
  def export_getResources( self, resourceName, resourceType, serviceType, 
                           siteName, gridSiteName, kwargs ):

    gLogger.info( "getResources_1" )
    resQuery = rsDB.getResources( resourceName, resourceType, serviceType, 
                                  siteName, gridSiteName, **kwargs )
    gLogger.info( "getResources_2" )  
    return resQuery  
  
################################################################################
     
  types_getResourcesStatus = [ ( str, NoneType ), ( str, NoneType ), 
                               ( str, NoneType ), ( str, NoneType ), 
                               ( str, NoneType ), ( datetime, NoneType ), 
                               ( datetime, NoneType ), ( datetime, NoneType ), 
                               ( datetime, NoneType ), ( datetime, NoneType ),
                               dict ]    
      
  def export_getResourcesStatus( self, resourceName, statusType, status, reason, 
                                 tokenOwner, tokenExpiration, dateCreated, 
                                 dateEffective, dateEnd, lastCheckTime, kwargs ):

    gLogger.info( "getResourcesStatus_1" )
    resQuery = rsDB.getResourcesStatus( resourceName, statusType, status, reason, 
                                        tokenOwner, tokenExpiration, dateCreated, 
                                        dateEffective, dateEnd, lastCheckTime, 
                                        **kwargs )
    gLogger.info( "getResourcesStatus_2" )  
    return resQuery  
  
################################################################################
      
  types_getResourcesHistory = [ ( str, NoneType ), ( str, NoneType ), 
                                ( str, NoneType ), ( str, NoneType ), 
                                ( str, NoneType ), ( datetime, NoneType ), 
                                ( datetime, NoneType ), ( datetime, NoneType ), 
                                ( datetime, NoneType ), ( datetime, NoneType ),
                                dict ]    
               
  def export_getResourcesHistory( self, resourceName, statusType, status, reason, 
                                  tokenOwner, tokenExpiration, dateCreated, 
                                  dateEffective, dateEnd, lastCheckTime, kwargs ):

    gLogger.info( "getResourcesHistory_1" )
    resQuery = rsDB.getResourcesHistory( resourceName, statusType, status, reason, 
                                         tokenOwner, tokenExpiration, dateCreated, 
                                         dateEffective, dateEnd, lastCheckTime, 
                                         **kwargs )
    gLogger.info( "getResourcesHistory_2" )  
    return resQuery  
    
################################################################################
        
  types_getResourcesScheduledStatus = [ ( str, NoneType ), ( str, NoneType ), 
                                        ( str, NoneType ), ( str, NoneType ), 
                                        ( str, NoneType ), ( datetime, NoneType ), 
                                        ( datetime, NoneType ), ( datetime, NoneType ), 
                                        ( datetime, NoneType ), ( datetime, NoneType ),
                                        dict ]      
        
  def export_getResourcesScheduledStatus( self, resourceName, statusType, status,
                                          reason, tokenOwner, tokenExpiration, 
                                          dateCreated, dateEffective, dateEnd, 
                                          lastCheckTime, kwargs ):
    
    gLogger.info( "getResourcesScheduledStatus_1" )
    resQuery = rsDB.getResourcesScheduledStatus( resourceName, statusType, status,
                                                 reason, tokenOwner, tokenExpiration, 
                                                 dateCreated, dateEffective, dateEnd, 
                                                 lastCheckTime, **kwargs )
    gLogger.info( "getResourcesScheduledStatus_2" )  
    return resQuery  
  
################################################################################
      
  types_getResourcesPresent = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ), 
                                ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                                ( str, NoneType ), ( str, NoneType ), ( datetime, NoneType ),
                                ( str, NoneType ), ( datetime, NoneType ), ( str, NoneType ),
                                ( datetime, NoneType ), ( str, NoneType ), dict ]    
      
  def export_getResourcesPresent( self, resourceName, siteName, serviceType, gridSiteName, 
                                  siteType, resourceType, statusType, status, 
                                  dateEffective, reason, lastCheckTime, tokenOwner, 
                                  tokenExpiration, formerStatus, kwargs ):

    gLogger.info( "getResourcesPresent_1" )
    resQuery = rsDB.getResourcesPresent( resourceName, siteName, serviceType, 
                                         gridSiteName, siteType, resourceType, 
                                         statusType, status, dateEffective, 
                                         reason, lastCheckTime, tokenOwner, 
                                         tokenExpiration, formerStatus, **kwargs )
    gLogger.info( "getResourcesPresent_2" )  
    return resQuery  
  
################################################################################
     
  types_deleteResources = [ str ]   
     
  def export_deleteResources( self, resourceName ):
    
    gLogger.info( "deleteResources_1" )
    resQuery = rsDB.deleteResources( resourceName )
    gLogger.info( "deleteResources_2" )  
    return resQuery    
  
################################################################################
      
  types_deleteResourcesScheduledStatus = [ ( str, NoneType ), ( str, NoneType ), 
                                           ( str, NoneType ), ( str, NoneType ), 
                                           ( str, NoneType ), ( datetime, NoneType ), 
                                           ( datetime, NoneType ), ( datetime, NoneType ), 
                                           ( datetime, NoneType ), ( datetime, NoneType ) ]    
      
  def export_deleteResourcesScheduledStatus( self, resourceName, statusType, status, 
                                             reason, tokenOwner, tokenExpiration, 
                                             dateCreated, dateEffective, dateEnd, 
                                             lastCheckTime ):

    gLogger.info( "deleteResourcesScheduledStatus_1" )
    resQuery = rsDB.deleteResourcesScheduledStatus( resourceName, statusType, status, 
                                                    reason, tokenOwner, tokenExpiration, 
                                                    dateCreated, dateEffective, 
                                                    dateEnd, lastCheckTime )
    gLogger.info( "deleteResourcesScheduledStatus_2" )  
    return resQuery    
  
################################################################################
      
  types_deleteResourcesHistory = [ ( str, NoneType ), ( str, NoneType ), 
                                   ( str, NoneType ), ( str, NoneType ), 
                                   ( str, NoneType ), ( datetime, NoneType ), 
                                   ( datetime, NoneType ), ( datetime, NoneType ), 
                                   ( datetime, NoneType ), ( datetime, NoneType ), 
                                   dict ]    
      
  def export_deleteResourcesHistory( self, resourceName, statusType, status, reason, 
                                     tokenOwner, tokenExpiration, dateCreated, 
                                     dateEffective, dateEnd, lastCheckTime, kwargs ):

    gLogger.info( "deleteResourcesHistory_1" )
    resQuery = rsDB.deleteResourcesHistory( resourceName, statusType, status, reason, 
                                            tokenOwner, tokenExpiration, dateCreated, 
                                            dateEffective, dateEnd, lastCheckTime, 
                                            **kwargs )
    gLogger.info( "deleteResourcesHistory_2" )  
    return resQuery    

################################################################################

################################################################################
# StorageElements functions
################################################################################

################################################################################

  types_addOrModifyStorageElement = [ str, str, str ]
   
  def export_addOrModifyStorageElement( self, storageElementName, resourceName, 
                                        gridSiteName ):

    gLogger.info( "addOrModifyStorageElement_1" )
    resQuery = rsDB.addOrModifyStorageElement( storageElementName, resourceName, 
                                               gridSiteName )
    gLogger.info( "addOrModifyStorageElement_2" )  
    return resQuery    
  
################################################################################

  types_setStorageElementStatus = [ str, str, str, str, str, ( datetime, NoneType ),
                                    ( datetime, NoneType ),( datetime, NoneType ),
                                    ( datetime, NoneType ),( datetime, NoneType ) ]
         
  def export_setStorageElementStatus( self, storageElementName, statusType, status, 
                                      reason, tokenOwner, tokenExpiration, 
                                      dateCreated, dateEffective, dateEnd, 
                                      lastCheckTime ):

    gLogger.info( "setStorageElementStatus_1" )
    resQuery = rsDB.setStorageElementStatus( storageElementName, statusType, status, 
                                             reason, tokenOwner, tokenExpiration, 
                                             dateCreated, dateEffective, dateEnd, 
                                             lastCheckTime )
    gLogger.info( "setStorageElementStatus_2" )  
    return resQuery    
  
################################################################################
             
  types_setStorageElementScheduledStatus = [ str, str, str, str, str, 
                                            ( datetime, NoneType ), ( datetime, NoneType ),
                                            ( datetime, NoneType ), ( datetime, NoneType ),
                                            ( datetime, NoneType ) ]           
             
  def export_setStorageElementScheduledStatus( self, storageElementName, statusType, 
                                               status, reason, tokenOwner, tokenExpiration, 
                                               dateCreated, dateEffective, dateEnd, 
                                               lastCheckTime ):
    
    gLogger.info( "setStorageElementScheduledStatus_1" )
    resQuery = rsDB.setStorageElementScheduledStatus( storageElementName, statusType, 
                                                      status, reason, tokenOwner, 
                                                      tokenExpiration, dateCreated, 
                                                      dateEffective, dateEnd, 
                                                      lastCheckTime )
    gLogger.info( "setStorageElementScheduledStatus_2" )  
    return resQuery    
  
################################################################################
             
  types_updateStorageElementStatus = [ str, ( str, NoneType ), ( str, NoneType ),
                                       ( str, NoneType ), ( str, NoneType ), 
                                       ( datetime, NoneType ), ( datetime, NoneType ),
                                       ( datetime, NoneType ), ( datetime, NoneType ),
                                       ( datetime, NoneType ) ]           
             
  def export_updateStorageElementStatus( self, storageElementName, statusType, status, 
                                         reason, tokenOwner, tokenExpiration, 
                                         dateCreated, dateEffective, dateEnd, 
                                         lastCheckTime ):

    gLogger.info( "updateStorageElementStatus_1" )
    resQuery = rsDB.updateStorageElementStatus( storageElementName, statusType, 
                                                status, reason, tokenOwner, 
                                                tokenExpiration, dateCreated, 
                                                dateEffective, dateEnd, lastCheckTime )
    gLogger.info( "updateStorageElementStatus_2" )  
    return resQuery    

################################################################################
             
  types_getStorageElements = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ),
                               dict ]           
             
  def export_getStorageElements( self, storageElementName, resourceName, 
                                 gridSiteName, kwargs ):

    gLogger.info( "getStorageElements_1" )
    resQuery = rsDB.getStorageElements( storageElementName, resourceName, 
                                        gridSiteName, **kwargs )
    gLogger.info( "getStorageElements_2" )  
    return resQuery    
  
################################################################################
             
  types_getStorageElementsStatus = [ ( str, NoneType ), ( str, NoneType ), 
                                     ( str, NoneType ), ( str, NoneType ), 
                                     ( str, NoneType ), ( datetime, NoneType ),
                                     ( datetime, NoneType ), ( datetime, NoneType ),
                                     ( datetime, NoneType ), ( datetime, NoneType ),
                                     dict ]           
             
  def export_getStorageElementsStatus( self, storageElementName, statusType, 
                                       status, reason, tokenOwner, tokenExpiration, 
                                       dateCreated, dateEffective, dateEnd, 
                                       lastCheckTime, kwargs ):

    gLogger.info( "getStorageElementsStatus_1" )
    resQuery = rsDB.getStorageElementsStatus( storageElementName, statusType, 
                                              status, reason, tokenOwner, 
                                              tokenExpiration, dateCreated, 
                                              dateEffective, dateEnd, 
                                              lastCheckTime, **kwargs )
    gLogger.info( "getStorageElementsStatus_2" )  
    return resQuery    
  
################################################################################
          
  types_getStorageElementsHistory = [ ( str, NoneType ), ( str, NoneType ), 
                                      ( str, NoneType ), ( str, NoneType ), 
                                      ( str, NoneType ), ( datetime, NoneType ),
                                      ( datetime, NoneType ), ( datetime, NoneType ),
                                      ( datetime, NoneType ), ( datetime, NoneType ),
                                      dict ]        
          
  def export_getStorageElementsHistory( self, storageElementName, statusType, 
                                        status, reason, tokenOwner, tokenExpiration, 
                                        dateCreated, dateEffective, dateEnd, 
                                        lastCheckTime, kwargs ):
    
    gLogger.info( "getStorageElementsHistory_1" )
    resQuery = rsDB.getStorageElementsHistory( storageElementName, statusType, 
                                               status, reason, tokenOwner, 
                                               tokenExpiration, dateCreated, 
                                               dateEffective, dateEnd, 
                                               lastCheckTime, **kwargs )
    gLogger.info( "getStorageElementsHistory_2" )  
    return resQuery    
  
################################################################################
            
  types_getStorageElementsScheduledStatus = [ ( str, NoneType ), ( str, NoneType ), 
                                              ( str, NoneType ), ( str, NoneType ), 
                                              ( str, NoneType ), ( datetime, NoneType ),
                                              ( datetime, NoneType ), ( datetime, NoneType ),
                                              ( datetime, NoneType ), ( datetime, NoneType ),
                                              dict ]          
            
  def export_getStorageElementsScheduledStatus( self, storageElementName, statusType, 
                                                status, reason, tokenOwner, 
                                                tokenExpiration, dateCreated, 
                                                dateEffective, dateEnd, 
                                                lastCheckTime, kwargs ):

    gLogger.info( "getStorageElementsScheduledStatus_1" )
    resQuery = rsDB.getStorageElementsScheduledStatus( storageElementName, statusType, 
                                                       status, reason, tokenOwner, 
                                                       tokenExpiration, dateCreated, 
                                                       dateEffective, dateEnd, 
                                                       lastCheckTime, **kwargs )
    gLogger.info( "getStorageElementsScheduledStatus_2" )  
    return resQuery

################################################################################
           
  types_getStorageElementsPresent = [ ( str, NoneType ), ( str, NoneType ),
                                      ( str, NoneType ), ( str, NoneType ),
                                      ( str, NoneType ), ( str, NoneType ),
                                      ( datetime, NoneType ), ( str, NoneType ),
                                      ( datetime, NoneType ), ( str, NoneType ),
                                      ( datetime, NoneType ), ( str, NoneType ),
                                      dict ]         
           
  def export_getStorageElementsPresent( self, storageElementName, resourceName, 
                                        gridSiteName, siteType, statusType, 
                                        status, dateEffective, reason, 
                                        lastCheckTime, tokenOwner,tokenExpiration, 
                                        formerStatus, kwargs ):

    gLogger.info( "getStorageElementsPresent_1" )
    resQuery = rsDB.getStorageElementsPresent( storageElementName, resourceName, 
                                               gridSiteName, siteType, statusType, 
                                               status, dateEffective, reason, 
                                               lastCheckTime, tokenOwner,tokenExpiration, 
                                               formerStatus, **kwargs )
    gLogger.info( "getStorageElementsPresent_2" )  
    return resQuery

################################################################################
  
  types_deleteStorageElements = [ str ]
                                         
  def export_deleteStorageElements( self, storageElementName ):

    gLogger.info( "deleteStorageElements_1" )
    resQuery = rsDB.deleteStorageElements( storageElementName )
    gLogger.info( "deleteStorageElements_2" )  
    return resQuery
  
################################################################################
       
  types_deleteStorageElementsScheduledStatus = [ ( str, NoneType ), ( str, NoneType ),
                                                 ( str, NoneType ), ( str, NoneType ),
                                                 ( str, NoneType ), ( datetime, NoneType ), 
                                                 ( datetime, NoneType ), ( datetime, NoneType ),
                                                 ( datetime, NoneType ), ( datetime, NoneType )
                                                 ]     
       
  def export_deleteStorageElementsScheduledStatus( self, storageElementName, statusType, 
                                                   status, reason, tokenOwner, 
                                                   tokenExpiration, dateCreated, 
                                                   dateEffective, dateEnd, 
                                                   lastCheckTime ):

    gLogger.info( "deleteStorageElementsScheduledStatus_1" )
    resQuery = rsDB.deleteStorageElementsScheduledStatus( storageElementName, statusType, 
                                                          status, reason, tokenOwner, 
                                                          tokenExpiration, dateCreated, 
                                                          dateEffective, dateEnd, 
                                                          lastCheckTime )
    gLogger.info( "deleteStorageElementsScheduledStatus_2" )  
    return resQuery
  
################################################################################
      
  types_deleteStorageElementsHistory = [ ( str, NoneType ), ( str, NoneType ),
                                         ( str, NoneType ), ( str, NoneType ),
                                         ( str, NoneType ), ( datetime, NoneType ), 
                                         ( datetime, NoneType ), ( datetime, NoneType ),
                                         ( datetime, NoneType ), ( datetime, NoneType ),
                                         dict ]   
      
  def export_deleteStorageElementsHistory( self, storageElementName, statusType, 
                                           status, reason, tokenOwner, tokenExpiration, 
                                           dateCreated, dateEffective, dateEnd, 
                                           lastCheckTime, kwargs ):          

    gLogger.info( "deleteStorageElementsHistory_1" )
    resQuery = rsDB.deleteStorageElementsHistory( storageElementName, statusType, 
                                                  status, reason, tokenOwner, 
                                                  tokenExpiration, dateCreated, 
                                                  dateEffective, dateEnd, 
                                                  lastCheckTime, **kwargs )
    gLogger.info( "deleteStorageElementsHistory_2" )  
    return resQuery


################################################################################
  
################################################################################
# Stats functions
################################################################################
  
################################################################################  

  types_getServiceStats = [ str, ( str, NoneType ) ]  

  def export_getServiceStats( self, siteName, statusType ):
    
    gLogger.info( "getServiceStats_1" )
    resQuery = rsDB.getServiceStats( siteName, statusType )
    gLogger.info( "getServiceStats_2" )  
    return resQuery

################################################################################      
      
  types_getResourceStats = [ str, str, ( str, NoneType ) ]    
      
  def export_getResourceStats( self, element, name, statusType ):
    
    gLogger.info( "getResourceStats_1" )
    resQuery = rsDB.getResourceStats( element, name, statusType )
    gLogger.info( "getResourceStats_2" )  
    return resQuery
  
################################################################################  
       
  types_getStorageelementStats = [ str, str, ( str, NoneType ) ]   
     
  def export_getStorageElementStats( self, element, name, statusType ):
          
    gLogger.info( "getStorageElementStats_1" )
    resQuery = rsDB.getStorageElementStats( element, name, statusType )
    gLogger.info( "getStorageElementStats_2" )  
    return resQuery  
  
################################################################################

################################################################################
# GridSites functions
################################################################################

################################################################################  
  
  types_addOrModifyGridSite = [ str, str ]
  
  def export_addOrModifyGridSite( self, gridSiteName, gridTier ):

    gLogger.info( "addOrModifyGridSite_1" )
    resQuery = rsDB.addOrModifyGridSite( gridSiteName, gridTier )
    gLogger.info( "addOrModifyGridSite_2" )  
    return resQuery

################################################################################
      
  types_getGridSites = [ ( str, NoneType ), ( str, NoneType ), dict ]    
      
  def export_getGridSites( self, gridSiteName, gridTier, kwargs ):

    gLogger.info( "getGridSites_1" )
    resQuery = rsDB.getGridSites( gridSiteName, gridTier, **kwargs )
    gLogger.info( "getGridSites_2" )  
    return resQuery

################################################################################
  
  types_deleteGridSites = [ str ]
    
  def export_deleteGridSites( self, gridSiteName ):         

    gLogger.info( "deleteGridSites_1" )
    resQuery = rsDB.deleteGridSites( gridSiteName )
    gLogger.info( "deleteGridSites_2" )  
    return resQuery

################################################################################

################################################################################
# Misc functions
################################################################################

################################################################################

  types_getGeneralName = [ str, str, str ]
 
  def export_getGeneralName( self, from_element, name, to_element ):
    
    gLogger.info( "getGeneralName_1" )
    resQuery = rsDB.getGeneralName( from_element, name, to_element )
    gLogger.info( "getGeneralName_2" )  
    return resQuery

################################################################################

  types_getGridSiteName = [ str, str ]       
         
  def export_getGridSiteName( self, granularity, name ):

    gLogger.info( "getGridSiteName_1" )
    resQuery = rsDB.getGridSiteName( granularity, name )
    gLogger.info( "getGridSiteName_2" )  
    return resQuery

################################################################################

  types_getTokens = [ str, ( str, NoneType ), ( datetime, NoneType ),
                      ( str, NoneType ), dict ]       
         
  def export_getTokens( self, granularity, name, tokenExpiration, statusType, kwargs ): 

    gLogger.info( "getTokens_1" )
    resQuery = rsDB.getTokens( granularity, name, tokenExpiration, statusType, **kwargs )
    gLogger.info( "getTokens_2" )  
    return resQuery

################################################################################
       
  types_setToken = [ str, str, str, str, str, datetime ]    
       
  def export_setToken( self, granularity, name, statusType, reason, tokenOwner, 
                       tokenExpiration ):

    gLogger.info( "setToken_1" )
    resQuery = rsDB.setToken( granularity, name, statusType, reason, tokenOwner, 
                              tokenExpiration )
    gLogger.info( "setToken_2" )  
    return resQuery


################################################################################

  types_setReason = [ str, str, str, str ]
         
  def export_setReason( self, granularity, name, statusType, reason ):     

    gLogger.info( "setReason_1" )
    resQuery = rsDB.setReason( granularity, name, statusType, reason )
    gLogger.info( "setReason_2" )  
    return resQuery

################################################################################

  types_whatIs = [ str ]
         
  def export_whatIs( self, name ):  

    gLogger.info( "whatIs_1" )
    resQuery = rsDB.whatIs( name )
    gLogger.info( "whatIs_2" )  
    return resQuery

################################################################################

  types_getStuffToCheck = [ str, dict, dict ]       
     
  def export_getStuffToCheck( self, granularity, checkFrequency, kwargs ):

    gLogger.info( "getStuffToCheck_1" )
    resQuery = rsDB.getStuffToCheck( granularity, checkFrequency, kwargs )
    gLogger.info( "getStuffToCheck_2" )  
    return resQuery
    
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################



#############################################################################
#
#  types_getSite = [ str ]
#  def export_getSite( self, siteName ):
#
#    gLogger.info( "ResourceStatusHandler.getSite: Attempting to get Site" )
#
#    try:
#      resQuery = rsDB.getSites( siteName = siteName )
#      gLogger.info( "ResourceStatusHandler.getSite: got Site" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#      
#    errorStr += '\n ' + where( self, self.export_getSite )
#    return S_ERROR( errorStr )
#
##############################################################################
#
#  types_getSitesList = []
#  def export_getSitesList( self ):
#    """
#    Get sites list from the ResourceStatusDB.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getSitesList: Attempting to get sites list" )
#
#    try:
#      resQuery = rsDB.getSites()
#      gLogger.info( "ResourceStatusHandler.getSitesList: got sites list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#              
#    errorStr += '\n ' + where( self, self.export_getSitesList )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getSitesStatusWeb = [ dict, int, int ]
#  def export_getSitesStatusWeb( self, selectDict, startItem, maxItems ):
#    """ get present sites status list, for the web
#        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`
#
#        :Parameters:
#          `selectDict`
#            {
#              'SiteName':'name of a site' --- present status
#              'ExpandSiteHistory':'name of a site' --- site status history
#            }
#
#          `sortList`
#            (no sorting provided)
#
#          `startItem`
#
#          `maxItems`
#
#        :return:
#        {
#          'OK': XX,
#
#          'rpcStub': XX, 'getSitesStatusWeb', ({}, [], X, X)),
#
#          Value':
#          {
#
#            'ParameterNames': ['SiteName', 'Tier', 'GridType', 'Country', 'Status',
#             'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'],
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
#    gLogger.info( "ResourceStatusHandler.getSitesStatusWeb: Attempting to get sites list" )
#
#    try:
#      res = rsDB.getMonitoredsStatusWeb( 'Site', selectDict, startItem, maxItems )
#      gLogger.info( "ResourceStatusHandler.getSitesStatusWeb: got sites list" )
#      return res
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#        
#    errorStr += '\n ' + where( self, self.export_getSitesStatusWeb )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getSitesStatusList = []
#  def export_getSitesStatusList( self ):
#    """
#    Get sites list from the ResourceStatusDB.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getSitesList: Attempting to get sites list" )
#
#    res = []
#
#    try:
#      kwargs = { 'columns' : [ 'SiteName', 'Status' ] } 
#      r = rsDB.getSitesPresent( **kwargs )  
#      #r = rsDB.getMonitoredsList( 'Site', paramsList = [ 'SiteName', 'Status' ] )
#      for x in r:
#        res.append( x )
#      gLogger.info( "ResourceStatusHandler.getSitesList: got sites and status list" )
#      return S_OK( res )
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#              
#    errorStr += '\n ' + where( self, self.export_getSitesStatusList )
#    return S_ERROR( errorStr )

#############################################################################

#############################################################################
# Services functions
#############################################################################

#############################################################################

#  types_getService = [ str ]
#  def export_getService( self, serviceName ):
#
#    gLogger.info( "ResourceStatusHandler.getService: Attempting to get Service" )
#
#    try:
#      resQuery = rsDB.getServices( serviceName = serviceName )
#      gLogger.info( "ResourceStatusHandler.getService: got Service" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#        
#    errorStr += '\n ' + where( self, self.export_getService )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getServicesList = []
#  def export_getServicesList( self ):
#    """
#    Get services list from the ResourceStatusDB.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getServicesList: Attempting to get services list" )
#
#    try:
#      resQuery = rsDB.getServices()
#      gLogger.info( "ResourceStatusHandler.getServicesList: got services list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getServicesList )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getServicesStatusWeb = [ dict, int, int ]
#  def export_getServicesStatusWeb( self, selectDict, startItem, maxItems ):
#    """
#    Get present services status list, for the web.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`
#
#    :Parameters
#      `selectDict`
#        { 'ServiceName':['XX', ...] , 'ExpandServiceHistory': ['XX', ...], 'Status': ['XX', ...]}
#
#      `sortList`
#
#      `startItem`
#
#      `maxItems`
#
#    :return: {
#      `ParameterNames`: ['ServiceName', 'ServiceType', 'Site', 'GridType', 'Country',
#      'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'],
#
#      'Records': [[], [], ...],
#
#      'TotalRecords': X,
#
#      'Extras': {}
#
#      }
#    """
#
#    gLogger.info( "ResourceStatusHandler.getServicesStatusWeb: Attempting to get services list" )
#
#    try:
#      resQuery = rsDB.getMonitoredsStatusWeb( 'Service', selectDict, startItem, maxItems )
#      gLogger.info( "ResourceStatusHandler.getServicesStatusWeb: got services list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getServicesStatusWeb )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getServiceStats = [ str, ( str, NoneType ) ]
#  def export_getServiceStats( self, siteName, statusType ):
#    """
#    Returns simple statistics of active, probing and banned services of a site;
#
#    :Parameters
#      `siteName`
#        string - a site name
#
#    :returns:
#      S_OK { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
#      or S_Error
#    """
#
#    gLogger.info( "ResourceStatusHandler.getServiceStats: Attempting to get service stats for site %s" % siteName )
#
#    try:
#      resQuery = rsDB.getServiceStats( siteName, statusType )
#      gLogger.info( "ResourceStatusHandler.getServiceStats: got service stats" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getServiceStats )
#    return S_ERROR( errorStr )

#############################################################################

#############################################################################
# Resources functions
#############################################################################

#############################################################################

#  types_getResource = [ str ]
#  def export_getResource( self, resourceName ):
#
#    gLogger.info( "ResourceStatusHandler.getResource: Attempting to get Resource" )
#
#    try:
#      resQuery = rsDB.getResources( resourceName = resourceName )
#      gLogger.info( "ResourceStatusHandler.getResource: got Resource" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getResource )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getResourcesList = []
#  def export_getResourcesList( self ):
#    """
#    Get resources list from the ResourceStatusDB.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getResourcesList: Attempting to get resources list" )
#
#    try:
#      resQuery = rsDB.getResources()
#      gLogger.info( "ResourceStatusHandler.getResourcesList: got resources list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getResourcesList )
#    return S_ERROR( errorStr )
    
#############################################################################

#  types_getResourcesStatusWeb = [ dict, int, int ]
#  def export_getResourcesStatusWeb( self, selectDict, startItem, maxItems ):
#    """ get present resources status list
#        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`
#
#
#        :Parameters:
#          `selectDict`
#            {'ResourceName':'name of a resource' --- present status
#
#          `ExpandResourceHistory`
#            'name of a resource' --- resource status history }
#
#          `sortList`
#            [] (no sorting provided)
#
#          `startItem`
#
#          `maxItems`
#
#        `return`: { 'OK': XX, 'rpcStub': XX, 'getSitesStatusWeb', ({}, [], X, X)),
#
#          'Value': { 'ParameterNames': ['ResourceName', 'SiteName', 'ServiceExposed', 'Country',
#          'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'],
#
#          'Records': [[], [], ...]
#
#          'TotalRecords': X,
#
#          'Extras': {} } }
#    """
#
#    gLogger.info( "ResourceStatusHandler.getResourcesStatusWeb: Attempting to get resources list" )
#
#    try:
#      resQuery = rsDB.getMonitoredsStatusWeb( 'Resource', selectDict, startItem, maxItems )
#      gLogger.info( "ResourceStatusHandler.getResourcesStatusWeb: got resources list" )  
#      return resQuery
#    except RSSDBException, x: 
#      errorStr = whoRaised( x ) 
#    except RSSException, x: 
#      errorStr = whoRaised( x ) 
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getResourcesStatusWeb )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getResourceStats = [ str, str, ( str, NoneType ) ]
#  def export_getResourceStats( self, granularity, name, statusType ):
#    """
#    Returns simple statistics of active, probing and banned resources of a site or service;
#
#    :Parameters:
#      `granularity`
#        string, should be in ['Site', 'Service']
#
#      `name`
#        string, name of site or service
#
#    :return:
#      S_OK { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
#      or S_ERROR
#    """
#
#    gLogger.info( "ResourceStatusHandler.getResourceStats: Attempting to get resource stats for site %s" % name )
#
#    try:
#      resQuery = rsDB.getResourceStats( granularity, name, statusType )
#      gLogger.info( "ResourceStatusHandler.getResourceStats: got resource stats" )
#      return resQuery  
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getResourceStats )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getCEsList = []
#  def export_getCEsList( self ):
#    """
#    Get CEs list from the ResourceStatusDB.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getCEsList: Attempting to get CEs list" )
#
#    try:
#      resQuery = rsDB.getResources( resourceType = [ 'CE', 'CREAMCE' ] )
#      gLogger.info( "ResourceStatusHandler.getCEsList: got CEs list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )  
#        
#    errorStr += '\n ' + where( self, self.export_getCEsList )
#    return S_ERROR( errorStr )

#############################################################################

#############################################################################
# StorageElements functions
#############################################################################

#############################################################################

#  types_getStorageElement = [ str ]
#  def export_getStorageElement( self, storageElementName ):
#
#    gLogger.info( "ResourceStatusHandler.getStorageElement: Attempting to get SE" )
#
#    try:
#      resQuery = rsDB.getStorageElements( storageElementName )
#      gLogger.info( "ResourceStatusHandler.getStorageElement: got SE" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getStorageElement )
#    return S_ERROR( errorStr )
   
#############################################################################

#  types_getStorageElementsList = [ ]
#  def export_getStorageElementsList( self ):
#    """
#    Get sites list from the ResourceStatusDB.
#
#        :Parameters:
#
#          `access` : string - Read or Write
#
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
#    """
#
#    gLogger.info("ResourceStatusHandler.getStorageElementsList: Attempting to get sites list")
#
#    try:
#      resQuery = rsDB.getStorageElements( )
#      gLogger.info( "ResourceStatusHandler.getStorageElementsList: got sites list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#      
#    errorStr += '\n ' + where( self, self.export_getStorageElementsList )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getStorageElementsStatusWeb = [ dict, int, int ]
#  def export_getStorageElementsStatusWeb( self, selectDict, startItem, maxItems ):
#    """ Get present sites status list, for the web
#        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`
#
#
#        :Parameters:
#          `selectDict`
#            {
#              'StorageElementName':'name of a site' --- present status
#              'ExpandStorageElementHistory':'name of a site' --- site status history
#            }
#
#          `sortList`
#            [] (no sorting provided)
#
#          `startItem`
#
#          `maxItems`
#
#          `access`
#            Read or Write
#
#        :return:
#        {
#          'OK': XX,
#
#          'rpcStub': XX, 'getStorageElementsStatusWeb', ({}, [], X, X)),
#
#          Value':
#          {
#
#            'ParameterNames': ['StorageElementName', 'Tier', 'GridType', 'Country', 'Status',
#             'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'],
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
#    gLogger.info( "ResourceStatusHandler.getStorageElementsStatusWeb: Attempting to get SEs list" )
#
#    try:
#      resQuery = rsDB.getMonitoredsStatusWeb( 'StorageElement', selectDict, startItem, maxItems )
#      gLogger.info( "ResourceStatusHandler.getStorageElementsStatusWeb: got SEs list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#      
#    errorStr += '\n ' + where( self, self.export_getStorageElementsStatusWeb )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getStorageElementStats = [ str, str, ( str, NoneType ) ]
#  def export_getStorageElementStats( self, granularity, name, statusType ):
#    """
#    Returns simple statistics of active, probing and banned storageElementss of a site or resource;
#
#    :Parameters:
#      `granularity`
#        string, should be in ['Site', 'Resource']
#
#      `name`
#        string, name of site or service
#
#      `access`
#        string, Read or Write
#
#    :return:
#      S_OK { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
#      or S_Error
#    """
#
#    gLogger.info( "StorageElementsStatusHandler.getStorageElementStats: Attempting to get storageElements stats for %s" % name )
#
#    try:
#      resQuery = rsDB.getStorageElementStats( granularity, name, statusType )
#      gLogger.info( "StorageElementsStatusHandler.getStorageElementStats: got storageElements stats" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )        
#
#    errorStr += '\n ' + where( self, self.export_getStorageElementStats )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getSESitesList = [ ]
#  def export_getSESitesList( self ):
#    """
#    Get sites list of the storage elements from the ResourceStatusDB.
#
#        :Parameters:
#
#          `access` : string - Read or Write
#
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getSESitesList: Attempting to get SE sites list" )
#
#    try:
#
#      res = []
#
#      try:
#        resQuery = rsDB.getStorageElements()
#        for se in resQuery[ 'Value' ]:
#          gridSite = se[ 4 ]  
#          
#          DIRACsites = getDIRACSiteName( gridSite[ 0 ] )
#          if not DIRACsites[ 'OK' ]:
#            raise RSSException, "No DIRAC site name" + where( self, self.export_getSESitesList )
#          DIRACsites = DIRACsites[ 'Value' ]
#          for DIRACsite in DIRACsites:
#            if DIRACsite not in res:
#              res.append( DIRACsite )
#          
#
#      except RSSDBException, x:
#        gLogger.error( whoRaised( x ) )
#      except RSSException, x:
#        gLogger.error( whoRaised( x ) )
#
#      gLogger.info( "ResourceStatusHandler.getSESitesList: got SE sites list" )
#      return S_OK( res )
#
#    except Exception:
#      errorStr = where( self, self.export_getSitesList )
#      gLogger.exception( errorStr )
#      return S_ERROR( errorStr )
  
#############################################################################

#############################################################################
# GridSites functions
#############################################################################

#############################################################################  
      
#  types_getGridSitesList = []
#  def export_getGridSitesList( self ):
#    """
#    Get sites list from the ResourceStatusDB.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getGridSitesList`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getGridSitesList: Attempting to get sites list" )
#
#    try:
#      resQuery = rsDB.getGridSitesList()
#      gLogger.info( "ResourceStatusHandler.getGridSitesList: got sites list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getGridSitesList )
#    return S_ERROR( errorStr )      
      
#############################################################################

#  types_getGridSiteName = [ str, str ]
#  def export_getGridSiteName( self, granularity, name ):
#    """
#    Get Grid Site Name, given granularity and a name.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getGridSiteName`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getGridSiteName: Attempting to get the Grid Site Name" )
#
#    try:
#      resQuery = rsDB.getGridSiteName( granularity, name )
#      gLogger.info( "ResourceStatusHandler.getGridSiteName: got GridSiteName list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getGridSiteName )
#    return S_ERROR( errorStr )
           
#############################################################################

#############################################################################
# Mixed functions
#############################################################################

#############################################################################

#  types_getStatusList = []
#  def export_getStatusList( self ):
#    """
#    Get status list from the ResourceStatusDB.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getStatusList`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getStatusList: got status list" )
#    return S_OK( ValidStatus )

#############################################################################

#  types_getCountries = [ str ]
#  def export_getCountries( self, granularity ):
#    """
#    Get countries list from the ResourceStatusDB.
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getCountries`
#    """
#
#    gLogger.info( "ResourceStatusHandler.getCountries: Attempting to get countries list" )
#
#    try:
#      resQuery = rsDB.getCountries( granularity )
#      gLogger.info( "ResourceStatusHandler.getCountries: got countries list" )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getCountries )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getPeriods = [ str, str, str, int ]
#  def export_getPeriods( self, granularity, name, status, hours ):
#    """ get periods of time when name was in status (for a total of hours hours)
#    """
#
#    gLogger.info( "ResourceStatusHandler.getPeriods: Attempting to get %s periods when it was in %s" % ( name, status ) )
#
#    try:
#      resQuery = rsDB.getPeriods( granularity, name, status, int( hours ) )
#      gLogger.info( "ResourceStatusHandler.getPeriods: got %s periods" % name )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getPeriods )
#    return S_ERROR( errorStr )
        
#############################################################################

#  types_getGeneralName = [ str, str, str ]
#  def export_getGeneralName( self, granularity, name, toGranularity ):
#    """ get General Name
#    """
#
#    gLogger.info( "ResourceStatusHandler.getGeneralName: Attempting to get %s general name" % name )
#
#    try:
#      resQuery = rsDB.getGeneralName( granularity, name, toGranularity )
#      gLogger.info( "ResourceStatusHandler.getGeneralName: got %s general name" % name )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#
#    errorStr += '\n ' + where( self, self.export_getGeneralName )
#    return S_ERROR( errorStr )

#############################################################################

#  types_getDownTimesWeb = [dict, list, int, int]
#  def export_getDownTimesWeb(self, selectDict, sortList, startItem, maxItems):
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
#    try:
#      gLogger.info("ResourceStatusHandler.getDownTimesWeb: Attempting to get down times list")
#      try:
#        try:
#          granularity = selectDict['Granularity']
#        except KeyError:
#          granularity = []
#
#        if not isinstance(granularity, list):
#          granularity = [granularity]
#        commands = []
#        if granularity == []:
#          commands = ['DTEverySites', 'DTEveryResources']
#        elif 'Site' in granularity:
#          commands.append('DTEverySites')
#        elif 'Resource' in granularity:
#          commands.append('DTEveryResources')
#
#        try:
#          severity = selectDict['Severity']
#        except KeyError:
#          severity = []
#        if not isinstance(severity, list):
#          severity = [severity]
#        if severity == []:
#          severity = ['AT_RISK', 'OUTAGE']
#
#        res = rsDB.getClientsCacheStuff(['Name', 'Opt_ID', 'Value', 'Result', 'CommandName'],
#                                        commandName = commands)
#        records = []
#
#        if not ( res == () ):
#          made_IDs = []
#
#          for dt_tuple in res:
#            considered_ID = dt_tuple[1]
#            if considered_ID not in made_IDs:
#              name = dt_tuple[0]
#              if dt_tuple[4] == 'DTEverySites':
#                granularity = 'Site'
#              elif dt_tuple[4] == 'DTEveryResources':
#                granularity = 'Resource'
#              toTake = ['Severity', 'StartDate', 'EndDate', 'Description']
#
#              for dt_t in res:
#                if considered_ID == dt_t[1]:
#                  if toTake != []:
#                    if dt_t[2] in toTake:
#                      if dt_t[2] == 'Severity':
#                        sev = dt_t[3]
#                        toTake.remove('Severity')
#                      if dt_t[2] == 'StartDate':
#                        startDate = dt_t[3]
#                        toTake.remove('StartDate')
#                      if dt_t[2] == 'EndDate':
#                        endDate = dt_t[3]
#                        toTake.remove('EndDate')
#                      if dt_t[2] == 'Description':
#                        description = dt_t[3]
#                        toTake.remove('Description')
#
#              now = datetime.datetime.utcnow().replace(microsecond = 0, second = 0)
#              startDate_datetime = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M')
#              endDate_datetime = datetime.datetime.strptime(endDate, '%Y-%m-%d %H:%M')
#
#              if endDate_datetime < now:
#                when = 'Finished'
#              else:
#                if startDate_datetime < now:
#                  when = 'OnGoing'
#                else:
#                  hours = str(convertTime(startDate_datetime - now, 'hours'))
#                  when = 'In ' + hours + ' hours.'
#
#              if sev in severity:
#                records.append([ considered_ID, granularity, name, sev,
#                                when, startDate, endDate, description ])
#
#              made_IDs.append(considered_ID)
#
#        # adding downtime links to the GOC DB page in Extras
#        DT_links = []
#        for record in records:
#          DT_link = rsDB.getClientsCacheStuff(['Result'], opt_ID = record[0], value = 'Link')
#          DT_link = DT_link[0][0]
#          DT_links.append({ record[0] : DT_link } )
#
#        paramNames = ['ID', 'Granularity', 'Name', 'Severity', 'When', 'Start', 'End', 'Description']
#
#        finalDict = {}
#        finalDict['TotalRecords'] = len(records)
#        finalDict['ParameterNames'] = paramNames
#
#        # Return all the records if maxItems == 0 or the specified number otherwise
#        if maxItems:
#          finalDict['Records'] = records[startItem:startItem+maxItems]
#        else:
#          finalDict['Records'] = records
#
#        finalDict['Extras'] = DT_links
#
#
#      except RSSDBException, x:
#        gLogger.error(whoRaised(x))
#      except RSSException, x:
#        gLogger.error(whoRaised(x))
#      gLogger.info("ResourceStatusHandler.getDownTimesWeb: got DT list")
#      return S_OK(finalDict)
#    except Exception:
#      errorStr = where(self, self.export_getDownTimesWeb)
#      gLogger.exception(errorStr)
#      return S_ERROR(errorStr)

#############################################################################

#  types_enforcePolicies = [str, str, BooleanType]
#  def export_enforcePolicies(self, granularity, name, useNewRes = True):
#    """ Enforce all the policies. If `useNewRes` is False, use cached results only (where available).
#    """
#    try:
#      gLogger.info("ResourceStatusHandler.enforcePolicies: Attempting to enforce policies for %s %s" % (granularity, name))
#      try:
#        reason = serviceType = resourceType = None
#
#        res = rsDB.getStuffToCheck(granularity, name = name)[0]
#        status = res[1]
#        formerStatus = res[2]
#        siteType = res[3]
#        tokenOwner = res[len(res)-1]
#        if granularity == 'Resource':
#          resourceType = res[4]
#        elif granularity == 'Service':
#          serviceType = res[4]
#
#        from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
#        pep = PEP(VOExtension, granularity, name, status, formerStatus, reason, siteType,
#                  serviceType, resourceType, tokenOwner, useNewRes)
#        pep.enforce(rsDBIn = rsDB)
#
#      except RSSDBException, x:
#        gLogger.error(whoRaised(x))
#      except RSSException, x:
#        gLogger.error(whoRaised(x))
#      gLogger.info("ResourceStatusHandler.enforcePolicies: enforced for %s: %s" % (granularity, name))
#      return S_OK("ResourceStatusHandler.enforcePolicies: enforced for %s: %s" % (granularity, name))
#    except Exception:
#      errorStr = where(self, self.export_getCachedResult)
#      gLogger.exception(errorStr)
#      return S_ERROR(errorStr)

#############################################################################

#  types_publisher = [str, str, BooleanType]
#  def export_publisher(self, granularity, name, useNewRes = False):
#    """ get a view
#
#    :Parameters:
#      `granularity`
#        string - a ValidRes
#
#      `name`
#        string - name of the res
#
#      `useNewRes`
#        boolean. When set to true, will get new results,
#        otherwise it will get cached results (where available).
#    """
#    try:
#      gLogger.info("ResourceStatusHandler.publisher: Attempting to get info for %s: %s" % (granularity, name))
#      try:
#        if useNewRes == True:
#          from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
#          gLogger.info("ResourceStatusHandler.publisher: Recalculating policies for %s: %s" % (granularity, name))
#          if granularity in ('Site', 'Sites'):
#            res = rsDB.getStuffToCheck(granularity, name = name)[0]
#            status = res[1]
#            formerStatus = res[2]
#            siteType = res[3]
#            tokenOwner = res[4]
#
#            pep = PEP(VOExtension, granularity, name, status, formerStatus, None, siteType,
#                      None, None, tokenOwner, useNewRes)
#            pep.enforce(rsDBIn = rsDB)
#
#            res = rsDB.getMonitoredsList('Service', paramsList = ['ServiceName'], siteName = name)
#            services = [x[0] for x in res]
#            for s in services:
#              res = rsDB.getStuffToCheck('Service', name = s)[0]
#              status = res[1]
#              formerStatus = res[2]
#              siteType = res[3]
#              serviceType = res[4]
#
#              pep = PEP(VOExtension, 'Service', s, status, formerStatus, None, siteType,
#                        serviceType, None, tokenOwner, useNewRes)
#              pep.enforce(rsDBIn = rsDB)
#          else:
#            reason = serviceType = resourceType = None
#
#            res = rsDB.getStuffToCheck(granularity, name = name)[0]
#            status = res[1]
#            formerStatus = res[2]
#            siteType = res[3]
#            tokenOwner = res[len(res)-1]
#            if granularity == 'Resource':
#              resourceType = res[4]
#            elif granularity == 'Service':
#              serviceType = res[4]
#
#            from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
#            pep = PEP(VOExtension, granularity, name, status, formerStatus, reason, siteType,
#                      serviceType, resourceType, tokenOwner, useNewRes)
#            pep.enforce(rsDBIn = rsDB)
#
#        res = publisher.getInfo(granularity, name, useNewRes)
#      except InvalidRes, x:
#        errorStr = "Invalid granularity"
#        gLogger.exception(whoRaised(x) + errorStr)
#        return S_ERROR(errorStr)
#      except RSSException, x:
#        errorStr = "RSSException"
#        gLogger.exception(whoRaised(x) + errorStr)
#      gLogger.info("ResourceStatusHandler.publisher: got info for %s: %s" % (granularity, name))
#      return S_OK(res)
#    except Exception:
#      errorStr = where(self, self.export_publisher)
#      gLogger.exception(errorStr)
#      return S_ERROR(errorStr)

#############################################################################

#  types_reAssignToken = [ str, str, str ]
#  def export_reAssignToken( self, granularity, name, requester ):
#    """
#    Re-assign a token: if it was assigned to a human, assign it to 'RS_SVC' and viceversa.
#    """
#
#    str_ = "ResourceStatusHandler.reAssignToken: attempting to re-assign token "
#    str_ = str_ + "%s: %s: %s" % ( granularity, name, requester )
#    gLogger.info( str_ )
#
#    try:
#      token      = rsDB.getTokens( granularity, name = name )
#      tokenOwner = token[ 0 ][ 1 ]
#      if tokenOwner == 'RS_SVC':
#        if requester != 'RS_SVC':
#          rsDB.setToken( granularity, name, requester, datetime.utcnow() + timedelta( hours = 24 ) )
#      else:
#        rsDB.setToken( granularity, name, 'RS_SVC', datetime( 9999, 12, 31, 23, 59, 59 ) )
#
#      gLogger.info( "ResourceStatusHandler.reAssignToken: re-assigned token %s: %s: %s" % ( granularity, name, requester ) )
#      return S_OK()
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#              
#    errorStr += '\n ' + where( self, self.export_reAssignToken )
#    return S_ERROR( errorStr )

#############################################################################

#  types_extendToken = [ str, str, int ]
#  def export_extendToken( self, granularity, name, hrs ):
#    """
#    Extend the duration of token by the number of provided hours.
#    """
#
#    str_ = "ResourceStatusHandler.extendToken: attempting to extend token "
#    str_ = str_ + "%s: %s for %i hours" % ( granularity, name, hrs )
#    gLogger.info( str_ )
#
#    try:
#      token              = rsDB.getTokens( granularity, name )
#      tokenOwner         = token[ 0 ][ 1 ]
#      tokenExpiration    = token[ 0 ][ 2 ]
#      tokenNewExpiration = tokenExpiration
#      try:
#        tokenNewExpiration = tokenExpiration + timedelta( hours = hrs )
#      except OverflowError:
#        pass
#      rsDB.setToken( granularity, name, tokenOwner, tokenNewExpiration )
#      gLogger.info( "ResourceStatusHandler.extendToken: extended token %s: %s for %i hours" % ( granularity, name, hrs ) )
#      return S_OK()
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#              
#    errorStr += '\n ' + where( self, self.export_extendToken )
#    return S_ERROR( errorStr )
      
#############################################################################

#  types_whatIs = [ str ]
#  def export_whatIs( self, name ):
#    """
#    Find which is the granularity of name.
#    """
#
#    gLogger.info( "ResourceStatusHandler.whatIs: attempting to find granularity of %s" % name )
#
#    try:
#      resQuery = rsDB.whatIs( name )
#      gLogger.info( "ResourceStatusHandler.whatIs: got %s granularity" % name )
#      return resQuery
#    except RSSDBException, x:
#      errorStr = whoRaised( x )
#    except RSSException, x:
#      errorStr = whoRaised( x )
#    except Exception, x:
#      errorStr = whoRaised( x )
#        
#    errorStr += '\n ' + where( self, self.export_whatIs )
#    return S_ERROR( errorStr )

#############################################################################
