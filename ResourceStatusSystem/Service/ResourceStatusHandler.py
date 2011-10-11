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
#from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
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

from DIRAC.ResourceStatusSystem.Utilities.Decorators import HandlerExecution

db = False

def initializeResourceStatusHandler( _serviceInfo ):

  global db
  db = ResourceStatusDB()

#  rmDB = ResourceManagementDB()

#  cc = CommandCaller()

#  global VOExtension
#  VOExtension = getExt()

#  ig = InfoGetter( VOExtension )

#  WMSAdmin = RPCClient( "WorkloadManagement/WMSAdministrator" )

#  global publisher
#  publisher = Publisher( VOExtension, dbIn = db, commandCallerIn = cc,
#                         infoGetterIn = ig, WMSAdminIn = WMSAdmin )

  sync_O = Synchronizer()
  gConfig.addListenerToNewVersionEvent( sync_O.sync )

  return S_OK()

class ResourceStatusHandler( RequestHandler ):

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
# Sites functions
################################################################################

################################################################################

  types_addOrModifySite = [ str, str, str ]

  @HandlerExecution
  def export_addOrModifySite( self, siteName, siteType, gridSiteName ):
    return db  

################################################################################

  types_setSiteStatus = [ str, str, str, str, ( datetime, NoneType ),
                          ( datetime, NoneType ), ( datetime, NoneType ),
                          ( datetime, NoneType ), ( str, NoneType ), 
                          ( datetime, NoneType ) ]    

  @HandlerExecution
  def export_setSiteStatus( self, siteName, statusType, status, reason, dateCreated, 
                            dateEffective, dateEnd, lastCheckTime, tokenOwner,
                            tokenExpiration ):
    return db

################################################################################
    
  types_setSiteScheduledStatus = [ str, str, str, str, ( datetime, NoneType ),
                                   ( datetime, NoneType ), ( datetime, NoneType ),
                                   ( datetime, NoneType ), ( str, NoneType), 
                                   ( datetime, NoneType ) ]
    
  @HandlerExecution
  def export_setSiteScheduledStatus( self, siteName, statusType, status, reason, 
                                     dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration ):
    return db

################################################################################
      
  types_updateSiteStatus = [ str, str, ( str, NoneType ), 
                            ( str, NoneType ),
                            ( datetime, NoneType ), ( datetime, NoneType ), 
                            ( datetime, NoneType ), ( datetime, NoneType ),
                            ( str, NoneType ), ( datetime, NoneType ) ]  

  @HandlerExecution    
  def export_updateSiteStatus( self, siteName, statusType, status, reason, 
                               dateCreated, dateEffective, dateEnd, lastCheckTime, 
                               tokenOwner, tokenExpiration ):
    return db
      
################################################################################

  types_getSites = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                     ( str, list, NoneType ), dict ]
  
  @HandlerExecution    
  def export_getSites( self, siteName, siteType, gridSiteName, kwargs ):
    return db

################################################################################

  types_getSitesStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                          ( str, list, NoneType ), ( str, list, NoneType ), 
                          ( datetime, list, NoneType ), 
                          ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                          ( datetime, list, NoneType ), ( str, list, NoneType ), 
                          ( datetime, list, NoneType ), dict ]

  @HandlerExecution
  def export_getSitesStatus( self, siteName, statusType, status, reason, 
                             dateCreated, dateEffective, dateEnd, lastCheckTime, 
                             tokenOwner, tokenExpiration, kwargs ):
    return db
    
################################################################################  
  
  types_getSitesHistory = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( datetime, list, NoneType ), 
                            ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                            ( datetime, list, NoneType ), ( str, list, NoneType ), 
                            ( datetime, list, NoneType ), dict ]

  @HandlerExecution
  def export_getSitesHistory( self, siteName, statusType, status, reason, 
                              dateCreated, dateEffective, dateEnd, lastCheckTime, 
                              tokenOwner, tokenExpiration, kwargs ):
    return db

################################################################################

  types_getSitesScheduledStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                    ( str, list, NoneType ), ( str, list, NoneType ), 
                                    ( datetime, list, NoneType ), 
                                    ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                                    ( datetime, list, NoneType ), ( str, list, NoneType ), 
                                    ( datetime, list, NoneType ), dict ]

  @HandlerExecution
  def export_getSitesScheduledStatus( self, siteName, statusType, status, reason, 
                                      dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                      tokenOwner, tokenExpiration, kwargs ):
    return db

################################################################################

  types_getSitesPresent = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( datetime, list, NoneType ), ( str, list, NoneType ), 
                            ( datetime, list, NoneType ), ( str, list, NoneType ), 
                            ( datetime, list, NoneType ), ( str, list, NoneType ), 
                            dict ]

  @HandlerExecution
  def export_getSitesPresent( self, siteName, siteType, gridSiteName, gridTier, 
                              statusType, status, dateEffective, reason, 
                              lastCheckTime, tokenOwner, tokenExpiration, 
                              formerStatus, kwargs ):
    return db

################################################################################    

  types_deleteSites = [ ( str, list ) ]

  @HandlerExecution
  def export_deleteSites( self, siteName ):
    return db
    
################################################################################

  types_deleteSitesScheduledStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                       ( str, list, NoneType ), ( str, list, NoneType ),
                                       ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                       ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                                       ( str, list, NoneType ), ( datetime, list, NoneType ),
                                       dict ]

  @HandlerExecution
  def export_deleteSitesScheduledStatus( self, siteName, statusType, status, 
                                         reason, dateCreated, dateEffective, dateEnd, 
                                         lastCheckTime, tokenOwner, tokenExpiration, kwargs ):
    return db

################################################################################

  types_deleteSitesHistory = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), ( str, list, NoneType ),
                               ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                               ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                               ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]

  @HandlerExecution
  def export_deleteSitesHistory( self, siteName, statusType, status, 
                                 reason, dateCreated, dateEffective, dateEnd, 
                                 lastCheckTime, tokenOwner, tokenExpiration, kwargs ):
    return db

################################################################################

################################################################################
# Services functions
################################################################################

################################################################################

  types_addOrModifyService = [ str, str, str ]
  
  def export_addOrModifyService( self, serviceName, serviceType, siteName ):
    return db
  
################################################################################  
  
  types_setServiceStatus = [ str, str, str, str, ( datetime, NoneType ),
                            ( datetime, NoneType ), ( datetime, NoneType ),
                            ( datetime, NoneType ), ( str, NoneType), 
                            ( datetime, NoneType ) ]  
    
  def export_setServiceStatus( self, serviceName, statusType, status, reason, 
                               dateCreated, dateEffective, dateEnd, 
                               lastCheckTime,tokenOwner, tokenExpiration ):
    return db
   
################################################################################   
    
  types_setServiceScheduledStatus = [ str, str, str, str, ( datetime, NoneType ),
                                      ( datetime, NoneType ),( datetime, NoneType ),
                                      ( datetime, NoneType ),( str, NoneType),
                                      ( datetime, NoneType ) ]  
          
  def export_setServiceScheduledStatus( self, serviceName, statusType, status, 
                                        reason, 
                                        dateCreated, dateEffective, dateEnd, 
                                        lastCheckTime, tokenOwner, tokenExpiration ):
    return db

  
################################################################################  
    
  types_updateServiceStatus = [ str, str, ( str, NoneType ),
                                ( str, NoneType ), ( str, NoneType ), 
                                ( datetime, NoneType ), ( datetime, NoneType ),
                                ( datetime, NoneType ),( datetime, NoneType ), 
                                ( str, NoneType ), ( datetime, NoneType ) ]  
    
  def export_updateServiceStatus( self, serviceName, statusType, status, reason, dateCreated, 
                                  dateEffective, dateEnd, lastCheckTime, 
                                  tokenOwner, tokenExpiration ):
    return db
    

  
################################################################################  
  
  types_getServices = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                        ( str, list, NoneType ), dict ]
  
  def export_getServices( self, serviceName, serviceType, siteName, kwargs ):
    return db

  
################################################################################  
    
  types_getServicesStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                              ( str, list, NoneType ), ( str, list, NoneType ), 
                              ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                              ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                              ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]  
    
  def export_getServicesStatus( self, serviceName, statusType, status, reason, dateCreated, 
                                dateEffective, dateEnd, lastCheckTime, 
                                tokenOwner, tokenExpiration, kwargs ):
    return db
  
################################################################################  

  types_getServicesHistory = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                               ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                               ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]
      
  def export_getServicesHistory( self, serviceName, statusType, status, reason, dateCreated, 
                                 dateEffective, dateEnd, lastCheckTime, 
                                 tokenOwner, tokenExpiration, kwargs ):
    return db



################################################################################  

  types_getServicesScheduledStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                       ( str, list, NoneType ), ( str, list, NoneType ), 
                                       ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                       ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                                       ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]
    
  def export_getServicesScheduledStatus( self, serviceName, statusType, status, 
                                         reason, dateCreated, dateEffective, dateEnd, 
                                         lastCheckTime, tokenOwner, tokenExpiration, kwargs ):
    return db

    
################################################################################  
    
  types_getServicesPresent = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), ( str, list, NoneType ),
                               ( datetime, list, NoneType ), ( str, list, NoneType ), 
                               ( datetime, list, NoneType ), ( str, list, NoneType ), 
                               ( datetime, list, NoneType ), ( str, list, NoneType ), 
                               dict ]  
    
  def export_getServicesPresent( self, serviceName, siteName, siteType, serviceType, 
                                 statusType, status, dateEffective, reason, 
                                 lastCheckTime, tokenOwner, tokenExpiration, 
                                 formerStatus, kwargs ):
    return db    

  
################################################################################  
    
  types_deleteServices = [ ( str, list ) ]  
    
  def export_deleteServices( self, serviceName ):
    return db 
  
################################################################################  
    
  types_deleteServicesScheduledStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                          ( str, list, NoneType ), ( str, list, NoneType ), 
                                          ( str, list, NoneType ), ( datetime, list, NoneType ),
                                          ( str, list, NoneType ), ( str, list, NoneType ), 
                                          ( str, list, NoneType ), ( str, list, NoneType ),
                                          dict ]  
    
  def export_deleteServicesScheduledStatus( self, serviceName, statusType, status, 
                                            reason, dateCreated, dateEffective, dateEnd, 
                                            lastCheckTime, tokenOwner, tokenExpiration, 
                                            kwargs ):
    return db


################################################################################
    
  types_deleteServicesHistory = [ ( str, NoneType ), ( str, NoneType ), ( str, NoneType ), 
                                  ( str, NoneType ), ( str, NoneType ), ( datetime, NoneType ),
                                  ( str, NoneType ), ( str, NoneType ), ( str, NoneType ), 
                                  ( str, NoneType ), dict ]  
    
  def export_deleteServicesHistory( self, serviceName, statusType, status, reason, 
                                    dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                    tokenOwner, tokenExpiration, kwargs ):                                              
    return db


################################################################################

################################################################################
# Resources functions
################################################################################

################################################################################

  types_addOrModifyResource = [ str, str, str, str, str ]

  def export_addOrModifyResource( self, resourceName, resourceType, serviceType, 
                                  siteName, gridSiteName ):
    return db
  
################################################################################
  
  types_setResourceStatus = [ str, str, str, str, ( datetime, NoneType ),
                              ( datetime, NoneType ), ( datetime, NoneType ),
                              ( datetime, NoneType ), ( str, NoneType), 
                              ( datetime, NoneType ) ]
      
  def export_setResourceStatus( self, resourceName, statusType, status, reason, 
                                dateCreated, dateEffective, dateEnd, 
                                lastCheckTime, tokenOwner, tokenExpiration ):
    return db
    
################################################################################
   
  types_setResourceScheduledStatus = [ str, str, str, str, ( datetime, NoneType ),
                                      ( datetime, NoneType ), ( datetime, NoneType ),
                                      ( datetime, NoneType ), ( str, NoneType), 
                                      ( datetime, NoneType ) ]    
       
  def export_setResourceScheduledStatus( self, resourceName, statusType, status, 
                                         reason, dateCreated, dateEffective, dateEnd, 
                                         lastCheckTime, tokenOwner, tokenExpiration ):
    return db
         
################################################################################         
         
  types_updateResourceStatus = [ str, str, ( str, NoneType ),
                                ( str, NoneType ), ( str, NoneType ), 
                                ( datetime, NoneType ), ( datetime, NoneType ),
                                ( datetime, NoneType ),( datetime, NoneType ), 
                                ( str, NoneType ), ( datetime, NoneType ) ]   
         
  def export_updateResourceStatus( self, resourceName, statusType, status, reason, 
                                   dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                   tokenOwner, tokenExpiration ):
    return db
  
################################################################################
        
  types_getResources = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                         ( str, list, NoneType ), ( str, list, NoneType ), 
                         ( str, list, NoneType ), dict ]      
        
  def export_getResources( self, resourceName, resourceType, serviceType, 
                           siteName, gridSiteName, kwargs ):
    return db
  
################################################################################
     
  types_getResourcesStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                               ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                               ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]    
      
  def export_getResourcesStatus( self, resourceName, statusType, status, reason, 
                                 dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                 tokenOwner, tokenExpiration, kwargs ):
    return db 
  
################################################################################
      
  types_getResourcesHistory = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                               ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                               ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]    
               
  def export_getResourcesHistory( self, resourceName, statusType, status, reason, 
                                  dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                  tokenOwner, tokenExpiration, kwargs ):
    return db
      
################################################################################
        
  types_getResourcesScheduledStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                        ( str, list, NoneType ), ( str, list, NoneType ), 
                                        ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                        ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                                        ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]      
        
  def export_getResourcesScheduledStatus( self, resourceName, statusType, status,
                                          reason, dateCreated, dateEffective, dateEnd, 
                                          lastCheckTime, tokenOwner, tokenExpiration, 
                                          kwargs ): 
    return db
  
################################################################################
      
  types_getResourcesPresent = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                ( str, list, NoneType ), ( str, list, NoneType ), 
                                ( str, list, NoneType ), ( str, list, NoneType ),
                                ( str, list, NoneType ), ( str, list, NoneType ), 
                                ( datetime, list, NoneType ), ( str, list, NoneType ), 
                                ( datetime, list, NoneType ), ( str, list, NoneType ),
                                ( datetime, list, NoneType ), ( str, list, NoneType ), dict ]    
      
  def export_getResourcesPresent( self, resourceName, siteName, serviceType, gridSiteName, 
                                  siteType, resourceType, statusType, status, 
                                  dateEffective, reason, lastCheckTime, tokenOwner, 
                                  tokenExpiration, formerStatus, kwargs ):
    return db
  
################################################################################
     
  types_deleteResources = [ ( str, list ) ]   
     
  def export_deleteResources( self, resourceName ):
    return db 
  
################################################################################
      
  types_deleteResourcesScheduledStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                           ( str, list, NoneType ), ( str, list, NoneType ),
                                           ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                           ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                                           ( str, list, NoneType ), ( datetime, list, NoneType ),
                                           dict ]    
      
  def export_deleteResourcesScheduledStatus( self, resourceName, statusType, status, 
                                             reason, dateCreated, dateEffective, dateEnd, 
                                             lastCheckTime, tokenOwner, tokenExpiration, 
                                             kwargs ):
    return db
     
################################################################################
      
  types_deleteResourcesHistory = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                   ( str, list, NoneType ), ( str, list, NoneType ),
                                   ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                   ( datetime, list, NoneType ), ( datetime, list, NoneType ), 
                                   ( str, list, NoneType ), ( datetime, list, NoneType ),
                                   dict ]    
      
  def export_deleteResourcesHistory( self, resourceName, statusType, status, reason, 
                                     dateCreated, dateEffective, dateEnd, lastCheckTime,
                                     tokenOwner, tokenExpiration, kwargs ):
    return db
  
################################################################################

################################################################################
# StorageElements functions
################################################################################

################################################################################

  types_addOrModifyStorageElement = [ str, str, str ]
   
  def export_addOrModifyStorageElement( self, storageElementName, resourceName, 
                                        gridSiteName ):
    return db
  
################################################################################

  types_setStorageElementStatus = [ str, str, str, str, ( datetime, NoneType ),
                                    ( datetime, NoneType ), ( datetime, NoneType ),
                                    ( datetime, NoneType ), ( str, NoneType), 
                                    ( datetime, NoneType ) ]
         
  def export_setStorageElementStatus( self, storageElementName, statusType, status, 
                                      reason, dateCreated, dateEffective, dateEnd, 
                                      lastCheckTime, tokenOwner, tokenExpiration ):
    return db
    
################################################################################
             
  types_setStorageElementScheduledStatus = [ str, str, str, str, ( datetime, NoneType ),
                                             ( datetime, NoneType ), ( datetime, NoneType ),
                                             ( datetime, NoneType ), ( str, NoneType), 
                                             ( datetime, NoneType ) ]           
             
  def export_setStorageElementScheduledStatus( self, storageElementName, statusType, 
                                               status, reason, dateCreated, dateEffective, 
                                               dateEnd, lastCheckTime, tokenOwner, 
                                               tokenExpiration ):
    return db
  
################################################################################
             
  types_updateStorageElementStatus = [ str, str, ( str, NoneType ),
                                       ( str, NoneType ), ( str, NoneType ), 
                                       ( datetime, NoneType ), ( datetime, NoneType ),
                                       ( datetime, NoneType ),( datetime, NoneType ), 
                                       ( str, NoneType ), ( datetime, NoneType ) ]           
             
  def export_updateStorageElementStatus( self, storageElementName, statusType, status, 
                                         reason, dateCreated, dateEffective, dateEnd, 
                                         lastCheckTime, tokenOwner, tokenExpiration ):
    return db  

################################################################################
             
  types_getStorageElements = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), dict ]           
             
  def export_getStorageElements( self, storageElementName, resourceName, 
                                 gridSiteName, kwargs ):
    return db
    
################################################################################
             
  types_getStorageElementsStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                     ( str, list, NoneType ), ( str, list, NoneType ), 
                                     ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                     ( datetime, list, NoneType ), ( datetime, list, NoneType ),  
                                     ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]           
             
  def export_getStorageElementsStatus( self, storageElementName, statusType, 
                                       status, reason, dateCreated, dateEffective, dateEnd, 
                                       lastCheckTime, tokenOwner, tokenExpiration, kwargs ):
    return db
    
################################################################################
          
  types_getStorageElementsHistory = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                      ( str, list, NoneType ), ( str, list, NoneType ), 
                                      ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                      ( datetime, list, NoneType ), ( datetime, list, NoneType ),  
                                      ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]        
          
  def export_getStorageElementsHistory( self, storageElementName, statusType, 
                                        status, reason, dateCreated, dateEffective, dateEnd, 
                                        lastCheckTime, tokenOwner, tokenExpiration, kwargs ):
    return db
    
################################################################################
            
  types_getStorageElementsScheduledStatus = [ ( str, list, NoneType ), ( str, list, NoneType ), 
                                              ( str, list, NoneType ), ( str, list, NoneType ), 
                                              ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                              ( datetime, list, NoneType ), ( datetime, list, NoneType ),  
                                              ( str, list, NoneType ), ( datetime, list, NoneType ), dict ]          
            
  def export_getStorageElementsScheduledStatus( self, storageElementName, statusType, 
                                                status, reason, dateCreated, 
                                                dateEffective, dateEnd, 
                                                lastCheckTime, tokenOwner, 
                                                tokenExpiration, kwargs ):
    return db
  
################################################################################
           
  types_getStorageElementsPresent = [ ( str, list, NoneType ), ( str, list, NoneType ),
                                      ( str, list, NoneType ), ( str, list, NoneType ),
                                      ( str, list, NoneType ), ( str, list, NoneType ),
                                      ( datetime, list, NoneType ), ( str, list, NoneType ),
                                      ( datetime, list, NoneType ), ( str, list, NoneType ),
                                      ( datetime, list, NoneType ), ( str, list, NoneType ),
                                      dict ]         
           
  def export_getStorageElementsPresent( self, storageElementName, resourceName, 
                                        gridSiteName, siteType, statusType, 
                                        status, dateEffective, reason, 
                                        lastCheckTime, tokenOwner,tokenExpiration, 
                                        formerStatus, kwargs ):
    return db
  
################################################################################
  
  types_deleteStorageElements = [ ( str, list ) ]
                                         
  def export_deleteStorageElements( self, storageElementName ):
    return db
    
################################################################################
       
  types_deleteStorageElementsScheduledStatus = [ ( str, list, NoneType ), ( str, list, NoneType ),
                                                 ( str, list, NoneType ), ( str, list, NoneType ),
                                                 ( str, list, NoneType ), ( datetime, list, NoneType ), 
                                                 ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                                 ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                                 dict ]     
       
  def export_deleteStorageElementsScheduledStatus( self, storageElementName, statusType, 
                                                   status, reason, dateCreated, 
                                                   dateEffective, dateEnd, 
                                                   lastCheckTime, tokenOwner, 
                                                   tokenExpiration, kwargs ):
    return db
    
################################################################################
      
  types_deleteStorageElementsHistory = [ ( str, list, NoneType ), ( str, list, NoneType ),
                                         ( str, list, NoneType ), ( str, list, NoneType ),
                                         ( str, list, NoneType ), ( datetime, list, NoneType ), 
                                         ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                         ( datetime, list, NoneType ), ( datetime, list, NoneType ),
                                         dict ]   
      
  def export_deleteStorageElementsHistory( self, storageElementName, statusType, 
                                           status, reason, dateCreated, dateEffective, 
                                           dateEnd, lastCheckTime, tokenOwner, 
                                           tokenExpiration, kwargs ):          
    return db

################################################################################
  
################################################################################
# Stats functions
################################################################################
  
################################################################################  

  types_getServiceStats = [ str, ( str, NoneType ) ]  

  def export_getServiceStats( self, siteName, statusType ):
    
    gLogger.info( "getServiceStats_1" )
    resQuery = db.getServiceStats( siteName, statusType )
    gLogger.info( "getServiceStats_2" )  
    return resQuery

################################################################################      
      
  types_getResourceStats = [ str, str, ( str, NoneType ) ]    
      
  def export_getResourceStats( self, element, name, statusType ):
    
    gLogger.info( "getResourceStats_1" )
    resQuery = db.getResourceStats( element, name, statusType )
    gLogger.info( "getResourceStats_2" )  
    return resQuery
  
################################################################################  
       
  types_getStorageelementStats = [ str, str, ( str, NoneType ) ]   
     
  def export_getStorageElementStats( self, element, name, statusType ):
          
    gLogger.info( "getStorageElementStats_1" )
    resQuery = db.getStorageElementStats( element, name, statusType )
    gLogger.info( "getStorageElementStats_2" )  
    return resQuery  
  
################################################################################

################################################################################
# GridSites functions
################################################################################

################################################################################  
  
  types_addOrModifyGridSite = [ str, str ]
  
  @HandlerExecution
  def export_addOrModifyGridSite( self, gridSiteName, gridTier ):
    return db

################################################################################
      
  types_getGridSites = [ ( str, list, NoneType ), ( str, list, NoneType ), dict ]    
      
  @HandlerExecution    
  def export_getGridSites( self, gridSiteName, gridTier, kwargs ):
    return db

################################################################################
  
  types_deleteGridSites = [ ( str, list ) ]
    
  @HandlerExecution  
  def export_deleteGridSites( self, gridSiteName ):         
    return db

################################################################################

################################################################################
# Misc functions
################################################################################

################################################################################

# Check ResourceStatusBooster.py
    
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
