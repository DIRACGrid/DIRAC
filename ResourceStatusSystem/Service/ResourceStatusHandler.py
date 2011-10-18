################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from datetime                                          import datetime
from types                                             import NoneType

from DIRAC                                             import gConfig, S_OK
from DIRAC.Core.DISET.RequestHandler                   import RequestHandler

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB    import ResourceStatusDB
from DIRAC.ResourceStatusSystem.Utilities.Decorators   import HandlerDec
from DIRAC.ResourceStatusSystem.Utilities.Synchronizer import Synchronizer

db = False

def initializeResourceStatusHandler( _serviceInfo ):

  global db
  db = ResourceStatusDB()

# Publisher is on boxes right now
# 
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
  '''
  The ResourceStatusHandler exposes the DB front-end functions through a XML-RPC
  server.
  
  According to the ResourceStatusDB philosophy, only functions of the type:
    o insert
    o update
    o get
    o delete 
  
  are exposed. If you need anything more complicated, either look for it on the 
  ResourceStatusClient, or code it yourself. This way the DB and the service keep
  clean and tidied.

  To can use this service on this way, but you MUST NOT DO IT. Use it through the
  ResourceStatusClient. If offers in the worst case as good performance as the 
  ResourceStatusHandler, if not better.

   >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
   >>> server = RPCCLient("ResourceStatus/ResourceStatus")
   
  If you want to know more about ResourceStatusHandler, scroll down to the end of
  the file.  
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
  # SITE FUNCTIONS
  ##############################################################################
  '''
  __site_IU = [ str, str, str ]
  __site_GD = [ ( t, list, NoneType ) for t in __site_IU ] + [ dict ] 

  types_insertSite = __site_IU
  @HandlerDec
  def export_insertSite( self, siteName, siteType, gridSiteName ):
    return db  

  types_updateSite = __site_IU
  @HandlerDec
  def export_updateSite( self, siteName, siteType, gridSiteName ):
    return db  

  types_getSite = __site_GD
  @HandlerDec    
  def export_getSite( self, siteName, siteType, gridSiteName, kwargs ):
    return db
  
  types_deleteSite = __site_GD
  @HandlerDec    
  def export_deleteSite( self, siteName, siteType, gridSiteName, kwargs ):
    return db  

  '''
  ##############################################################################
  # SITE STATUS FUNCTIONS
  ##############################################################################
  '''
  __siteStatus_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                      str, datetime ]
  __siteStatus_GD = [ ( t, list ,NoneType ) for t in __siteStatus_IU ] + [ dict ] 

  types_insertSiteStatus = __siteStatus_IU
  @HandlerDec
  def export_insertSiteStatus( self, siteName, statusType, status, reason, 
                               dateCreated, dateEffective, dateEnd, lastCheckTime, 
                               tokenOwner, tokenExpiration ):
    return db

  types_updateSiteStatus = __siteStatus_IU    
  @HandlerDec
  def export_updateSiteStatus( self, siteName, statusType, status, reason, 
                               dateCreated, dateEffective, dateEnd, lastCheckTime, 
                               tokenOwner, tokenExpiration ):
    return db

  types_getSiteStatus = __siteStatus_GD
  @HandlerDec
  def export_getSiteStatus( self, siteName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner, tokenExpiration, kwargs ):
    return db
  
  types_deleteSiteStatus = __siteStatus_GD
  @HandlerDec
  def export_deleteSiteStatus( self, siteName, statusType, status, reason, 
                               dateCreated, dateEffective, dateEnd, lastCheckTime, 
                               tokenOwner, tokenExpiration, kwargs ):
    return db  

  '''
  ##############################################################################
  # SITE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  __siteScheduled_IU = [ str, str, str, str, datetime, datetime, datetime, 
                         datetime, str, datetime  ]
  __siteScheduled_GD = [ ( t, list, NoneType ) for t in __siteScheduled_IU ] + [ dict ] 

  types_insertSiteScheduledStatus = __siteScheduled_IU 
  @HandlerDec
  def export_insertSiteScheduledStatus( self, siteName, statusType, status, reason, 
                                        dateCreated, dateEffective, dateEnd, 
                                        lastCheckTime, tokenOwner, tokenExpiration ):
    return db
  
  types_updateSiteScheduledStatus = __siteScheduled_IU 
  @HandlerDec
  def export_updateSiteScheduledStatus( self, siteName, statusType, status, reason, 
                                        dateCreated, dateEffective, dateEnd, 
                                        lastCheckTime, tokenOwner, tokenExpiration ):
    return db
  
  types_getSiteScheduledStatus = __siteScheduled_GD
  @HandlerDec
  def export_getSiteScheduledStatus( self, siteName, statusType, status, reason, 
                                     dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration, 
                                     kwargs ):
    return db

  types_deleteSiteScheduledStatus = __siteScheduled_GD
  @HandlerDec
  def export_deleteSiteScheduledStatus( self, siteName, statusType, status, reason, 
                                        dateCreated, dateEffective, dateEnd, 
                                        lastCheckTime, tokenOwner, tokenExpiration, 
                                        kwargs ):
    return db   
   
  '''
  ##############################################################################
  # SITE HISTORY FUNCTIONS
  ##############################################################################
  '''   
  __siteHistory_IU = [ str, str, str, str, datetime, datetime, datetime, 
                       datetime, str, datetime  ]
  __siteHistory_GD = [ ( t, list, NoneType) for t in __siteHistory_IU ] + [ dict ] 

  types_insertSiteHistory = __siteHistory_IU 
  @HandlerDec
  def export_insertSiteHistory( self, siteName, statusType, status, reason, 
                                dateCreated, dateEffective, dateEnd, 
                                lastCheckTime, tokenOwner, tokenExpiration ):
    return db
  
  types_updateSiteHistory = __siteHistory_IU 
  @HandlerDec
  def export_updateSiteHistory( self, siteName, statusType, status, reason, 
                                dateCreated, dateEffective, dateEnd, 
                                lastCheckTime, tokenOwner, tokenExpiration ):
    return db
  
  types_getSiteHistory = __siteHistory_GD
  @HandlerDec
  def export_getSiteHistory( self, siteName, statusType, status, reason, 
                             dateCreated, dateEffective, dateEnd, lastCheckTime, 
                             tokenOwner, tokenExpiration, kwargs ):
    return db

  types_deleteSiteHistory = __siteHistory_GD
  @HandlerDec
  def export_deleteSiteHistory( self, siteName, statusType, status, reason, 
                                dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                tokenOwner, tokenExpiration, kwargs ):
    return db  
  
  '''
  ##############################################################################
  # SITE PRESENT FUNCTIONS
  ##############################################################################
  '''   
  __sitePresent   = [ str, str, str, str, str, str, datetime, str, datetime, str,
                      datetime, str,]
  __sitePresent_G = [ ( t, list, NoneType) for t in __sitePresent ] + [ dict ]
  
  types_getSitePresent = __sitePresent_G
  @HandlerDec
  def export_getSitePresent( self, siteName, siteType, gridSiteName, gridTier, 
                             statusType, status, dateEffective, reason, 
                             lastCheckTime, tokenOwner, tokenExpiration, 
                             formerStatus, kwargs ):
    return db

################################################################################
################################################################################

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''
  __ser_IU = [ str, str, str ] 
  __ser_GD = [ ( t, list, NoneType ) for t in __ser_IU ] + [ dict ]
  
  types_insertService = __ser_IU
  @HandlerDec
  def export_insertService( self, serviceName, serviceType, siteName ):
    return db

  types_updateService = __ser_IU
  @HandlerDec
  def export_updateService( self, serviceName, serviceType, siteName ):
    return db
  
  types_getService = __ser_GD 
  @HandlerDec
  def export_getService( self, serviceName, serviceType, siteName, kwargs ):
    return db  

  types_deleteService = __ser_GD 
  @HandlerDec
  def export_deleteService( self, serviceName, serviceType, siteName, kwargs ):
    return db  

  '''
  ##############################################################################
  # SERVICE STATUS FUNCTIONS
  ##############################################################################
  '''  
  __serStatus_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                     str, datetime ] 
  __serStatus_GD = [ ( t, list, NoneType ) for t in __serStatus_IU ] + [ dict ]
  
  types_insertServiceStatus = __serStatus_IU   
  @HandlerDec  
  def export_insertServiceStatus( self, serviceName, statusType, status, reason, 
                                  dateCreated, dateEffective, dateEnd, 
                                  lastCheckTime,tokenOwner, tokenExpiration ):
    return db
  
  types_updateServiceStatus = __serStatus_IU   
  @HandlerDec  
  def export_updateServiceStatus( self, serviceName, statusType, status, reason, 
                                  dateCreated, dateEffective, dateEnd, 
                                  lastCheckTime,tokenOwner, tokenExpiration ):
    return db

  types_getServiceStatus = __serStatus_GD  
  @HandlerDec  
  def export_getServiceStatus( self, serviceName, statusType, status, reason, 
                               dateCreated, dateEffective, dateEnd, lastCheckTime, 
                               tokenOwner, tokenExpiration, kwargs ):
    return db

  types_deleteServiceStatus = __serStatus_GD  
  @HandlerDec  
  def export_deleteServiceStatus( self, serviceName, statusType, status, reason, 
                                  dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                  tokenOwner, tokenExpiration, kwargs ):
    return db
   
  '''
  ##############################################################################
  # SERVICE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''     
  __serScheduled_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                        str, datetime ]
  __serScheduled_GD = [ ( t, list, NoneType ) for t in __serScheduled_IU ] + [ dict ] 
    
  types_insertServiceScheduledStatus = __serScheduled_IU  
  @HandlerDec        
  def export_insertServiceScheduledStatus( self, serviceName, statusType, status, 
                                           reason, dateCreated, dateEffective, 
                                           dateEnd, lastCheckTime, tokenOwner, 
                                           tokenExpiration ):
    return db

  types_updateServiceScheduledStatus = __serScheduled_IU  
  @HandlerDec        
  def export_updateServiceScheduledStatus( self, serviceName, statusType, status, 
                                           reason, dateCreated, dateEffective, 
                                           dateEnd, lastCheckTime, tokenOwner, 
                                           tokenExpiration ):
    return db

  types_getServiceScheduledStatus = __serScheduled_GD
  @HandlerDec    
  def export_getServiceScheduledStatus( self, serviceName, statusType, status, 
                                        reason, dateCreated, dateEffective, dateEnd, 
                                        lastCheckTime, tokenOwner, tokenExpiration, 
                                        kwargs ):
    return db

  types_deleteServiceScheduledStatus = __serScheduled_GD
  @HandlerDec    
  def export_deleteServiceScheduledStatus( self, serviceName, statusType, status, 
                                           reason, dateCreated, dateEffective, 
                                           dateEnd, lastCheckTime, tokenOwner, 
                                           tokenExpiration,kwargs ):
    return db

  '''
  ##############################################################################
  # SERVICE HISTORY FUNCTIONS
  ##############################################################################
  '''  
  __serHistory_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                      str, datetime ]
  __serHistory_GD = [ ( t, list, NoneType ) for t in __serHistory_IU ] + [ dict ] 

  types_insertServiceHistory = __serHistory_IU
  @HandlerDec      
  def export_insertServiceHistory( self, serviceName, statusType, status, reason, 
                                   dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                   tokenOwner, tokenExpiration ):
    return db

  types_updateServiceHistory = __serHistory_IU
  @HandlerDec      
  def export_updateServiceHistory( self, serviceName, statusType, status, reason, 
                                   dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                   tokenOwner, tokenExpiration ):
    return db

  types_getServiceHistory = __serHistory_IU
  @HandlerDec      
  def export_getServiceHistory( self, serviceName, statusType, status, reason, 
                                dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                tokenOwner, tokenExpiration, kwargs ):
    return db

  types_deleteServiceHistory = __serHistory_IU
  @HandlerDec      
  def export_deleteServiceHistory( self, serviceName, statusType, status, reason, 
                                   dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                   tokenOwner, tokenExpiration, kwargs ):
    return db

  '''
  ##############################################################################
  # SERVICE PRESENT FUNCTIONS
  ##############################################################################
  '''    
  __servicePresent   = [ str, str, str, str, str, str, datetime, str, datetime, 
                         str, datetime, str,]
  __servicePresent_G = [ ( t, list, NoneType) for t in __servicePresent ] + [ dict ]  
    
  types_getServicePresent = __servicePresent_G
  @HandlerDec  
  def export_getServicePresent( self, serviceName, siteName, siteType, serviceType, 
                                statusType, status, dateEffective, reason, 
                                lastCheckTime, tokenOwner, tokenExpiration, 
                                formerStatus, kwargs ):
    return db    

################################################################################
################################################################################
    
  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''    
  __res_IU = [ str, str, str, str, str ]
  __res_GD = [ ( t, list, NoneType ) for t in __res_IU ] + [ dict ]

  types_insertResource = __res_IU
  @HandlerDec
  def export_insertResource( self, resourceName, resourceType, serviceType, 
                             siteName, gridSiteName ):
    return db

  types_updateResource = __res_IU
  @HandlerDec
  def export_updateResource( self, resourceName, resourceType, serviceType, 
                             siteName, gridSiteName ):
    return db

  types_getResource = __res_GD
  @HandlerDec      
  def export_getResource( self, resourceName, resourceType, serviceType, 
                          siteName, gridSiteName, kwargs ):
    return db  
  
  types_deleteResource = __res_GD
  @HandlerDec      
  def export_deleteResource( self, resourceName, resourceType, serviceType, 
                             siteName, gridSiteName, kwargs ):
    return db  

  '''
  ##############################################################################
  # RESOURCE STATUS FUNCTIONS
  ##############################################################################
  '''  
  __resStatus_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                     str, datetime ]
  __resStatus_IU = [ ( t, list, NoneType ) for t in __resStatus_IU ] + [ dict ]
  
  types_insertResourceStatus = __resStatus_IU
  @HandlerDec    
  def export_insertResourceStatus( self, resourceName, statusType, status, reason, 
                                   dateCreated, dateEffective, dateEnd, 
                                   lastCheckTime, tokenOwner, tokenExpiration ):
    return db

  types_updateResourceStatus = __resStatus_IU
  @HandlerDec    
  def export_updateResourceStatus( self, resourceName, statusType, status, reason, 
                                   dateCreated, dateEffective, dateEnd, 
                                   lastCheckTime, tokenOwner, tokenExpiration ):
    return db
    
  types_getResourceStatus = __resStatus_IU  
  @HandlerDec    
  def export_getResourceStatus( self, resourceName, statusType, status, reason, 
                                dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                tokenOwner, tokenExpiration, kwargs ):
    return db 

  types_deleteResourceStatus = __resStatus_IU  
  @HandlerDec    
  def export_deleteResourceStatus( self, resourceName, statusType, status, reason, 
                                   dateCreated, dateEffective, dateEnd, 
                                   lastCheckTime, tokenOwner, tokenExpiration, 
                                   kwargs ):
    return db 

  '''
  ##############################################################################
  # RESOURCE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  
  __resScheduled_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                        str, datetime ]
  __resScheduled_GD = [ ( t, list, NoneType ) for t in __resScheduled_IU ] + [ dict ] 
   
  types_insertResourceScheduledStatus = __resScheduled_IU
  @HandlerDec     
  def export_insertResourceScheduledStatus( self, resourceName, statusType, status, 
                                            reason, dateCreated, dateEffective, 
                                            dateEnd, lastCheckTime, tokenOwner, 
                                            tokenExpiration ):
    return db

  types_updateResourceScheduledStatus = __resScheduled_IU
  @HandlerDec     
  def export_updateResourceScheduledStatus( self, resourceName, statusType, status, 
                                            reason, dateCreated, dateEffective, 
                                            dateEnd, lastCheckTime, tokenOwner, 
                                            tokenExpiration ):
    return db

  types_getResourceScheduledStatus = __resScheduled_GD       
  @HandlerDec      
  def export_getResourceScheduledStatus( self, resourceName, statusType, status,
                                         reason, dateCreated, dateEffective, 
                                         dateEnd, lastCheckTime, tokenOwner, 
                                         tokenExpiration, kwargs ): 
    return db

  types_deleteResourceScheduledStatus = __resScheduled_GD       
  @HandlerDec      
  def export_deleteResourceScheduledStatus( self, resourceName, statusType, status,
                                           reason, dateCreated, dateEffective, 
                                           dateEnd, lastCheckTime, tokenOwner, 
                                           tokenExpiration, kwargs ): 
    return db

  '''
  ##############################################################################
  # RESOURCE HISTORY FUNCTIONS
  ##############################################################################
  ''' 
  __resHistory_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                      str, datetime ]
  __resHistory_GD = [ ( t, list, NoneType ) for t in __resHistory_IU ] + [ dict ]   

  types_insertResourceHistory = __resHistory_IU    
  @HandlerDec                
  def export_insertResourceHistory( self, resourceName, statusType, status, reason, 
                                    dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                    tokenOwner, tokenExpiration, kwargs ):
    return db
  
  types_updateResourceHistory = __resHistory_IU    
  @HandlerDec                
  def export_updateResourceHistory( self, resourceName, statusType, status, reason, 
                                    dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                    tokenOwner, tokenExpiration, kwargs ):
    return db  
      
  types_getResourceHistory = __resHistory_GD    
  @HandlerDec                
  def export_getResourceHistory( self, resourceName, statusType, status, reason, 
                                 dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                 tokenOwner, tokenExpiration, kwargs ):
    return db

  types_deleteResourceHistory = __resHistory_GD    
  @HandlerDec                
  def export_deleteResourceHistory( self, resourceName, statusType, status, reason, 
                                    dateCreated, dateEffective, dateEnd, 
                                    lastCheckTime, tokenOwner, tokenExpiration, 
                                    kwargs ):
    return db
      
  '''
  ##############################################################################
  # RESOURCE PRESENT FUNCTIONS
  ##############################################################################
  ''' 
  __resourcePresent   = [ str, str, str, str, str, str, datetime, str, datetime, 
                         str, datetime, str,]
  __resourcePresent_G = [ ( t, list, NoneType) for t in __resourcePresent ] + [ dict ]       
      
  types_getResourcePresent = __resourcePresent_G    
  @HandlerDec    
  def export_getResourcePresent( self, resourceName, siteName, serviceType, 
                                 gridSiteName, siteType, resourceType, statusType, 
                                 status, dateEffective, reason, lastCheckTime, 
                                 tokenOwner, tokenExpiration, formerStatus, kwargs ):
    return db
  
################################################################################
################################################################################

  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''
  __stEl_IU = [ str, str, str ]
  __stEl_GD = [ ( t, list, NoneType ) for t in __stEl_IU ] + [ dict ]

  types_insertStorageElement = __stEl_IU 
  @HandlerDec 
  def export_insertStorageElement( self, storageElementName, resourceName, 
                                   gridSiteName ):
    return db

  types_updateStorageElement = __stEl_IU 
  @HandlerDec 
  def export_updateStorageElement( self, storageElementName, resourceName, 
                                   gridSiteName ):
    return db

  types_getStorageElement = __stEl_GD            
  @HandlerDec           
  def export_getStorageElement( self, storageElementName, resourceName, 
                                gridSiteName, kwargs ):
    return db

  types_deleteStorageElement = __stEl_GD            
  @HandlerDec           
  def export_deleteStorageElement( self, storageElementName, resourceName, 
                                gridSiteName, kwargs ):
    return db

  '''
  ##############################################################################
  # STORAGE ELEMENT STATUS FUNCTIONS
  ##############################################################################
  '''
  __stElStatus_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                      str, datetime ]
  __stElStatus_GD = [ ( t, list, NoneType) for t in __stElStatus_IU ] + [ dict ]

  types_insertStorageElementStatus = __stElStatus_IU
  @HandlerDec                  
  def export_insertStorageElementStatus( self, storageElementName, statusType, 
                                         status, reason, dateCreated, dateEffective, 
                                         dateEnd, lastCheckTime, tokenOwner, 
                                         tokenExpiration ):
    return db

  types_updateStorageElementStatus = __stElStatus_IU
  @HandlerDec                  
  def export_updateStorageElementStatus( self, storageElementName, statusType, 
                                         status, reason, dateCreated, dateEffective, 
                                         dateEnd, lastCheckTime, tokenOwner, 
                                         tokenExpiration ):
    return db

  types_getStorageElementStatus = __stElStatus_GD
  @HandlerDec                  
  def export_getStorageElementStatus( self, storageElementName, statusType, 
                                      status, reason, dateCreated, dateEffective, 
                                      dateEnd, lastCheckTime, tokenOwner, 
                                      tokenExpiration, kwargs ):
    return db

  types_deleteStorageElementStatus = __stElStatus_GD
  @HandlerDec                  
  def export_deleteStorageElementStatus( self, storageElementName, statusType, 
                                         status, reason, dateCreated, dateEffective, 
                                         dateEnd, lastCheckTime, tokenOwner, 
                                         tokenExpiration, kwargs ):
    return db

  '''
  ##############################################################################
  # STORAGE ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''

  __stElScheduled_IU = [ str, str, str, str, datetime, datetime, datetime, 
                         datetime, str, datetime ]
  __stElScheduled_GD = [ ( t, list, NoneType) for t in __stElScheduled_IU ] + [ dict ]

  types_insertStorageElementScheduledStatus = __stElScheduled_IU          
  @HandlerDec                             
  def export_insertStorageElementScheduledStatus( self, storageElementName, statusType, 
                                                  status, reason, dateCreated, 
                                                  dateEffective, dateEnd, lastCheckTime, 
                                                  tokenOwner, tokenExpiration ):
    return db

  types_updateStorageElementScheduledStatus = __stElScheduled_IU          
  @HandlerDec                             
  def export_updateStorageElementScheduledStatus( self, storageElementName, statusType, 
                                                  status, reason, dateCreated, 
                                                  dateEffective, dateEnd, lastCheckTime, 
                                                  tokenOwner, tokenExpiration ):
    return db

  types_getStorageElementScheduledStatus = __stElScheduled_GD          
  @HandlerDec                             
  def export_getStorageElementScheduledStatus( self, storageElementName, statusType, 
                                               status, reason, dateCreated, 
                                               dateEffective, dateEnd, lastCheckTime, 
                                               tokenOwner, tokenExpiration, kwargs ):
    return db

  types_deleteStorageElementScheduledStatus = __stElScheduled_GD          
  @HandlerDec                             
  def export_deleteStorageElementScheduledStatus( self, storageElementName, statusType, 
                                                  status, reason, dateCreated, 
                                                  dateEffective, dateEnd, lastCheckTime, 
                                                  tokenOwner, tokenExpiration, kwargs ):
    return db

  '''
  ##############################################################################
  # STORAGE ELEMENT HISTORY FUNCTIONS
  ##############################################################################
  '''
  __stElHistory_IU = [ str, str, str, str, datetime, datetime, datetime, datetime, 
                       str, datetime ]
  __stElHistory_GD = [ ( t, list, NoneType) for t in __stElHistory_IU ] + [ dict ]

  types_insertStorageElementHistory = __stElHistory_IU        
  @HandlerDec             
  def export_insertStorageElementHistory( self, storageElementName, statusType, 
                                          status, reason, dateCreated, dateEffective, 
                                          dateEnd, lastCheckTime, tokenOwner, 
                                          tokenExpiration ):
    return db

  types_updateStorageElementHistory = __stElHistory_IU        
  @HandlerDec             
  def export_updateStorageElementHistory( self, storageElementName, statusType, 
                                          status, reason, dateCreated, dateEffective, 
                                          dateEnd, lastCheckTime, tokenOwner, 
                                          tokenExpiration ):
    return db

  types_getStorageElementHistory = __stElHistory_GD        
  @HandlerDec             
  def export_getStorageElementHistory( self, storageElementName, statusType, 
                                       status, reason, dateCreated, dateEffective, 
                                       dateEnd, lastCheckTime, tokenOwner, 
                                       tokenExpiration, kwargs ):
    return db
    
  types_deleteStorageElementHistory = __stElHistory_GD        
  @HandlerDec             
  def export_deleteStorageElementHistory( self, storageElementName, statusType, 
                                          status, reason, dateCreated, dateEffective, 
                                          dateEnd, lastCheckTime, tokenOwner, 
                                          tokenExpiration, kwargs ):
    return db

  '''
  ##############################################################################
  # STORAGE ELEMENT PRESENT FUNCTIONS
  ##############################################################################
  '''
  __stElPresent   = [ str, str, str, str, str, str, datetime, str, datetime, 
                         str, datetime, str,]
  __stElPresent_G = [ ( t, list, NoneType) for t in __stElPresent ] + [ dict ]  

  types_getStorageElementPresent = __stElPresent_G
  def export_getStorageElementPresent( self, storageElementName, resourceName, 
                                       gridSiteName, siteType, statusType, 
                                       status, dateEffective, reason, 
                                       lastCheckTime, tokenOwner,tokenExpiration, 
                                       formerStatus, kwargs ):
    return db

################################################################################
################################################################################
  
  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''  
  __gs_IU = [ str, str ]
  __gs_GD = [ ( t, list, NoneType ) for t in __gs_IU ] + [ dict ]
  
  types_insertGridSite = __gs_IU
  @HandlerDec
  def export_insertGridSite( self, gridSiteName, gridTier ):
    return db

  types_updateGridSite = __gs_IU
  @HandlerDec
  def export_updateGridSite( self, gridSiteName, gridTier ):
    return db
      
  types_getGridSites = __gs_GD    
  @HandlerDec    
  def export_getGridSites( self, gridSiteName, gridTier, kwargs ):
    return db

  types_deleteGridSites = __gs_GD    
  @HandlerDec  
  def export_deleteGridSites( self, gridSiteName, gridTier, kwargs ):         
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
#  Cleaning ongoing  
#  
################################################################################
#
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
#
#############################################################################
#
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
#
#############################################################################
#
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
#
#############################################################################
#
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
#
#############################################################################
#
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
#
#############################################################################
#
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
#      