################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC                                                      import S_OK
from DIRAC.Core.DISET.RPCClient                                 import RPCClient

from DIRAC.ResourceStatusSystem                                 import ValidRes,\
  ValidStatus, ValidStatusTypes, ValidSiteType, ValidServiceType, ValidResourceType
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB             import ResourceStatusDB 
from DIRAC.ResourceStatusSystem.Utilities.Decorators            import ClientDec2
from DIRAC.ResourceStatusSystem.Utilities.ResourceStatusBooster import ResourceStatusBooster
       
class ResourceStatusClient:
  """
  The ResourceStatusClient class exposes the ResourceStatus API. All functions
  you need are on this client.
  
  It has the 'direct-db-access' functions, the ones of the type:
    o insert
    o update
    o get
    o delete 
  
  plus a set of functions of the type:
    o getValid 
    
  that return parts of the RSSConfiguration stored on the CS, and used everywhere
  on the RSS module. Finally, and probably more interesting, it exposes a set
  of functions, badly called 'boosters'. They are 'home made' functions using the
  basic database functions that are interesting enough to be exposed.  
  
  The client will ALWAYS try to connect to the DB, and in case of failure, to the
  XML-RPC server ( namely ResourceStatusDB and ResourceStatusHancler ).

  You can use this client on this way

   >>> from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import \
         ResourceStatusClient
   >>> rsClient = ResourceStatusClient()
   
  If you want to know more about ResourceStatusClient, scroll down to the end of
  the file.  
  """

  def __init__( self , serviceIn = None ):
 
    if serviceIn == None:
      try:
        self.gate = ResourceStatusDB()
      except Exception:
        self.gate = RPCClient( "ResourceStatus/ResourceStatus" )
    else:
      self.gate = serviceIn
      
    self.booster = ResourceStatusBooster( self )  

################################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertSite( self, siteName, siteType, gridSiteName ):
    pass
  @ClientDec2
  def updateSite( self, siteName, siteType, gridSiteName ):
    pass
  @ClientDec2
  def getSite( self, siteName = None, siteType = None, gridSiteName = None, 
               **kwargs ):
    pass
  @ClientDec2
  def deleteSite( self, siteName = None, siteType = None, gridSiteName = None, 
                  **kwargs ):
    pass      

  '''
  ##############################################################################
  # SITE STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                        tokenExpiration ):
    pass
  @ClientDec2
  def updateSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                        tokenExpiration ):
    pass
  @ClientDec2
  def getSiteStatus( self, siteName = None, statusType = None, status = None, 
                     reason = None, dateCreated = None, dateEffective = None, 
                     dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                     tokenExpiration = None, **kwargs ):
    pass
  @ClientDec2
  def deleteSiteStatus( self, siteName = None, statusType = None, status = None, 
                        reason = None, dateCreated = None, dateEffective = None, 
                        dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                        tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SITE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertSiteScheduledStatus( self, siteName, statusType, status, reason, 
                                 dateCreated, dateEffective, dateEnd, 
                                 lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def updateSiteScheduledStatus( self, siteName, statusType, status, reason, 
                                 dateCreated, dateEffective, dateEnd, 
                                 lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def getSiteScheduledStatus( self, siteName = None, statusType = None, 
                              status = None, reason = None, dateCreated = None, 
                              dateEffective = None, dateEnd = None, 
                              lastCheckTime = None, tokenOwner = None,
                              tokenExpiration = None, **kwargs ):
    pass        
  @ClientDec2
  def deleteSiteScheduledStatus( self, siteName = None, statusType = None, 
                                 status = None, reason = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, tokenOwner = None,
                                 tokenExpiration = None, **kwargs ):
    pass        

  '''
  ##############################################################################
  # SITE HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration ):
    pass
  @ClientDec2
  def updateSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration ):
    pass
  @ClientDec2
  def getSiteHistory( self, siteName = None, statusType = None, status = None, 
                      reason = None, dateCreated = None, dateEffective = None, 
                      dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, **kwargs ):
    pass        
  @ClientDec2
  def deleteSiteHistory( self, siteName = None, statusType = None, status = None, 
                         reason = None, dateCreated = None, dateEffective = None, 
                         dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass        

  '''
  ##############################################################################
  # SITE PRESENT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def getSitePresent( self, siteName = None, siteType = None, gridSiteName = None,
                       gridTier = None, statusType = None, status = None, 
                       dateEffective = None, reason = None, lastCheckTime = None, 
                       tokenOwner = None, tokenExpiration = None, 
                       formerStatus = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertService( self, serviceName, serviceType, siteName ):
    pass
  @ClientDec2
  def updateService( self, serviceName, serviceType, siteName ):
    pass
  @ClientDec2
  def getService( self, serviceName = None, serviceType = None, siteName = None, 
                  **kwargs ):
    pass
  @ClientDec2
  def deleteService( self, serviceName = None, serviceType = None, 
                     siteName = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertServiceStatus( self, serviceName, statusType, status, reason, 
                           dateCreated, dateEffective, dateEnd, lastCheckTime,
                           tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def updateServiceStatus( self, serviceName, statusType, status, reason, 
                           dateCreated, dateEffective, dateEnd, lastCheckTime,
                           tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def getServiceStatus( self, serviceName = None, statusType = None, 
                        status = None, reason = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None,
                        tokenExpiration = None, **kwargs ):
    pass
  @ClientDec2
  def deleteServiceStatus( self, serviceName = None, statusType = None, 
                           status = None, reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertServiceScheduledStatus( self, serviceName, statusType, status, 
                                    reason, dateCreated, dateEffective, dateEnd, 
                                    lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def updateServiceScheduledStatus( self, serviceName, statusType, status, 
                                    reason, dateCreated, dateEffective, dateEnd, 
                                    lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def getServiceScheduledStatus( self, serviceName = None, statusType = None, 
                                 status = None, reason = None, 
                                 dateCreated = None, dateEffective = None, 
                                 dateEnd = None, lastCheckTime = None, 
                                 tokenOwner = None,tokenExpiration = None, 
                                 **kwargs ):
    pass
  @ClientDec2
  def deleteServiceScheduledStatus( self, serviceName = None, statusType = None, 
                                    status = None, reason = None, 
                                    dateCreated = None, dateEffective = None, 
                                    dateEnd = None, lastCheckTime = None, 
                                    tokenOwner = None, tokenExpiration = None, 
                                    **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertServiceHistory( self, serviceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime,
                            tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def updateServiceHistory( self, serviceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime,
                            tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def getServiceHistory( self, serviceName = None, statusType = None, 
                         status = None, reason = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  @ClientDec2
  def deleteServiceHistory( self, serviceName = None, statusType = None, 
                            status = None, reason = None, dateCreated = None, 
                            dateEffective = None, dateEnd = None, 
                            lastCheckTime = None, tokenOwner = None, 
                            tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE PRESENT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2  
  def getServicePresent( self, serviceName = None, siteName = None, 
                         siteType = None, serviceType = None, statusType = None, 
                         status = None, dateEffective = None, reason = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, formerStatus = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName ):
    pass
  @ClientDec2
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName ):
    pass
  @ClientDec2
  def getResource( self, resourceName = None, resourceType = None, 
                   serviceType = None, siteName = None, gridSiteName = None, 
                   **kwargs ):
    pass
  @ClientDec2
  def deleteResource( self, resourceName = None, resourceType = None, 
                      serviceType = None, siteName = None, gridSiteName = None, 
                      **kwargs ):
    pass

  '''
  ##############################################################################
  # RESOURCE STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration ):
    pass
  @ClientDec2
  def updateResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration ):
    pass
  @ClientDec2      
  def getResourceStatus( self, resourceName = None, statusType = None, 
                         status = None, reason = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, 
                         lastCheckTime = None,tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  @ClientDec2      
  def deleteResourceStatus( self, resourceName = None, statusType = None, 
                            status = None, reason = None, dateCreated = None, 
                            dateEffective = None, dateEnd = None, 
                            lastCheckTime = None,tokenOwner = None, 
                            tokenExpiration = None, **kwargs ):
    pass  

  '''
  ##############################################################################
  # RESOURCE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertResourceScheduledStatus( self, resourceName, statusType, status, 
                                     reason, dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def updateResourceScheduledStatus( self, resourceName, statusType, status, 
                                     reason, dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec2      
  def getResourceScheduledStatus( self, resourceName = None, statusType = None, 
                                  status = None, reason = None, 
                                  dateCreated = None, dateEffective = None, 
                                  dateEnd = None, lastCheckTime = None,
                                  tokenOwner = None, tokenExpiration = None, 
                                  **kwargs ):
    pass
  @ClientDec2      
  def deleteResourceScheduledStatus( self, resourceName = None, 
                                     statusType = None, status = None, 
                                     reason = None, dateCreated = None, 
                                     dateEffective = None, dateEnd = None, 
                                     lastCheckTime = None,tokenOwner = None, 
                                     tokenExpiration = None, **kwargs ):
    pass  

  '''
  ##############################################################################
  # RESOURCE HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertResourceHistory( self, resourceName, statusType, status, reason, 
                             dateCreated, dateEffective, dateEnd, lastCheckTime, 
                             tokenOwner,tokenExpiration ):
    pass
  @ClientDec2
  def updateResourceHistory( self, resourceName, statusType, status, reason, 
                             dateCreated, dateEffective, dateEnd, lastCheckTime, 
                             tokenOwner,tokenExpiration ):
    pass
  @ClientDec2      
  def getResourceHistory( self, resourceName = None, statusType = None,
                          status = None, reason = None, dateCreated = None, 
                          dateEffective = None, dateEnd = None, 
                          lastCheckTime = None,tokenOwner = None, 
                          tokenExpiration = None, **kwargs ):
    pass
  @ClientDec2      
  def deleteResourceHistory( self, resourceName = None, statusType = None, 
                             status = None, reason = None, dateCreated = None, 
                             dateEffective = None, dateEnd = None, 
                             lastCheckTime = None,tokenOwner = None, 
                             tokenExpiration = None, **kwargs ):
    pass  

  '''
  ##############################################################################
  # RESOURCE PRESENT FUNCTIONS
  ##############################################################################
  '''  
  @ClientDec2      
  def getResourcePresent( self, resourceName = None, siteName = None, 
                          serviceType = None, gridSiteName = None, 
                          siteType = None, resourceType = None, statusType = None, 
                          status = None, dateEffective = None, reason = None, 
                          lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, formerStatus = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertStorageElement( self, storageElementName, resourceName, gridSiteName ):
    pass
  @ClientDec2
  def updateStorageElement( self, storageElementName, resourceName, gridSiteName ):
    pass
  @ClientDec2       
  def getStorageElement( self, storageElementName = None, resourceName = None, 
                         gridSiteName = None, **kwargs ):
    pass
  @ClientDec2       
  def deleteStorageElement( self, storageElementName = None, resourceName = None, 
                            gridSiteName = None, **kwargs ):
    pass    

  '''
  ##############################################################################
  # STORAGE ELEMENT STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def updateStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec2       
  def getStorageElementStatus( self, storageElementName = None, statusType = None, 
                               status = None, reason = None, dateCreated = None, 
                               dateEffective = None, dateEnd = None, 
                               lastCheckTime = None, tokenOwner = None, 
                               tokenExpiration = None, **kwargs ):
    pass    
  @ClientDec2       
  def deleteStorageElementStatus( self, storageElementName = None, 
                                  statusType = None, status = None, 
                                  reason = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, 
                                  lastCheckTime = None, tokenOwner = None, 
                                  tokenExpiration = None, **kwargs ):
    pass      

  '''
  ##############################################################################
  # STORAGE ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertStorageElementScheduledStatus( self, storageElementName, statusType, 
                                           status, reason, dateCreated, 
                                           dateEffective, dateEnd, lastCheckTime, 
                                           tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def updateStorageElementScheduledStatus( self, storageElementName, statusType, 
                                           status, reason, dateCreated, 
                                           dateEffective, dateEnd, lastCheckTime, 
                                           tokenOwner, tokenExpiration ):
    pass
  @ClientDec2       
  def getStorageElementScheduledStatus( self, storageElementName = None, 
                                        statusType = None, status = None, 
                                        reason = None, dateCreated = None, 
                                        dateEffective = None, dateEnd = None, 
                                        lastCheckTime = None, tokenOwner = None, 
                                        tokenExpiration = None, **kwargs ):
    pass    
  @ClientDec2       
  def deleteStorageElementScheduledStatus( self, storageElementName = None, 
                                           statusType = None, status = None, 
                                           reason = None, dateCreated = None, 
                                           dateEffective = None, dateEnd = None, 
                                           lastCheckTime = None, tokenOwner = None, 
                                           tokenExpiration = None, **kwargs ):
    pass     

  '''
  ##############################################################################
  # STORAGE ELEMENT HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec2
  def updateStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec2       
  def getStorageElementHistory( self, storageElementName = None, statusType = None, 
                                status = None, reason = None, dateCreated = None, 
                                dateEffective = None, dateEnd = None, 
                                lastCheckTime = None, tokenOwner = None, 
                                tokenExpiration = None, **kwargs ):
    pass    
  @ClientDec2       
  def deleteStorageElementHistory( self, storageElementName = None, 
                                   statusType = None, status = None, 
                                   reason = None, dateCreated = None, 
                                   dateEffective = None, dateEnd = None, 
                                   lastCheckTime = None, tokenOwner = None, 
                                   tokenExpiration = None, **kwargs ):
    pass     

  '''
  ##############################################################################
  # STORAGE ELEMENT PRESENT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2      
  def getStorageElementPresent( self, storageElementName = None, 
                                resourceName = None, gridSiteName = None, 
                                siteType = None, statusType = None, 
                                status = None, dateEffective = None, 
                                reason = None, lastCheckTime = None, 
                                tokenOwner = None, tokenExpiration = None, 
                                formerStatus = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec2
  def insertGridSite( self, gridSiteName, gridTier ):
    pass
  @ClientDec2
  def updateGridSite( self, gridSiteName, gridTier ):
    pass
  @ClientDec2    
  def getGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):
    pass
  @ClientDec2    
  def deleteGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):        
    pass
    
# DB ###########################################################################
# BOOSTER ######################################################################

  '''
  ##############################################################################
  # DB specific Boosters
  ##############################################################################
  '''

  @ClientDec2
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    pass
  @ClientDec2
  def addOrModifyService( self, serviceName, serviceType, siteName ):
    pass
  @ClientDec2
  def addOrModifyResource( self, resourceName, resourceType, serviceType, 
                           siteName, gridSiteName ):
    pass
  @ClientDec2
  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    pass
  @ClientDec2
  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    pass      

  @ClientDec2
  def modifySiteStatus( self, siteName, statusType, status = None, reason = None, 
                        dateCreated = None, dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, 
                        tokenExpiration = None ):
    pass
  @ClientDec2
  def modifyServiceStatus( self, serviceName, statusType, status = None, 
                           reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None ):
    pass
  @ClientDec2
  def modifyResourceStatus( self, resourceName, statusType, status = None, 
                            reason = None, dateCreated = None, 
                            dateEffective = None, dateEnd = None, 
                            lastCheckTime = None, tokenOwner = None, 
                            tokenExpiration = None ):
    pass
  @ClientDec2
  def modifyStorageElementStatus( self, storageElementName, statusType, 
                                  status = None, reason = None, 
                                  dateCreated = None, dateEffective = None, 
                                  dateEnd = None, lastCheckTime = None, 
                                  tokenOwner = None, tokenExpiration = None ):
    pass
      
  @ClientDec2
  def removeSite( self, siteName ):
    pass
  @ClientDec2
  def removeService( self, serviceName ):
    pass
  @ClientDec2
  def removeResource( self, resourceName ):
    pass
  @ClientDec2
  def removeStorageElement( self, storageElementName ):
    pass
  
  '''
  ##############################################################################
  # Stats functions
  ##############################################################################
  '''
  @ClientDec2
  def getServiceStats( self, siteName, statusType = None ):
    pass
  @ClientDec2
  def getResourceStats( self, element, name, statusType = None ):
    pass
  @ClientDec2
  def getStorageElementStats( self, element, name, statusType = None ):
    pass
  
  '''
  ##############################################################################
  # Misc functions
  ##############################################################################
  '''

  @ClientDec2 
  def getGeneralName( self, from_element, name, to_element ):
    pass
    
  @ClientDec2     
  def getGridSiteName( self, granularity, name ):
    pass
    
  @ClientDec2     
  def getTokens( self, granularity, name = None, tokenExpiration = None, 
                 statusType = None, **kwargs ): 
    pass
   
  @ClientDec2    
  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):
    pass
    
  @ClientDec2     
  def setReason( self, granularity, name, statusType, reason ):     
    pass
  
  @ClientDec2     
  def setDateEnd( self, granularity, name, statusType, dateEnd ):     
    pass 
    
  @ClientDec2     
  def whatIs( self, name ):  
    pass
  
  @ClientDec2   
  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    pass    
  
  @ClientDec2      
  def getMonitoredStatus( self, granularity, name ):
    pass
    
  @ClientDec2  
  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, 
                              maxItems ):
    pass

# BOOSTER ######################################################################
# CS ###########################################################################

  '''
  ##############################################################################
  # CS VALID ELEMENTS
  ##############################################################################
  '''
  def getValidElements( self ):
    return S_OK( ValidRes )
  def getValidStatuses( self ):
    return S_OK( ValidStatus )
  def getValidStatusTypes( self ):  
    return S_OK( ValidStatusTypes )
  def getValidSiteTypes( self ):
    return S_OK( ValidSiteType )
  def getValidServiceTypes( self ):
    return S_OK( ValidServiceType ) 
  def getValidResourceTypes( self ):
    return S_OK( ValidResourceType )

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF