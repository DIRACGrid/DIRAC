"""
ResourceStatusClient class is a client for requesting info from the ResourceStatusService.
"""
# it crashes epydoc
# __docformat__ = "restructuredtext en"

from DIRAC                                            import S_OK#, S_ERROR
from DIRAC.Core.DISET.RPCClient                       import RPCClient
from DIRAC.ResourceStatusSystem                       import ValidRes, ValidStatus, \
  ValidStatusTypes, ValidSiteType, ValidServiceType, ValidResourceType

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB   import ResourceStatusDB 
from DIRAC.ResourceStatusSystem.Utilities.ResourceStatusBooster import ResourceStatusBooster

from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientExecution
       
class ResourceStatusClient:

################################################################################

  def __init__( self , serviceIn = None ):
    """ Constructor of the ResourceStatusClient class
    """
 
    if serviceIn == None:
      try:
        self.gate = ResourceStatusDB()
      except Exception, x:
        self.gate = RPCClient( "ResourceStatus/ResourceStatus" )
        
    else:
      self.gate = serviceIn
      
    self.booster = ResourceStatusBooster( self )  

################################################################################

################################################################################
# Sites functions
################################################################################

################################################################################

  @ClientExecution
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    pass

  @ClientExecution
  def setSiteStatus( self, siteName, statusType, status, reason = None, 
                     dateCreated = None, dateEffective = None, dateEnd = None, 
                     lastCheckTime = None, tokenOwner = None, tokenExpiration = None ):
    pass

  @ClientExecution
  def setSiteScheduledStatus( self, siteName, statusType, status, reason = None, 
                              dateCreated = None, dateEffective = None, dateEnd = None, 
                              lastCheckTime = None, tokenOwner = None, tokenExpiration = None ):
    pass
  
  @ClientExecution
  def updateSiteStatus( self, siteName, statusType = None, status = None, reason = None, 
                        dateCreated = None, dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, tokenExpiration = None ):
    pass

  @ClientExecution
  def getSites( self, siteName = None, siteType = None, gridSiteName = None, **kwargs ):
    pass
  
  @ClientExecution
  def getSitesStatus( self, siteName = None, statusType = None, status = None, 
                      reason = None, dateCreated = None, dateEffective = None, 
                      dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution
  def getSitesHistory( self, siteName = None, statusType = None, status = None, 
                      reason = None, dateCreated = None, dateEffective = None, 
                      dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution
  def getSitesScheduledStatus( self, siteName = None, statusType = None, status = None, 
                               reason = None, dateCreated = None, dateEffective = None, 
                               dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                               tokenExpiration = None, **kwargs):
    pass
  
  @ClientExecution
  def getSitesPresent( self, siteName = None, siteType = None, gridSiteName = None,
                       gridTier = None, statusType = None, status = None, dateEffective = None,
                       reason = None, lastCheckTime = None, tokenOwner = None,
                       tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
  
  @ClientExecution
  def deleteSites( self, siteName ):
    pass
  
  @ClientExecution
  def deleteSitesScheduledStatus( self, siteName = None, statusType = None, status = None, 
                                  reason = None, dateCreated = None, dateEffective = None, 
                                  dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                                  tokenExpiration = None, **kwargs):
    pass
  
  @ClientExecution
  def deleteSitesHistory( self, siteName = None, statusType = None, status = None, 
                          reason = None, dateCreated = None, dateEffective = None, 
                          dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, **kwargs ):
    pass
  
################################################################################

################################################################################
# Services functions
################################################################################

################################################################################

  @ClientExecution
  def addOrModifyService( self, serviceName, serviceType, siteName ):
    pass
  
  @ClientExecution  
  def setServiceStatus( self, serviceName, statusType, status, reason, tokenOwner, 
                        tokenExpiration = None, dateCreated = None, dateEffective = None, 
                        dateEnd = None, lastCheckTime = None ):
    pass
   
  @ClientExecution  
  def setServiceScheduledStatus( self, serviceName, statusType, status, reason, 
                                 tokenOwner, tokenExpiration = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None ):
    pass
  
  @ClientExecution  
  def updateServiceStatus( self, serviceName, statusType = None, status = None, 
                           reason = None, tokenOwner = None, tokenExpiration = None, 
                           dateCreated = None, dateEffective = None, dateEnd = None, 
                           lastCheckTime = None ):
    pass
  
  @ClientExecution
  def getServices( self, serviceName = None, serviceType = None, siteName = None, 
                   **kwargs ):
    pass
  
  @ClientExecution  
  def getServicesStatus( self, serviceName = None, statusType = None, status = None, 
                         reason = None, tokenOwner = None, tokenExpiration = None, 
                         dateCreated = None, dateEffective = None, dateEnd = None, 
                         lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecution  
  def getServicesHistory( self, serviceName = None, statusType = None, status = None, 
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecution  
  def getServicesScheduledStatus( self, serviceName = None, statusType = None, 
                                 status = None, reason = None, tokenOwner = None, 
                                 tokenExpiration = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecution  
  def getServicesPresent( self, serviceName = None, siteName = None, siteType = None, 
                          serviceType = None, statusType = None, status = None, 
                          dateEffective = None, reason = None, lastCheckTime = None, 
                          tokenOwner = None, tokenExpiration = None, 
                          formerStatus = None, **kwargs ):
    pass
  
  @ClientExecution  
  def deleteServices( self, serviceName ):
    pass
  
  @ClientExecution  
  def deleteServicesScheduledStatus( self, serviceName = None, statusType = None, 
                                     status = None, reason = None, tokenOwner = None, 
                                     tokenExpiration = None, dateCreated = None, 
                                     dateEffective = None, dateEnd = None, 
                                     lastCheckTime = None):
    pass
  
  @ClientExecution  
  def deleteServicesHistory( self, serviceName = None, statusType = None, status = None, 
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None, **kwargs ):                                              
    pass
  
################################################################################

################################################################################
# Resources functions
################################################################################

################################################################################

  @ClientExecution  
  def addOrModifyResource( self, resourceName, resourceType, serviceType, siteName,
                           gridSiteName ):
    pass
  
  @ClientExecution      
  def setResourceStatus( self, resourceName, statusType, status, reason, tokenOwner, 
                         tokenExpiration = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, lastCheckTime = None ):
    pass
  
  @ClientExecution      
  def setResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                  tokenOwner, tokenExpiration = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, lastCheckTime = None ):
    pass
  
  @ClientExecution          
  def updateResourceStatus( self, resourceName, statusType = None, status = None, reason = None, 
                         tokenOwner = None, tokenExpiration = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, lastCheckTime = None ):
    pass
  
  @ClientExecution      
  def getResources( self, resourceName = None, resourceType = None, 
                    serviceType = None, siteName = None, gridSiteName = None, 
                    **kwargs ):
    pass
  
  @ClientExecution      
  def getResourcesStatus( self, resourceName = None, statusType = None, status = None,
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecution      
  def getResourcesHistory( self, resourceName = None, statusType = None, status = None,
                           reason = None, tokenOwner = None, tokenExpiration = None, 
                           dateCreated = None, dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecution        
  def getResourcesScheduledStatus( self, resourceName = None, statusType = None, status = None,
                                  reason = None, tokenOwner = None, tokenExpiration = None, 
                                  dateCreated = None, dateEffective = None, dateEnd = None, 
                                  lastCheckTime = None, **kwargs):
    pass
  
  @ClientExecution      
  def getResourcesPresent( self, resourceName = None, siteName = None, serviceType = None,
                           gridSiteName = None, siteType = None, resourceType = None,
                           statusType = None, status = None, dateEffective = None, 
                           reason = None, lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
  
  @ClientExecution     
  def deleteResources( self, resourceName ):
    pass
  
  @ClientExecution      
  def deleteResourcesScheduledStatus( self, resourceName = None, statusType = None, 
                                      status = None, reason = None, tokenOwner = None, 
                                      tokenExpiration = None, dateCreated = None, 
                                      dateEffective = None, dateEnd = None, 
                                      lastCheckTime = None):
    pass
  
  @ClientExecution      
  def deleteResourcesHistory( self, resourceName = None, statusType = None, status = None, 
                              reason = None, tokenOwner = None, tokenExpiration = None, 
                              dateCreated = None, dateEffective = None, dateEnd = None, 
                              lastCheckTime = None, **kwargs ):
    pass

################################################################################

################################################################################
# StorageElements functions
################################################################################

################################################################################

  @ClientExecution   
  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    pass
  
  @ClientExecution       
  def setStorageElementStatus( self, storageElementName, statusType, status, 
                               reason, tokenOwner, tokenExpiration = None, 
                               dateCreated = None, dateEffective = None, dateEnd = None, 
                               lastCheckTime = None ):
    pass
      
  @ClientExecution       
  def setStorageElementScheduledStatus( self, storageElementName, statusType, status, 
                                        reason, tokenOwner, tokenExpiration = None, 
                                        dateCreated = None, dateEffective = None, 
                                        dateEnd = None, lastCheckTime = None ):
    pass
      
  @ClientExecution       
  def updateStorageElementStatus( self, storageElementName, statusType = None, status = None, 
                                 reason = None , tokenOwner = None, tokenExpiration = None, 
                                 dateCreated = None, dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None ):
    pass
      
  @ClientExecution       
  def getStorageElements( self, storageElementName = None, resourceName = None, 
                          gridSiteName = None, **kwargs ):
    pass
      
  @ClientExecution       
  def getStorageElementsStatus( self, storageElementName = None, statusType = None, 
                                status = None, reason = None, tokenOwner = None, 
                                tokenExpiration = None, dateCreated = None, 
                                dateEffective = None, dateEnd = None, 
                                lastCheckTime = None, **kwargs ):
    pass    
    
  @ClientExecution       
  def getStorageElementsHistory( self, storageElementName = None, statusType = None, 
                                 status = None, reason = None, tokenOwner = None, 
                                 tokenExpiration = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, **kwargs ):
    pass
      
  @ClientExecution       
  def getStorageElementsScheduledStatus( self, storageElementName = None, statusType = None, 
                                         status = None, reason = None, tokenOwner = None, 
                                         tokenExpiration = None, dateCreated = None, 
                                         dateEffective = None, dateEnd = None, 
                                         lastCheckTime = None, **kwargs ):
    pass
      
  @ClientExecution      
  def getStorageElementsPresent( self, storageElementName = None, resourceName = None, 
                                 gridSiteName = None, siteType = None, statusType = None, 
                                 status = None, dateEffective = None, reason = None, 
                                 lastCheckTime = None, tokenOwner = None,
                                 tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
      
  @ClientExecution                                    
  def deleteStorageElements( self, storageElementName ):
    pass
      
  @ClientExecution  
  def deleteStorageElementsScheduledStatus( self, storageElementName = None, statusType = None, 
                                            status = None, reason = None, tokenOwner = None, 
                                            tokenExpiration = None, dateCreated = None, 
                                            dateEffective = None, dateEnd = None, 
                                            lastCheckTime = None ):
    pass
      
  @ClientExecution
  def deleteStorageElementsHistory( self, storageElementName = None, statusType = None, 
                                    status = None, reason = None, tokenOwner = None, 
                                    tokenExpiration = None, dateCreated = None, 
                                    dateEffective = None, dateEnd = None, 
                                    lastCheckTime = None, **kwargs ):          
    pass

################################################################################

################################################################################
# Stats functions
################################################################################

################################################################################
  
  @ClientExecution
  def getServiceStats( self, siteName, statusType = None ):
    pass
    
  @ClientExecution  
  def getResourceStats( self, element, name, statusType = None ):
    pass
    
  @ClientExecution  
  def getStorageElementStats( self, element, name, statusType = None ):      
    pass

################################################################################

################################################################################
# GridSites functions
################################################################################

################################################################################

  @ClientExecution
  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    pass
  
  @ClientExecution    
  def getGridSites( self, gridSiteName = None, gridTier = None, **kwargs ):
    pass

  @ClientExecution    
  def deleteGridSites( self, gridSiteName ):        
    pass

################################################################################

################################################################################
# BOOSTER functions
################################################################################

################################################################################

  @ClientExecution 
  def getGeneralName( self, from_element, name, to_element ):
    pass
    
  @ClientExecution     
  def getGridSiteName( self, granularity, name ):
    pass
    
  @ClientExecution     
  def getTokens( self, granularity, name = None, tokenExpiration = None, 
                 statusType = None, **kwargs ): 
    pass
   
  @ClientExecution    
  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):
    pass
    
  @ClientExecution     
  def setReason( self, granularity, name, statusType, reason ):     
    pass
  
  @ClientExecution     
  def setDateEnd( self, granularity, name, statusType, dateEnd ):     
    pass 
    
  @ClientExecution     
  def whatIs( self, name ):  
    pass
  
  @ClientExecution   
  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    pass    
  
  @ClientExecution      
  def getMonitoredStatus( self, granularity, name ):
    pass
    
  @ClientExecution  
  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, maxItems ):
    pass
              
################################################################################

################################################################################
# General config valid elements functions
################################################################################

################################################################################

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
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF