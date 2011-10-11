"""
ResourceStatusClient class is a client for requesting info from the ResourceStatusService.
"""
# it crashes epydoc
# __docformat__ = "restructuredtext en"

from DIRAC                                                      import S_OK#, S_ERROR
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem                                 import ValidRes, ValidStatus, \
  ValidStatusTypes, ValidSiteType, ValidServiceType, ValidResourceType

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB             import ResourceStatusDB 
from DIRAC.ResourceStatusSystem.Utilities.Decorators            import ClientExecution
from DIRAC.ResourceStatusSystem.Utilities.ResourceStatusBooster import ResourceStatusBooster
       
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
  def addOrModifySiteStatus( self, siteName, statusType, status, reason = None, 
                     dateCreated = None, dateEffective = None, dateEnd = None, 
                     lastCheckTime = None, tokenOwner = None, tokenExpiration = None ):
    pass

  @ClientExecution
  def addOrModifySiteScheduledStatus( self, siteName, statusType, status, reason = None, 
                              dateCreated = None, dateEffective = None, dateEnd = None, 
                              lastCheckTime = None, tokenOwner = None, tokenExpiration = None ):
    pass

  @ClientExecution
  def getSite( self, siteName = None, siteType = None, gridSiteName = None, **kwargs ):
    pass
  
  @ClientExecution
  def getSiteStatus( self, siteName = None, statusType = None, status = None, 
                      reason = None, dateCreated = None, dateEffective = None, 
                      dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution
  def getSiteHistory( self, siteName = None, statusType = None, status = None, 
                      reason = None, dateCreated = None, dateEffective = None, 
                      dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution
  def getSiteScheduledStatus( self, siteName = None, statusType = None, status = None, 
                               reason = None, dateCreated = None, dateEffective = None, 
                               dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                               tokenExpiration = None, **kwargs):
    pass
  
  @ClientExecution
  def getSitePresent( self, siteName = None, siteType = None, gridSiteName = None,
                       gridTier = None, statusType = None, status = None, dateEffective = None,
                       reason = None, lastCheckTime = None, tokenOwner = None,
                       tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
  
  @ClientExecution
  def deleteSite( self, siteName ):
    pass
  
  @ClientExecution
  def deleteSiteScheduledStatus( self, siteName = None, statusType = None, status = None, 
                                  reason = None, dateCreated = None, dateEffective = None, 
                                  dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                                  tokenExpiration = None, **kwargs):
    pass
  
  @ClientExecution
  def deleteSiteHistory( self, siteName = None, statusType = None, status = None, 
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
  def addOrModifyServiceStatus( self, serviceName, statusType, status, reason, 
                        dateCreated = None, dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, tokenExpiration = None ):
    pass
   
  @ClientExecution  
  def addOrModifyServiceScheduledStatus( self, serviceName, statusType, status, reason, 
                                 dateCreated = None, dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, tokenOwner = None, tokenExpiration = None  ):
    pass
    
  @ClientExecution
  def getService( self, serviceName = None, serviceType = None, siteName = None, 
                   **kwargs ):
    pass
  
  @ClientExecution  
  def getServiceStatus( self, serviceName = None, statusType = None, status = None, 
                         reason = None, dateCreated = None, dateEffective = None, 
                         dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution  
  def getServiceHistory( self, serviceName = None, statusType = None, status = None, 
                          reason = None, dateCreated = None, dateEffective = None, 
                          dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution  
  def getServiceScheduledStatus( self, serviceName = None, statusType = None, 
                                 status = None, reason = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, lastCheckTime = None, 
                                 tokenOwner = None, tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution  
  def getServicePresent( self, serviceName = None, siteName = None, siteType = None, 
                          serviceType = None, statusType = None, status = None, 
                          dateEffective = None, reason = None, lastCheckTime = None, 
                          tokenOwner = None, tokenExpiration = None, 
                          formerStatus = None, **kwargs ):
    pass
  
  @ClientExecution  
  def deleteService( self, serviceName ):
    pass
  
  @ClientExecution  
  def deleteServiceScheduledStatus( self, serviceName = None, statusType = None, 
                                     status = None, reason = None, dateCreated = None, 
                                     dateEffective = None, dateEnd = None, 
                                     lastCheckTime = None, tokenOwner = None, 
                                     tokenExpiration = None, **kwargs):
    pass
  
  @ClientExecution  
  def deleteServiceHistory( self, serviceName = None, statusType = None, status = None, 
                            reason = None, dateCreated = None, dateEffective = None, 
                            dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                            tokenExpiration = None, **kwargs ):                                              
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
  def addOrModifyResourceStatus( self, resourceName, statusType, status, reason,  dateCreated = None, 
                                 dateEffective = None, dateEnd = None, lastCheckTime = None,
                                 tokenOwner = None, tokenExpiration = None ):
    pass
  
  @ClientExecution      
  def addOrModifyResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                          dateCreated = None, dateEffective = None, dateEnd = None, 
                                          lastCheckTime = None, tokenOwner = None, 
                                          tokenExpiration = None ):
    pass
  
  @ClientExecution      
  def getResource( self, resourceName = None, resourceType = None, serviceType = None, 
                   siteName = None, gridSiteName = None, **kwargs ):
    pass
  
  @ClientExecution      
  def getResourceStatus( self, resourceName = None, statusType = None, status = None,
                         reason = None, dateCreated = None, dateEffective = None, 
                         dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution      
  def getResourceHistory( self, resourceName = None, statusType = None, status = None,
                          reason = None, dateCreated = None, dateEffective = None, 
                          dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                          tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution        
  def getResourceScheduledStatus( self, resourceName = None, statusType = None, 
                                  status = None, reason = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, lastCheckTime = None,
                                  tokenOwner = None, tokenExpiration = None, **kwargs ):
    pass
  
  @ClientExecution      
  def getResourcePresent( self, resourceName = None, siteName = None, serviceType = None,
                          gridSiteName = None, siteType = None, resourceType = None,
                          statusType = None, status = None, dateEffective = None, 
                          reason = None, lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
  
  @ClientExecution     
  def deleteResource( self, resourceName ):
    pass
  
  @ClientExecution      
  def deleteResourceScheduledStatus( self, resourceName = None, statusType = None, 
                                     status = None, reason = None, dateCreated = None, 
                                     dateEffective = None, dateEnd = None, 
                                     lastCheckTime = None,tokenOwner = None, 
                                     tokenExpiration = None, **kwargs):
    pass
  
  @ClientExecution      
  def deleteResourceHistory( self, resourceName = None, statusType = None, status = None, 
                             reason = None, dateCreated = None, dateEffective = None, 
                             dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                             tokenExpiration = None, **kwargs ):
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
  def addOrModifyStorageElementStatus( self, storageElementName, statusType, status, 
                                       reason, dateCreated = None, dateEffective = None, 
                                       dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                                       tokenExpiration = None ):
    pass
      
  @ClientExecution       
  def addOrModifyStorageElementScheduledStatus( self, storageElementName, statusType, status, 
                                                reason, dateCreated = None, dateEffective = None, 
                                                dateEnd = None, lastCheckTime = None, 
                                                tokenOwner = None, tokenExpiration = None ):
    pass
      
  @ClientExecution       
  def getStorageElement( self, storageElementName = None, resourceName = None, 
                         gridSiteName = None, **kwargs ):
    pass
      
  @ClientExecution       
  def getStorageElementStatus( self, storageElementName = None, statusType = None, 
                               status = None, reason = None, dateCreated = None, 
                               dateEffective = None, dateEnd = None, 
                               lastCheckTime = None, tokenOwner = None, 
                               tokenExpiration = None, **kwargs ):
    pass    
    
  @ClientExecution       
  def getStorageElementHistory( self, storageElementName = None, statusType = None, 
                                status = None, reason = None, tokenOwner = None, 
                                tokenExpiration = None, dateCreated = None, 
                                dateEffective = None, dateEnd = None, 
                                lastCheckTime = None, **kwargs ):
    pass
      
  @ClientExecution       
  def getStorageElementScheduledStatus( self, storageElementName = None, statusType = None, 
                                        status = None, reason = None, dateCreated = None, 
                                        dateEffective = None, dateEnd = None, 
                                        lastCheckTime = None, tokenOwner = None, 
                                        tokenExpiration = None, **kwargs ):
    pass
      
  @ClientExecution      
  def getStorageElementPresent( self, storageElementName = None, resourceName = None, 
                                gridSiteName = None, siteType = None, statusType = None, 
                                status = None, dateEffective = None, reason = None, 
                                lastCheckTime = None, tokenOwner = None,
                                tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
      
  @ClientExecution                                    
  def deleteStorageElement( self, storageElementName ):
    pass
      
  @ClientExecution  
  def deleteStorageElementScheduledStatus( self, storageElementName = None, statusType = None, 
                                           status = None, reason = None, dateCreated = None, 
                                           dateEffective = None, dateEnd = None, 
                                           lastCheckTime = None, tokenOwner = None, 
                                           tokenExpiration = None, **kwargs ):
    pass
      
  @ClientExecution
  def deleteStorageElementHistory( self, storageElementName = None, statusType = None, 
                                   status = None, reason = None, dateCreated = None, 
                                   dateEffective = None, dateEnd = None, 
                                   lastCheckTime = None, tokenOwner = None, 
                                   tokenExpiration = None, **kwargs ):          
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