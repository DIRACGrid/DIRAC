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
from DIRAC.ResourceStatusSystem.Utilities.Decorators            import ClientDec
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
# DB ###########################################################################

  '''
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertSite( self, siteName, siteType, gridSiteName ):
    pass
  @ClientDec
  def updateSite( self, siteName, siteType, gridSiteName ):
    pass
  @ClientDec
  def getSite( self, siteName = None, siteType = None, gridSiteName = None, **kwargs ):
    pass
  @ClientDec
  def deleteSite( self, siteName = None, siteType = None, gridSiteName = None, **kwargs ):
    pass      

  '''
  ##############################################################################
  # SITE STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def updateSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def getSiteStatus( self, siteName = None, statusType = None, status = None, reason = None, 
                     dateCreated = None, dateEffective = None, dateEnd = None, 
                     lastCheckTime = None, tokenOwner = None, tokenExpiration = None, **kwargs ):
    pass
  @ClientDec
  def deleteSiteStatus( self, siteName = None, statusType = None, status = None, reason = None, 
                        dateCreated = None, dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SITE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                 tokenExpiration ):
    pass
  @ClientDec
  def updateSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                 tokenExpiration ):
    pass
  @ClientDec
  def getSiteScheduledStatus( self, siteName = None, statusType = None, status = None, 
                              reason = None, dateCreated = None, dateEffective = None, 
                              dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                              tokenExpiration = None, **kwargs ):
    pass        
  @ClientDec
  def deleteSiteScheduledStatus( self, siteName = None, statusType = None, status = None, 
                                 reason = None, dateCreated = None, dateEffective = None, 
                                 dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                                 tokenExpiration = None, **kwargs ):
    pass        

  '''
  ##############################################################################
  # SITE HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration ):
    pass
  @ClientDec
  def updateSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration ):
    pass
  @ClientDec
  def getSiteHistory( self, siteName = None, statusType = None, status = None, 
                      reason = None, dateCreated = None, dateEffective = None, 
                      dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, **kwargs ):
    pass        
  @ClientDec
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
  @ClientDec
  def getSitePresent( self, siteName = None, siteType = None, gridSiteName = None,
                       gridTier = None, statusType = None, status = None, dateEffective = None,
                       reason = None, lastCheckTime = None, tokenOwner = None,
                       tokenExpiration = None, formerStatus = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertService( self, serviceName, serviceType, siteName ):
    pass
  @ClientDec
  def updateService( self, serviceName, serviceType, siteName ):
    pass
  @ClientDec
  def getService( self, serviceName = None, serviceType = None, siteName = None, **kwargs ):
    pass
  @ClientDec
  def deleteService( self, serviceName = None, serviceType = None, siteName = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def updateServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def getServiceStatus( self, serviceName = None, statusType = None, status = None, 
                        reason = None, dateCreated = None, dateEffective = None, 
                        dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                        tokenExpiration = None, **kwargs ):
    pass
  @ClientDec
  def deleteServiceStatus( self, serviceName = None, statusType = None, status = None, 
                           reason = None, dateCreated = None, dateEffective = None, 
                           dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertServiceScheduledStatus( self, serviceName, statusType, status, reason, dateCreated,
                                    dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def updateServiceScheduledStatus( self, serviceName, statusType, status, reason, dateCreated,
                                    dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def getServiceScheduledStatus( self, serviceName = None, statusType = None, status = None, 
                                 reason = None, dateCreated = None, dateEffective = None, 
                                 dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                                 tokenExpiration = None, **kwargs ):
    pass
  @ClientDec
  def deleteServiceScheduledStatus( self, serviceName = None, statusType = None, status = None, 
                                    reason = None, dateCreated = None, dateEffective = None, 
                                    dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                                    tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def updateServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def getServiceHistory( self, serviceName = None, statusType = None, status = None, 
                         reason = None, dateCreated = None, dateEffective = None, 
                         dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  @ClientDec
  def deleteServiceHistory( self, serviceName = None, statusType = None, status = None, 
                            reason = None, dateCreated = None, dateEffective = None, 
                            dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                            tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE PRESENT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec  
  def getServicePresent( self, serviceName = None, siteName = None, siteType = None, 
                          serviceType = None, statusType = None, status = None, 
                          dateEffective = None, reason = None, lastCheckTime = None, 
                          tokenOwner = None, tokenExpiration = None, 
                          formerStatus = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName ):
    pass
  @ClientDec
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName ):
    pass
  @ClientDec
  def getResource( self, resourceName = None, resourceType = None, serviceType = None, 
                   siteName = None, gridSiteName = None, **kwargs ):
    pass
  @ClientDec
  def deleteResource( self, resourceName = None, resourceType = None, serviceType = None, 
                      siteName = None, gridSiteName = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # RESOURCE STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration ):
    pass
  @ClientDec
  def updateResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration ):
    pass
  @ClientDec      
  def getResourceStatus( self, resourceName = None, statusType = None, status = None,
                         reason = None, dateCreated = None, dateEffective = None, 
                         dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  @ClientDec      
  def deleteResourceStatus( self, resourceName = None, statusType = None, status = None,
                            reason = None, dateCreated = None, dateEffective = None, 
                            dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                            tokenExpiration = None, **kwargs ):
    pass  

  '''
  ##############################################################################
  # RESOURCE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                     dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                     tokenOwner,tokenExpiration ):
    pass
  @ClientDec
  def updateResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                     dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                     tokenOwner,tokenExpiration ):
    pass
  @ClientDec      
  def getResourceScheduledStatus( self, resourceName = None, statusType = None, status = None,
                                  reason = None, dateCreated = None, dateEffective = None, 
                                  dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                                  tokenExpiration = None, **kwargs ):
    pass
  @ClientDec      
  def deleteResourceScheduledStatus( self, resourceName = None, statusType = None, status = None,
                                     reason = None, dateCreated = None, dateEffective = None, 
                                     dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                                     tokenExpiration = None, **kwargs ):
    pass  

  '''
  ##############################################################################
  # RESOURCE HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertResourceHistory( self, resourceName, statusType, status, reason, 
                             dateCreated, dateEffective, dateEnd, lastCheckTime, 
                             tokenOwner,tokenExpiration ):
    pass
  @ClientDec
  def updateResourceHistory( self, resourceName, statusType, status, reason, 
                             dateCreated, dateEffective, dateEnd, lastCheckTime, 
                             tokenOwner,tokenExpiration ):
    pass
  @ClientDec      
  def getResourceHistory( self, resourceName = None, statusType = None, status = None,
                          reason = None, dateCreated = None, dateEffective = None, 
                          dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                          tokenExpiration = None, **kwargs ):
    pass
  @ClientDec      
  def deleteResourceHistory( self, resourceName = None, statusType = None, status = None,
                             reason = None, dateCreated = None, dateEffective = None, 
                             dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                             tokenExpiration = None, **kwargs ):
    pass  

  '''
  ##############################################################################
  # RESOURCE PRESENT FUNCTIONS
  ##############################################################################
  '''  
  @ClientDec      
  def getResourcePresent( self, resourceName = None, siteName = None, serviceType = None,
                          gridSiteName = None, siteType = None, resourceType = None,
                          statusType = None, status = None, dateEffective = None, 
                          reason = None, lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, formerStatus = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertStorageElement( self, storageElementName, resourceName, gridSiteName ):
    pass
  @ClientDec
  def updateStorageElement( self, storageElementName, resourceName, gridSiteName ):
    pass
  @ClientDec       
  def getStorageElement( self, storageElementName = None, resourceName = None, 
                         gridSiteName = None, **kwargs ):
    pass
  @ClientDec       
  def deleteStorageElement( self, storageElementName = None, resourceName = None, 
                            gridSiteName = None, **kwargs ):
    pass    

  '''
  ##############################################################################
  # STORAGE ELEMENT STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def updateStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec       
  def getStorageElementStatus( self, storageElementName = None, statusType = None, 
                               status = None, reason = None, dateCreated = None, 
                               dateEffective = None, dateEnd = None, 
                               lastCheckTime = None, tokenOwner = None, 
                               tokenExpiration = None, **kwargs ):
    pass    
  @ClientDec       
  def deleteStorageElementStatus( self, storageElementName = None, statusType = None, 
                                  status = None, reason = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, 
                                  lastCheckTime = None, tokenOwner = None, 
                                  tokenExpiration = None, **kwargs ):
    pass      

  '''
  ##############################################################################
  # STORAGE ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def updateStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec       
  def getStorageElementScheduledStatus( self, storageElementName = None, statusType = None, 
                                        status = None, reason = None, dateCreated = None, 
                                        dateEffective = None, dateEnd = None, 
                                        lastCheckTime = None, tokenOwner = None, 
                                        tokenExpiration = None, **kwargs ):
    pass    
  @ClientDec       
  def deleteStorageElementScheduledStatus( self, storageElementName = None, statusType = None, 
                                           status = None, reason = None, dateCreated = None, 
                                           dateEffective = None, dateEnd = None, 
                                           lastCheckTime = None, tokenOwner = None, 
                                           tokenExpiration = None, **kwargs ):
    pass     

  '''
  ##############################################################################
  # STORAGE ELEMENT HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec
  def updateStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @ClientDec       
  def getStorageElementHistory( self, storageElementName = None, statusType = None, 
                                status = None, reason = None, dateCreated = None, 
                                dateEffective = None, dateEnd = None, 
                                lastCheckTime = None, tokenOwner = None, 
                                tokenExpiration = None, **kwargs ):
    pass    
  @ClientDec       
  def deleteStorageElementHistory( self, storageElementName = None, statusType = None, 
                                   status = None, reason = None, dateCreated = None, 
                                   dateEffective = None, dateEnd = None, 
                                   lastCheckTime = None, tokenOwner = None, 
                                   tokenExpiration = None, **kwargs ):
    pass     

  '''
  ##############################################################################
  # STORAGE ELEMENT PRESENT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec      
  def getStorageElementPresent( self, storageElementName = None, resourceName = None, 
                                gridSiteName = None, siteType = None, statusType = None, 
                                status = None, dateEffective = None, reason = None, 
                                lastCheckTime = None, tokenOwner = None,
                                tokenExpiration = None, formerStatus = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertGridSite( self, gridSiteName, gridTier ):
    pass
  @ClientDec
  def updateGridSite( self, gridSiteName, gridTier ):
    pass
  @ClientDec    
  def getGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):
    pass
  @ClientDec    
  def deleteGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):        
    pass
    
# DB ###########################################################################
# BOOSTER ######################################################################

  '''
  ##############################################################################
  # DB specific Boosters
  ##############################################################################
  '''

  @ClientDec
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    pass
  @ClientDec
  def addOrModifyService( self, serviceName, serviceType, siteName ):
    pass
  @ClientDec
  def addOrModifyResource( self, resourceName, resourceType, serviceType, siteName, gridSiteName ):
    pass
  @ClientDec
  def addOrModifyStorageElement( self, storageElementName, resourceName, gridSiteName ):
    pass
  @ClientDec
  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    pass      
  
  @ClientDec
  def removeSite( self, siteName ):
    pass
  @ClientDec
  def removeService( self, serviceName ):
    pass
  @ClientDec
  def removeResource( self, resourceName ):
    pass
  @ClientDec
  def removeStorageElement( self, storageElementName ):
    pass
  
  '''
  ##############################################################################
  # Stats functions
  ##############################################################################
  '''
  @ClientDec
  def getServiceStats( self, siteName, statusType = None ):
    pass
  @ClientDec
  def getResourceStats( self, element, name, statusType = None ):
    pass
  @ClientDec
  def getStorageElementStats( self, element, name, statusType = None ):
    pass
  
  '''
  ##############################################################################
  # Misc functions
  ##############################################################################
  '''

  @ClientDec 
  def getGeneralName( self, from_element, name, to_element ):
    pass
    
  @ClientDec     
  def getGridSiteName( self, granularity, name ):
    pass
    
  @ClientDec     
  def getTokens( self, granularity, name = None, tokenExpiration = None, 
                 statusType = None, **kwargs ): 
    pass
   
  @ClientDec    
  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):
    pass
    
  @ClientDec     
  def setReason( self, granularity, name, statusType, reason ):     
    pass
  
  @ClientDec     
  def setDateEnd( self, granularity, name, statusType, dateEnd ):     
    pass 
    
  @ClientDec     
  def whatIs( self, name ):  
    pass
  
  @ClientDec   
  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    pass    
  
  @ClientDec      
  def getMonitoredStatus( self, granularity, name ):
    pass
    
  @ClientDec  
  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, maxItems ):
    pass
              
################################################################################

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
  
# CS ###########################################################################
################################################################################

## end of the API !!
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF