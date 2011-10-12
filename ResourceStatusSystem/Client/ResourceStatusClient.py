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
from DIRAC.ResourceStatusSystem.Utilities.Decorators            import Client
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
  @Client
  def insertSite( self, siteName, siteType, gridSiteName ):
    pass
  @Client
  def updateSite( self, siteName, siteType, gridSiteName ):
    pass
  @Client
  def getSite( self, siteName = None, siteType = None, gridSiteName = None, **kwargs ):
    pass
  @Client
  def deleteSite( self, siteName = None, siteType = None, gridSiteName = None, **kwargs ):
    pass      

  '''
  ##############################################################################
  # SITE STATUS FUNCTIONS
  ##############################################################################
  '''
  @Client
  def insertSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @Client
  def updateSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @Client
  def getSiteStatus( self, siteName = None, statusType = None, status = None, reason = None, 
                     dateCreated = None, dateEffective = None, dateEnd = None, 
                     lastCheckTime = None, tokenOwner = None, tokenExpiration = None, **kwargs ):
    pass
  @Client
  def deleteSiteStatus( self, siteName = None, statusType = None, status = None, reason = None, 
                        dateCreated = None, dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SITE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @Client
  def insertSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                 tokenExpiration ):
    pass
  @Client
  def updateSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                 tokenExpiration ):
    pass
  @Client
  def getSiteScheduledStatus( self, siteName = None, statusType = None, status = None, 
                              reason = None, dateCreated = None, dateEffective = None, 
                              dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                              tokenExpiration = None, **kwargs ):
    pass        
  @Client
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
  @Client
  def insertSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration ):
    pass
  @Client
  def updateSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration ):
    pass
  @Client
  def getSiteHistory( self, siteName = None, statusType = None, status = None, 
                      reason = None, dateCreated = None, dateEffective = None, 
                      dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, **kwargs ):
    pass        
  @Client
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
  @Client
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
  @Client
  def insertService( self, serviceName, serviceType, siteName ):
    pass
  @Client
  def updateService( self, serviceName, serviceType, siteName ):
    pass
  @Client
  def getService( self, serviceName = None, serviceType = None, siteName = None, **kwargs ):
    pass
  @Client
  def deleteService( self, serviceName = None, serviceType = None, siteName = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE STATUS FUNCTIONS
  ##############################################################################
  '''
  @Client
  def insertServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @Client
  def updateServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @Client
  def getServiceStatus( self, serviceName = None, statusType = None, status = None, 
                        reason = None, dateCreated = None, dateEffective = None, 
                        dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                        tokenExpiration = None, **kwargs ):
    pass
  @Client
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
  @Client
  def insertServiceScheduledStatus( self, serviceName, statusType, status, reason, dateCreated,
                                    dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @Client
  def updateServiceScheduledStatus( self, serviceName, statusType, status, reason, dateCreated,
                                    dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @Client
  def getServiceScheduledStatus( self, serviceName = None, statusType = None, status = None, 
                                 reason = None, dateCreated = None, dateEffective = None, 
                                 dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                                 tokenExpiration = None, **kwargs ):
    pass
  @Client
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
  @Client
  def insertServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @Client
  def updateServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime,tokenOwner, tokenExpiration ):
    pass
  @Client
  def getServiceHistory( self, serviceName = None, statusType = None, status = None, 
                         reason = None, dateCreated = None, dateEffective = None, 
                         dateEnd = None, lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  @Client
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
  @Client  
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
  @Client
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName ):
    pass
  @Client
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName ):
    pass
  @Client
  def getResource( self, resourceName = None, resourceType = None, serviceType = None, 
                   siteName = None, gridSiteName = None, **kwargs ):
    pass
  @Client
  def deleteResource( self, resourceName = None, resourceType = None, serviceType = None, 
                      siteName = None, gridSiteName = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # RESOURCE STATUS FUNCTIONS
  ##############################################################################
  '''
  @Client
  def insertResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration ):
    pass
  @Client
  def updateResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration ):
    pass
  @Client      
  def getResourceStatus( self, resourceName = None, statusType = None, status = None,
                         reason = None, dateCreated = None, dateEffective = None, 
                         dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  @Client      
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
  @Client
  def insertResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                     dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                     tokenOwner,tokenExpiration ):
    pass
  @Client
  def updateResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                     dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                     tokenOwner,tokenExpiration ):
    pass
  @Client      
  def getResourceScheduledStatus( self, resourceName = None, statusType = None, status = None,
                                  reason = None, dateCreated = None, dateEffective = None, 
                                  dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                                  tokenExpiration = None, **kwargs ):
    pass
  @Client      
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
  @Client
  def insertResourceHistory( self, resourceName, statusType, status, reason, 
                             dateCreated, dateEffective, dateEnd, lastCheckTime, 
                             tokenOwner,tokenExpiration ):
    pass
  @Client
  def updateResourceHistory( self, resourceName, statusType, status, reason, 
                             dateCreated, dateEffective, dateEnd, lastCheckTime, 
                             tokenOwner,tokenExpiration ):
    pass
  @Client      
  def getResourceHistory( self, resourceName = None, statusType = None, status = None,
                          reason = None, dateCreated = None, dateEffective = None, 
                          dateEnd = None, lastCheckTime = None,tokenOwner = None, 
                          tokenExpiration = None, **kwargs ):
    pass
  @Client      
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
  @Client      
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
  @Client
  def insertStorageElement( self, storageElementName, resourceName, gridSiteName ):
    pass
  @Client
  def updateStorageElement( self, storageElementName, resourceName, gridSiteName ):
    pass
  @Client       
  def getStorageElement( self, storageElementName = None, resourceName = None, 
                         gridSiteName = None, **kwargs ):
    pass
  @Client       
  def deleteStorageElement( self, storageElementName = None, resourceName = None, 
                            gridSiteName = None, **kwargs ):
    pass    

  '''
  ##############################################################################
  # STORAGE ELEMENT STATUS FUNCTIONS
  ##############################################################################
  '''
  @Client
  def insertStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @Client
  def updateStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @Client       
  def getStorageElementStatus( self, storageElementName = None, statusType = None, 
                               status = None, reason = None, dateCreated = None, 
                               dateEffective = None, dateEnd = None, 
                               lastCheckTime = None, tokenOwner = None, 
                               tokenExpiration = None, **kwargs ):
    pass    
  @Client       
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
  @Client
  def insertStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @Client
  def updateStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @Client       
  def getStorageElementScheduledStatus( self, storageElementName = None, statusType = None, 
                                        status = None, reason = None, dateCreated = None, 
                                        dateEffective = None, dateEnd = None, 
                                        lastCheckTime = None, tokenOwner = None, 
                                        tokenExpiration = None, **kwargs ):
    pass    
  @Client       
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
  @Client
  def insertStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @Client
  def updateStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration ):
    pass
  @Client       
  def getStorageElementHistory( self, storageElementName = None, statusType = None, 
                                status = None, reason = None, dateCreated = None, 
                                dateEffective = None, dateEnd = None, 
                                lastCheckTime = None, tokenOwner = None, 
                                tokenExpiration = None, **kwargs ):
    pass    
  @Client       
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
  @Client      
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
  @Client
  def insertGridSite( self, gridSiteName, gridTier ):
    pass
  @Client
  def updateGridSite( self, gridSiteName, gridTier ):
    pass
  @Client    
  def getGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):
    pass
  @Client    
  def deleteGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):        
    pass
    
################################################################################
################################################################################
# Stats functions
################################################################################
#  
#  @Client
#  def getServiceStats( self, siteName, statusType = None ):
#    pass
#    
#  @Client  
#  def getResourceStats( self, element, name, statusType = None ):
#    pass
#    
#  @Client  
#  def getStorageElementStats( self, element, name, statusType = None ):      
#    pass
#
################################################################################

# DB ###########################################################################
# BOOSTER ######################################################################

  '''
  ##############################################################################
  # DB specific Boosters
  ##############################################################################
  '''

  @Client
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    pass
  @Client
  def addOrModifyService( self, serviceName, serviceType, siteName ):
    pass
  @Client
  def addOrModifyResource( self, resourceName, resourceType, serviceType, siteName, gridSiteName ):
    pass
  @Client
  def addOrModifyStorageElement( self, storageElementName, resourceName, gridSiteName ):
    pass
  @Client
  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    pass      
  
  @Client
  def removeSite( self, siteName ):
    pass
  @Client
  def removeService( self, serviceName ):
    pass
  @Client
  def removeResource( self, resourceName ):
    pass
  @Client
  def removeStorageElement( self, storageElementName ):
    pass
  
  '''
  ##############################################################################
  # Misc functions
  ##############################################################################
  '''

  @Client 
  def getGeneralName( self, from_element, name, to_element ):
    pass
    
  @Client     
  def getGridSiteName( self, granularity, name ):
    pass
    
  @Client     
  def getTokens( self, granularity, name = None, tokenExpiration = None, 
                 statusType = None, **kwargs ): 
    pass
   
  @Client    
  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):
    pass
    
  @Client     
  def setReason( self, granularity, name, statusType, reason ):     
    pass
  
  @Client     
  def setDateEnd( self, granularity, name, statusType, dateEnd ):     
    pass 
    
  @Client     
  def whatIs( self, name ):  
    pass
  
  @Client   
  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    pass    
  
  @Client      
  def getMonitoredStatus( self, granularity, name ):
    pass
    
  @Client  
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