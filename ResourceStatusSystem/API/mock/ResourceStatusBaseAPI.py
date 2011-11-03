from DIRAC.ResourceStatusSystem.Client.mock.ResourceStatusClient import ResourceStatusClient

from DIRAC import S_OK

from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientDec5

from DIRAC.ResourceStatusSystem                             import ValidRes,\
  ValidStatus, ValidStatusTypes, ValidSiteType, ValidServiceType, \
  ValidResourceType

class ResourceStatusBaseAPI( object ):
  
  def __init__( self ):
    self.client = ResourceStatusClient()
      
  '''
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''
  
  @ClientDec5
  def insertSite( self, siteName, siteType, gridSiteName, **kwargs ):
    pass
  @ClientDec5
  def updateSite( self, siteName, siteType, gridSiteName, **kwargs ):
    pass
  @ClientDec5
  def getSite( self, siteName = None, siteType = None, gridSiteName = None, 
               **kwargs ):
    pass
  @ClientDec5
  def deleteSite( self, siteName = None, siteType = None, gridSiteName = None, 
                  **kwargs ):
    pass      
  @ClientDec5
  def getSitePresent( self, siteName = None, siteType = None, 
                      gridSiteName = None, gridTier = None, statusType = None, 
                      status = None, dateEffective = None, reason = None, 
                      lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, formerStatus = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec5
  def insertService( self, serviceName, serviceType, siteName, **kwargs ):
    pass
  @ClientDec5
  def updateService( self, serviceName, serviceType, siteName, **kwargs ):
    pass
  @ClientDec5
  def getService( self, serviceName = None, serviceType = None, siteName = None, 
                  **kwargs ):
    pass
  @ClientDec5
  def deleteService( self, serviceName = None, serviceType = None, 
                     siteName = None, **kwargs ):
    pass
  @ClientDec5  
  def getServicePresent( self, serviceName = None, siteName = None, 
                         siteType = None, serviceType = None, statusType = None, 
                         status = None, dateEffective = None, reason = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, formerStatus = None, 
                         **kwargs ):
    pass

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec5
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):
    pass
  @ClientDec5
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):
    pass
  @ClientDec5
  def getResource( self, resourceName = None, resourceType = None, 
                   serviceType = None, siteName = None, gridSiteName = None, 
                   **kwargs ):
    pass
  @ClientDec5
  def deleteResource( self, resourceName = None, resourceType = None, 
                      serviceType = None, siteName = None, gridSiteName = None, 
                      **kwargs ):
    pass
  @ClientDec5      
  def getResourcePresent( self, resourceName = None, siteName = None, 
                          serviceType = None, gridSiteName = None, 
                          siteType = None, resourceType = None, 
                          statusType = None, status = None, 
                          dateEffective = None, reason = None, 
                          lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, formerStatus = None, 
                          **kwargs ):
    pass

  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec5
  def insertStorageElement( self, storageElementName, resourceName, 
                            gridSiteName, **kwargs ):
    pass
  @ClientDec5
  def updateStorageElement( self, storageElementName, resourceName, 
                            gridSiteName, **kwargs ):
    pass
  @ClientDec5       
  def getStorageElement( self, storageElementName = None, resourceName = None, 
                         gridSiteName = None, **kwargs ):
    pass
  @ClientDec5       
  def deleteStorageElement( self, storageElementName = None, 
                            resourceName = None, gridSiteName = None, 
                            **kwargs ):
    pass    
  @ClientDec5      
  def getStorageElementPresent( self, storageElementName = None, 
                                resourceName = None, gridSiteName = None, 
                                siteType = None, statusType = None, 
                                status = None, dateEffective = None, 
                                reason = None, lastCheckTime = None, 
                                tokenOwner = None, tokenExpiration = None, 
                                formerStatus = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec5
  def insertGridSite( self, gridSiteName, gridTier, **kwargs ):
    pass
  @ClientDec5
  def updateGridSite( self, gridSiteName, gridTier, **kwargs ):
    pass
  @ClientDec5    
  def getGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):
    pass
  @ClientDec5    
  def deleteGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):        
    pass

  '''
  ##############################################################################
  # ELEMENT STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec5
  def insertElementStatus( self, element, elementName, statusType, status, 
                           reason, dateCreated, dateEffective, dateEnd, 
                           lastCheckTime, tokenOwner, tokenExpiration, 
                           **kwargs ): 
    pass
  @ClientDec5
  def updateElementStatus( self, element, elementName, statusType, status, 
                           reason, dateCreated, dateEffective, dateEnd, 
                           lastCheckTime, tokenOwner, tokenExpiration, 
                           **kwargs ):
    pass
  @ClientDec5
  def getElementStatus( self, element, elementName = None, statusType = None, 
                        status = None, reason = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, 
                        tokenExpiration = None, **kwargs ):
    pass
  @ClientDec5
  def deleteElementStatus( self, element, elementName = None, statusType = None, 
                           status = None, reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, **kwargs ):
    pass

  '''
  ##############################################################################
  # ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientDec5
  def insertElementScheduledStatus( self, element, elementName, statusType, 
                                    status, reason, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, tokenOwner, 
                                    tokenExpiration, **kwargs ): 
    pass
  @ClientDec5
  def updateElementScheduledStatus( self, element, elementName, statusType, 
                                    status, reason, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, tokenOwner, 
                                    tokenExpiration, **kwargs ):
    pass
  @ClientDec5
  def getElementScheduledStatus( self, element, elementName = None, 
                                 statusType = None, status = None, 
                                 reason = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, tokenOwner = None, 
                                 tokenExpiration = None, **kwargs ):
    pass
  @ClientDec5
  def deleteElementScheduledStatus( self, element, elementName = None, 
                                    statusType = None, status = None, 
                                    reason = None, dateCreated = None,
                                    dateEffective = None, dateEnd = None, 
                                    lastCheckTime = None, tokenOwner = None, 
                                    tokenExpiration = None, **kwargs ):
    pass
      
  '''
  ##############################################################################
  # ELEMENT HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientDec5
  def insertElementHistory( self, element, elementName, statusType, status, 
                            reason, dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, 
                            **kwargs ): 
    pass
  @ClientDec5
  def updateElementHistory( self, element, elementName, statusType, status, 
                            reason, dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, 
                            **kwargs ):
    pass
  @ClientDec5
  def getElementHistory( self, element, elementName = None, statusType = None, 
                         status = None, reason = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    pass
  @ClientDec5
  def deleteElementHistory( self, element, elementName = None, 
                            statusType = None, status = None, reason = None, 
                            dateCreated = None, dateEffective = None, 
                            dateEnd = None, lastCheckTime = None, 
                            tokenOwner = None, tokenExpiration = None, 
                            **kwargs ):
    pass  

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