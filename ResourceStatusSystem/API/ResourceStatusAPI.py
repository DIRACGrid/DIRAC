from DIRAC.ResourceStatusSystem.API.private.ResourceStatusExtendedBaseAPI import ResourceStatusExtendedBaseAPI

from DIRAC.ResourceStatusSystem.Utilities.Decorators import APIDecorator

class ResourceStatusAPI( object ):
  '''  
  The :class:`ResourceStatusAPI` class exposes all methods needed by RSS to
  interact with the database. This includes methods that interact directly with
  the database, and methods that actually do some processing using the outputs
  of the first ones.
  
  The methods that `directly` ( though the client ) access the database follow
  this convention:
  
    - insert + <TableName>||ElementStatus||ElementScheduledStatus||ElementHistory
    - udpate + <TableName>||ElementStatus||ElementScheduledStatus||ElementHistory
    - get + <TableName>||ElementStatus||ElementScheduledStatus||ElementHistory
    - delete + <TableName>||ElementStatus||ElementScheduledStatus||ElementHistory
    
  If you want to use it, you can do it as follows:
  
   >>> from DIRAC.ResourceStatusSystem.API.ResourceStatusAPI import ResourceStatusAPI
   >>> rmAPI = ResourceStatusAPI()
   >>> rmAPI.getSite()
   
  All `direct database access` functions have the possibility of using keyword 
  arguments to tune the SQL queries.
  '''
  
  def __init__( self ):  
    self.eBaseAPI = ResourceStatusExtendedBaseAPI()
    
  '''  
  ##############################################################################    
  # BASE API METHODS
  ##############################################################################
  '''
  
  @APIDecorator
  def insertSite( self, siteName, siteType, gridSiteName, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def updateSite( self, siteName, siteType, gridSiteName, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getSite( self, siteName = None, siteType = None, gridSiteName = None, 
               **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deleteSite( self, siteName = None, siteType = None, gridSiteName = None, 
                  **kwargs ):
    '''
    '''
    pass      
  @APIDecorator
  def getSitePresent( self, siteName = None, siteType = None, gridSiteName = None,
                       gridTier = None, statusType = None, status = None, 
                       dateEffective = None, reason = None, lastCheckTime = None, 
                       tokenOwner = None, tokenExpiration = None, 
                       formerStatus = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def insertService( self, serviceName, serviceType, siteName, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def updateService( self, serviceName, serviceType, siteName, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getService( self, serviceName = None, serviceType = None, siteName = None, 
                  **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deleteService( self, serviceName = None, serviceType = None, 
                     siteName = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator  
  def getServicePresent( self, serviceName = None, siteName = None, 
                         siteType = None, serviceType = None, statusType = None, 
                         status = None, dateEffective = None, reason = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, formerStatus = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getResource( self, resourceName = None, resourceType = None, 
                   serviceType = None, siteName = None, gridSiteName = None, 
                   **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deleteResource( self, resourceName = None, resourceType = None, 
                      serviceType = None, siteName = None, gridSiteName = None, 
                      **kwargs ):
    '''
    '''
    pass
  @APIDecorator      
  def getResourcePresent( self, resourceName = None, siteName = None, 
                          serviceType = None, gridSiteName = None, 
                          siteType = None, resourceType = None, statusType = None, 
                          status = None, dateEffective = None, reason = None, 
                          lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, formerStatus = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def insertStorageElement( self, storageElementName, resourceName, gridSiteName,
                            **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def updateStorageElement( self, storageElementName, resourceName, gridSiteName,
                            **kwargs ):
    '''
    '''
    pass
  @APIDecorator       
  def getStorageElement( self, storageElementName = None, resourceName = None, 
                         gridSiteName = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator       
  def deleteStorageElement( self, storageElementName = None, resourceName = None, 
                            gridSiteName = None, **kwargs ):
    '''
    '''
    pass    
  @APIDecorator      
  def getStorageElementPresent( self, storageElementName = None, 
                                resourceName = None, gridSiteName = None, 
                                siteType = None, statusType = None, 
                                status = None, dateEffective = None, 
                                reason = None, lastCheckTime = None, 
                                tokenOwner = None, tokenExpiration = None, 
                                formerStatus = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def insertGridSite( self, gridSiteName, gridTier, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def updateGridSite( self, gridSiteName, gridTier, **kwargs ):
    '''
    '''
    pass
  @APIDecorator    
  def getGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator    
  def deleteGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):        
    '''
    '''
    pass
  @APIDecorator
  def insertElementStatus( self, element, elementName, statusType, status, reason, 
                           dateCreated, dateEffective, dateEnd, lastCheckTime, 
                           tokenOwner, tokenExpiration, **kwargs ): 
    '''
    '''
    pass
  @APIDecorator
  def updateElementStatus( self, element, elementName, statusType, status, reason, 
                           dateCreated, dateEffective, dateEnd, lastCheckTime, 
                           tokenOwner, tokenExpiration, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getElementStatus( self, element, elementName = None, statusType = None, 
                        status = None, reason = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, 
                        tokenExpiration = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deleteElementStatus( self, element, elementName = None, statusType = None, 
                           status = None, reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def insertElementScheduledStatus( self, element, elementName, statusType, 
                                    status, reason, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, tokenOwner, 
                                    tokenExpiration, **kwargs ): 
    '''
    '''
    pass
  @APIDecorator
  def updateElementScheduledStatus( self, element, elementName, statusType, 
                                    status, reason, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, tokenOwner, 
                                    tokenExpiration, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getElementScheduledStatus( self, element, elementName = None, 
                                 statusType = None, status = None, reason = None, 
                                 dateCreated = None, dateEffective = None, 
                                 dateEnd = None, lastCheckTime = None, 
                                 tokenOwner = None, tokenExpiration = None, 
                                 **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deleteElementScheduledStatus( self, element, elementName = None, 
                                    statusType = None, status = None, 
                                    reason = None, dateCreated = None,
                                    dateEffective = None, dateEnd = None, 
                                    lastCheckTime = None, tokenOwner = None, 
                                    tokenExpiration = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def insertElementHistory( self, element, elementName, statusType, status, 
                            reason, dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, 
                            **kwargs ): 
    '''
    '''
    pass
  @APIDecorator
  def updateElementHistory( self, element, elementName, statusType, status, 
                            reason, dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, 
                            **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getElementHistory( self, element, elementName = None, statusType = None, 
                         status = None, reason = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deleteElementHistory( self, element, elementName = None, statusType = None, 
                            status = None, reason = None, dateCreated = None,
                            dateEffective = None, dateEnd = None, 
                            lastCheckTime = None, tokenOwner = None,
                            tokenExpiration = None, **kwargs ):
    '''
    '''
    pass  
  @APIDecorator
  def getValidElements( self ):
    '''
    '''
    pass
  @APIDecorator
  def getValidStatuses( self ):
    '''
    '''
    pass
  @APIDecorator
  def getValidStatusTypes( self ):  
    '''
    '''
    pass
  @APIDecorator
  def getValidSiteTypes( self ):
    '''
    '''
    pass
  @APIDecorator
  def getValidServiceTypes( self ):
    '''
    '''
    pass 
  @APIDecorator
  def getValidResourceTypes( self ):
    '''
    '''
    pass

  '''
  ##############################################################################
  # EXTENDED BASE API METHODS
  ##############################################################################
  '''
  
  @APIDecorator
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    '''
    '''
    pass
  @APIDecorator
  def addOrModifyService( self, serviceName, serviceType, siteName ):
    '''
    '''
    pass
  @APIDecorator
  def addOrModifyResource( self, resourceName, resourceType, serviceType, 
                           siteName, gridSiteName ):
    '''
    '''
    pass
  @APIDecorator
  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    '''
    '''
    pass
  @APIDecorator
  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    '''
    '''
    pass
  @APIDecorator
  def modifyElementStatus( self, element, elementName, statusType, status = None, 
                           reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None,
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None ):
    '''
    '''
    pass
  @APIDecorator
  def removeElement( self, element, elementName ):
    '''
    '''
    pass
  @APIDecorator
  def getServiceStats( self, siteName, statusType = None ):
    '''
    '''
    pass
  @APIDecorator
  def getResourceStats( self, element, name, statusType = None ):
    '''
    '''
    pass
  @APIDecorator
  def getStorageElementStats( self, element, name, statusType = None ):
    '''
    '''
    pass
  @APIDecorator
  def getGeneralName( self, from_element, name, to_element ):
    '''
    '''
    pass
  @APIDecorator
  def getGridSiteName( self, granularity, name ):
    '''
    '''
    pass
  @APIDecorator
  def getTokens( self, granularity, name, tokenExpiration, statusType, 
                 **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):
    '''
    '''
    pass
  @APIDecorator
  def setReason( self, granularity, name, statusType, reason ):
    '''
    '''
    pass
  @APIDecorator
  def setDateEnd( self, granularity, name, statusType, dateEffective ):
    '''
    '''
    pass
  @APIDecorator
  def whatIs( self, name ):
    '''
    '''
    pass
  @APIDecorator
  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getMonitoredStatus( self, granularity, name ):
    '''
    '''
    pass
  @APIDecorator
  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, 
                              maxItems ):
    '''
    '''
    pass        
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                                                     