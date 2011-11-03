from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.API.mock.ResourceStatusBaseAPI import ResourceStatusBaseAPI

class ResourceStatusExtendedBaseAPI( ResourceStatusBaseAPI ):
  
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    return S_OK()
  def addOrModifyService( self, serviceName, serviceType, siteName ):
    return S_OK()
  def addOrModifyResource( self, resourceName, resourceType, serviceType, 
                           siteName, gridSiteName ):
    return S_OK()

  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    return S_OK()

  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    return S_OK() 

  def modifyElementStatus( self, element, elementName, statusType, 
                           status = None, reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None,
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None ):
    return S_OK()

  def removeElement( self, element, elementName ):
    return S_OK()

  def getServiceStats( self, siteName, statusType = None ):
    return S_OK()

  def getResourceStats( self, element, name, statusType = None ):
    return S_OK()
 
  def getStorageElementStats( self, element, name, statusType = None ):
    return S_OK()
  
  def getGeneralName( self, from_element, name, to_element ):
    return S_OK()

  def getGridSiteName( self, granularity, name ):
    return S_OK()

  def getTokens( self, granularity, name = None, tokenExpiration = None, 
                 statusType = None, **kwargs ):
    return S_OK()

  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):
    return S_OK()

  def setReason( self, granularity, name, statusType, reason ):
    return S_OK()

  def setDateEnd( self, granularity, name, statusType, dateEffective ):
    return S_OK()

  def whatIs( self, name ):
    return S_OK()

  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    return S_OK()
  def getMonitoredStatus( self, granularity, name ):
    return S_OK()

  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, 
                              maxItems ):
    return S_OK()

 