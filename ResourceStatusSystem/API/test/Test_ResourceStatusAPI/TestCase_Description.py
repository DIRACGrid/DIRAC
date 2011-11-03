import unittest
import inspect

class TestCase_Description( unittest.TestCase ):
  
  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.api.__init__ )
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )    
  
  def test_insertSite_definition( self ):
    
    ins = inspect.getargspec( self.api.insertSite.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  

  def test_updateSite_definition( self ):
    
    ins = inspect.getargspec( self.api.updateSite.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  
    
  def test_getSite_definition( self ):
    
    ins = inspect.getargspec( self.api.getSite.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )  

  def test_deleteSite_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteSite.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )  
    
  def test_getSitePresent_definition( self ):
    
    ins = inspect.getargspec( self.api.getSitePresent.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName','siteType','gridSiteName',
                                  'gridTier','statusType','status','dateEffective',
                                  'reason','lastCheckTime','tokenOwner',
                                  'tokenExpiration','formerStatus' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None,
                                      None, None, None, None, None ) )  

  def test_insertService_definition( self ):
    
    ins = inspect.getargspec( self.api.insertService.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  

  def test_updateService_definition( self ):
    
    ins = inspect.getargspec( self.api.updateService.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  

  def test_getService_definition( self ):
    
    ins = inspect.getargspec( self.api.getService.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )  

  def test_deleteService_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteService.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )  

  def test_getServicePresent_definition( self ):
    
    ins = inspect.getargspec( self.api.getServicePresent.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'siteName', 'siteType',
                                  'serviceType', 'statusType', 'status', 'dateEffective',
                                  'reason', 'lastCheckTime', 'tokenOwner', 'tokenExpiration',
                                  'formerStatus' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None, None, None, None, None ) )  

  def test_insertResource_definition( self ):
    
    ins = inspect.getargspec( self.api.insertResource.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
                                  'serviceType', 'siteName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  

  def test_updateResource_definition( self ):
    
    ins = inspect.getargspec( self.api.updateResource.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
                                  'serviceType', 'siteName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  

  def test_getResource_definition( self ):
    
    ins = inspect.getargspec( self.api.getResource.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
                                  'serviceType', 'siteName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None ) )

  def test_deleteResource_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteResource.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
                                  'serviceType', 'siteName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None ) )

  def test_getResourcePresent_definition( self ):
    
    ins = inspect.getargspec( self.api.getResourcePresent.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'siteName', 
                                  'serviceType', 'gridSiteName', 'siteType','resourceType',
                                  'statusType', 'status', 'dateEffective', 'reason','lastCheckTime', 'tokenOwner',
                                  'tokenExpiration', 'formerStatus' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None, None, None, None, None, None, None ) )

  def test_insertStorageElement_definition( self ):
    
    ins = inspect.getargspec( self.api.insertStorageElement.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
                                  'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )   
    
  def test_updateStorageElement_definition( self ):
    
    ins = inspect.getargspec( self.api.updateStorageElement.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
                                  'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  

  def test_getStorageElement_definition( self ):
    
    ins = inspect.getargspec( self.api.getStorageElement.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
                                  'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )  

  def test_deleteStorageElement_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteStorageElement.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
                                  'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )

  def test_getStorageElementPresent_definition( self ):
    
    ins = inspect.getargspec( self.api.getStorageElementPresent.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
                                  'gridSiteName', 'siteType', 'statusType', 'status',
                                  'dateEffective', 'reason', 'lastCheckTime',
                                  'tokenOwner', 'tokenExpiration', 'formerStatus' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None,None,None ) )

  def test_insertGridSite_definition( self ):
    
    ins = inspect.getargspec( self.api.insertGridSite.f )   
    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )    
    
  def test_updateGridSite_definition( self ):
    
    ins = inspect.getargspec( self.api.updateGridSite.f )   
    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  

  def test_getGridSite_definition( self ):
    
    ins = inspect.getargspec( self.api.getGridSite.f )   
    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None ) )  

  def test_deleteGridSite_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteGridSite.f )   
    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None ) )

  def test_insertElementStatus_definition( self ):
    
    ins = inspect.getargspec( self.api.insertElementStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )    

  def test_updateElementStatus_definition( self ):
    
    ins = inspect.getargspec( self.api.updateElementStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )   

  def test_getElementStatus_definition( self ):
    
    ins = inspect.getargspec( self.api.getElementStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None ) )   

  def test_deleteElementStatus_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteElementStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None ) )   

  def test_insertElementScheduledStatus_definition( self ):
    
    ins = inspect.getargspec( self.api.insertElementScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None ) 
    
  def test_updateElementScheduledStatus_definition( self ):
    
    ins = inspect.getargspec( self.api.updateElementScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )   

  def test_getElementScheduledStatus_definition( self ):
    
    ins = inspect.getargspec( self.api.getElementScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None ) )   

  def test_deleteElementScheduledStatus_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteElementScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None ) )   

  def test_insertElementHistory_definition( self ):
    
    ins = inspect.getargspec( self.api.insertElementHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )

  def test_updateElementHistory_definition( self ):
    
    ins = inspect.getargspec( self.api.updateElementHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )   

  def test_getElementHistory_definition( self ):
    
    ins = inspect.getargspec( self.api.getElementHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None ) )   

  def test_deleteElementHistory_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteElementHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None ) )

  def test_getValidElements_definition( self ):    
    
    ins = inspect.getargspec( self.api.getValidElements.f )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )    

  def test_getValidStatuses_definition( self ):    
    
    ins = inspect.getargspec( self.api.getValidStatuses.f )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )       

  def test_getValidStatusTypes_definition( self ):    
    
    ins = inspect.getargspec( self.api.getValidStatusTypes.f )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )   

  def test_getValidSiteTypes_definition( self ):    
    
    ins = inspect.getargspec( self.api.getValidSiteTypes.f )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )  

  def test_getValidServiceTypes_definition( self ):    
    
    ins = inspect.getargspec( self.api.getValidServiceTypes.f )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None ) 

  def test_getValidResourceTypes_definition( self ):    
    
    ins = inspect.getargspec( self.api.getValidResourceTypes.f )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None ) 

  def test_addOrModifySite_definition( self ):    
    
    ins = inspect.getargspec( self.api.addOrModifySite.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None ) 

  def test_addOrModifyService_definition( self ):    
    
    ins = inspect.getargspec( self.api.addOrModifyService.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None ) 

  def test_addOrModifyResource_definition( self ):    
    
    ins = inspect.getargspec( self.api.addOrModifyResource.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 'serviceType', 
                           'siteName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None ) 

  def test_addOrModifyStorageElement_definition( self ):    
    
    ins = inspect.getargspec( self.api.addOrModifyStorageElement.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None ) 

  def test_addOrModifyGridSite_definition( self ):    
    
    ins = inspect.getargspec( self.api.addOrModifyGridSite.f )   
    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None ) 

  def test_modifyElementStatus_definition( self ):    
    
    ins = inspect.getargspec( self.api.modifyElementStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType',
                                  'status', 'reason', 'dateCreated', 'dateEffective',
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None ) )
        
  def test_removeElement_definition( self ):    
    
    ins = inspect.getargspec( self.api.removeElement.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'elementName' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )    

  def test_getServiceStats_definition( self ):    
    
    ins = inspect.getargspec( self.api.getServiceStats.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )        

  def test_getStorageElementStats_definition( self ):    
    
    ins = inspect.getargspec( self.api.getStorageElementStats.f )   
    self.assertEqual( ins.args, [ 'self', 'element', 'name', 'statusType' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )     

  def test_getGeneralName_definition( self ):    
    
    ins = inspect.getargspec( self.api.getGeneralName.f )   
    self.assertEqual( ins.args, [ 'self', 'from_element', 'name', 'to_element' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )     

  def test_getGridSiteName_definition( self ):    
    
    ins = inspect.getargspec( self.api.getGridSiteName.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )                

  def test_getTokens_definition( self ):    
    
    ins = inspect.getargspec( self.api.getTokens.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'tokenExpiration', 'statusType' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )

  def test_setToken_definition( self ):    
    
    ins = inspect.getargspec( self.api.setToken.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'statusType', 'reason','tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )    

  def test_setReason_definition( self ):    
    
    ins = inspect.getargspec( self.api.setReason.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'statusType', 'reason' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )    

  def test_setDateEnd_definition( self ):    
    
    ins = inspect.getargspec( self.api.setDateEnd.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'statusType', 'dateEffective' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )                 

  def test_whatIs_definition( self ):    
    
    ins = inspect.getargspec( self.api.whatIs.f )   
    self.assertEqual( ins.args, [ 'self', 'name' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )    
      
  def test_getStuffToCheck_definition( self ):    
    
    ins = inspect.getargspec( self.api.getStuffToCheck.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'checkFrequency' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )    
      
  def test_getMonitoredStatus_definition( self ):    
    
    ins = inspect.getargspec( self.api.getMonitoredStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )    
      
  def test_getMonitoredsStatusWeb_definition( self ):    
    
    ins = inspect.getargspec( self.api.getMonitoredsStatusWeb.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'selectDict', 'startItem',
                                  'maxItems' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )       
                        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF