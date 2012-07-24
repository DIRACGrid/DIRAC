#import unittest
#import inspect
#
#class TestCase_Description( unittest.TestCase ):
#  
#  def test_init_definition( self ):
#    
#    ins = inspect.getargspec( self.client.__init__ )
#    self.assertEqual( ins.args, [ 'self', 'serviceIn' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, ) )    
#  
#  def test_insertSite_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#
#  def test_updateSite_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},))  
#    
#  def test_getSite_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) )  
#
#  def test_deleteSite_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) )  
#    
#  def test_getSitePresent_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getSitePresent.f )   
#    self.assertEqual( ins.args, [ 'self', 'siteName','siteType','gridSiteName',
#                                  'gridTier','statusType','status','dateEffective',
#                                  'reason','lastCheckTime','tokenOwner',
#                                  'tokenExpiration','formerStatus', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None,
#                                      None, None, None, None, None, {} ) )  
#
#  def test_insertService_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertService.f )   
#    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#
#  def test_updateService_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateService.f )   
#    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#
#  def test_getService_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getService.f )   
#    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) )  
#
#  def test_deleteService_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteService.f )   
#    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) )  
#
#  def test_getServicePresent_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getServicePresent.f )   
#    self.assertEqual( ins.args, [ 'self', 'serviceName', 'siteName', 'siteType',
#                                  'serviceType', 'statusType', 'status', 'dateEffective',
#                                  'reason', 'lastCheckTime', 'tokenOwner', 'tokenExpiration',
#                                  'formerStatus', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None, None, None, None, None, {} ) )  
#
#  def test_insertResource_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertResource.f )   
#    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
#                                  'serviceType', 'siteName', 'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#
#  def test_updateResource_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateResource.f )   
#    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
#                                  'serviceType', 'siteName', 'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#
#  def test_getResource_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getResource.f )   
#    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
#                                  'serviceType', 'siteName', 'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, {} ) )
#
#  def test_deleteResource_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteResource.f )   
#    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
#                                  'serviceType', 'siteName', 'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, {} ) )
#
#  def test_getResourcePresent_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getResourcePresent.f )   
#    self.assertEqual( ins.args, [ 'self', 'resourceName', 'siteName', 
#                                  'serviceType', 'gridSiteName', 'siteType','resourceType',
#                                  'statusType', 'status', 'dateEffective', 'reason','lastCheckTime', 'tokenOwner',
#                                  'tokenExpiration', 'formerStatus', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None, None, None, None, None, None, None, {} ) )
#
#  def test_insertStorageElement_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertStorageElement.f )   
#    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
#                                  'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )   
#    
#  def test_updateStorageElement_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateStorageElement.f )   
#    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
#                                  'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#
#  def test_getStorageElement_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getStorageElement.f )   
#    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
#                                  'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) )  
#
#  def test_deleteStorageElement_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteStorageElement.f )   
#    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
#                                  'gridSiteName', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) )
#
#  def test_getStorageElementPresent_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getStorageElementPresent.f )   
#    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 
#                                  'gridSiteName', 'siteType', 'statusType', 'status',
#                                  'dateEffective', 'reason', 'lastCheckTime',
#                                  'tokenOwner', 'tokenExpiration', 'formerStatus', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None,None,None,{} ) )
#
#  def test_insertGridSite_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertGridSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )    
#    
#  def test_updateGridSite_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateGridSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#
#  def test_getGridSite_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getGridSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, {} ) )  
#
#  def test_deleteGridSite_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteGridSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, {} ) )
#
#  def test_insertElementStatus_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertElementStatus.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )    
#
#  def test_updateElementStatus_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateElementStatus.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )   
#
#  def test_getElementStatus_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getElementStatus.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None,{} ) )   
#
#  def test_deleteElementStatus_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteElementStatus.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None,{} ) )   
#
#  def test_insertElementScheduledStatus_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertElementScheduledStatus.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) ) 
#    
#  def test_updateElementScheduledStatus_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateElementScheduledStatus.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )   
#
#  def test_getElementScheduledStatus_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getElementScheduledStatus.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None,{} ) )   
#
#  def test_deleteElementScheduledStatus_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteElementScheduledStatus.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None,{} ) )   
#
#  def test_insertElementHistory_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertElementHistory.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )
#
#  def test_updateElementHistory_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateElementHistory.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )   
#
#  def test_getElementHistory_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getElementHistory.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None, {} ) )   
#
#  def test_deleteElementHistory_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteElementHistory.f )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType', 
#                                  'status', 'reason', 'dateCreated', 'dateEffective', 
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
#                                  'tokenExpiration', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None, {} ) )
#
#  def test_getValidElements_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getValidElements )   
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )    
#
#  def test_getValidStatuses_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getValidStatuses )   
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )       
#
#  def test_getValidStatusTypes_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getValidStatusTypes )   
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )   
#
#  def test_getValidSiteTypes_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getValidSiteTypes )   
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )  
#
#  def test_getValidServiceTypes_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getValidServiceTypes )   
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#
#  def test_getValidResourceTypes_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getValidResourceTypes )   
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#
#  def test_addOrModifySite_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.addOrModifySite )   
#    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#
#  def test_addOrModifyService_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.addOrModifyService )   
#    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#
#  def test_addOrModifyResource_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.addOrModifyResource )   
#    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 'serviceType', 
#                           'siteName', 'gridSiteName' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#
#  def test_addOrModifyStorageElement_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.addOrModifyStorageElement )   
#    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 'gridSiteName' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#
#  def test_addOrModifyGridSite_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.addOrModifyGridSite )   
#    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#
#  def test_modifyElementStatus_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.modifyElementStatus )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName', 'statusType',
#                                  'status', 'reason', 'dateCreated', 'dateEffective',
#                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None ) )
#        
#  def test_removeElement_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.removeElement )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'elementName' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )    
#
#  def test_getServiceStats_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getServiceStats )   
#    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, ) )        
#
#  def test_getStorageElementStats_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getStorageElementStats )   
#    self.assertEqual( ins.args, [ 'self', 'element', 'name', 'statusType' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, ) )     
#
#  def test_getGeneralName_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getGeneralName )   
#    self.assertEqual( ins.args, [ 'self', 'from_element', 'name', 'to_element' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )     
#
#  def test_getGridSiteName_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getGridSiteName )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )                
#
#  def test_getTokens_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getTokens )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'tokenExpiration', 'statusType' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, 'kwargs' )
#    self.assertEqual( ins.defaults, ( None, None, None ) )
#
#  def test_setToken_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.setToken )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'statusType', 'reason','tokenOwner', 'tokenExpiration' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )    
#
#  def test_setReason_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.setReason )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'statusType', 'reason' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )    
#
#  def test_setDateEnd_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.setDateEnd )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'statusType', 'dateEffective' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )                 
#
#  def test_whatIs_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.whatIs )   
#    self.assertEqual( ins.args, [ 'self', 'name' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )    
#      
#  def test_getStuffToCheck_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getStuffToCheck )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'checkFrequency' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, 'kwargs' )
#    self.assertEqual( ins.defaults, None )    
#      
#  def test_getTopology( self ):    
#    
#    ins = inspect.getargspec( self.client.getTopology )   
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )    
#      
#  def test_getMonitoredStatus_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getMonitoredStatus )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )    
#      
#  def test_getMonitoredsStatusWeb_definition( self ):    
#    
#    ins = inspect.getargspec( self.client.getMonitoredsStatusWeb )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'selectDict', 'startItem',
#                                  'maxItems' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )       
#
#        
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   