#import unittest
#import inspect
#
#class TestCase_Unit( unittest.TestCase ):
#
#  def test_insertSite_nok( self ):
#
#    res = self.client.insertSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.insertSite( 1, 2, 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.insertSite( 1, 2, 3, siteName = 1 )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.insertSite( 1, 2, 3, siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], False )
#    
#  def test_insertSite_ok( self ):
#    
#    res = self.client.insertSite( 1, 2, 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.insertSite( siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.insertSite( 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.insertSite( 1, 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    
#################################################################################
#
#  def test_updateSite_nok( self ):
#    
#    res = self.client.updateSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.updateSite( 1, 2, 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.updateSite( 1, 2, 3, siteName = 1 )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.updateSite( 1, 2, 3, siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], False )
#
#  def test_updateSite_ok( self ):
#    
#    res = self.client.updateSite( siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.updateSite( 1, 2, 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.updateSite( 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.updateSite( 1, 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    
#################################################################################
#
#  def test_getSite_nok( self ):
#    
#    res = self.client.getSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.getSite( 1, 2, 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.getSite( 1, 2, 3, siteName = 1 )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.getSite( 1, 2, 3, siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], False )
#        
#  def test_getSite_ok( self ):
#    res = self.client.getSite()
#    self.assertEqual ( res['OK'], True )
#    res = self.client.getSite( 1 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.getSite( 1, 2 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.getSite( 1, 2, 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.getSite( siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.getSite( 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.getSite( 1, 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )   
#
#################################################################################
#
#  def test_deleteSite_nok( self ):
#    
#    res = self.client.deleteSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.deleteSite( 1, 2, 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.deleteSite( 1, 2, 3, siteName = 1 )
#    self.assertEqual ( res['OK'], False )
#    res = self.client.deleteSite( 1, 2, 3, siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], False )
#    
#  def test_deleteSite_ok( self ):
#    
#    res = self.client.deleteSite()
#    self.assertEqual ( res['OK'], True )
#    res = self.client.deleteSite( 1 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.deleteSite( 1, 2 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.deleteSite( 1, 2, 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.deleteSite( siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.deleteSite( 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )
#    res = self.client.deleteSite( 1, 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], True )   
#
#
#################################################################################
#
#  def test_getSitePresent_nok( self ):
#    
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,12, siteName = 1 )
#    self.assertEqual( res[ 'OK'], False )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,12, siteName=1,siteType=2,gridSiteName=3,
#                                   gridTier=4,statusType=5,status=6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], False )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,12, a = 'a' )
#    self.assertEqual( res[ 'OK'], False )
#    res = self.client.getSitePresent( siteName=1,siteType=2,gridSiteName=3,
#                                   gridTier=4,statusType=5,status=6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12, a = 'a' )
#    self.assertEqual( res[ 'OK'], False )
#        
#  def test_getSitePresent_ok( self ):
#
#    res = self.client.getSitePresent( )
#    self.assertEqual( res[ 'OK'], True )    
#    res = self.client.getSitePresent( 1 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3, 4 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3, 4, 5 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3, 4, 5, 6 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,12 )
#    self.assertEqual( res[ 'OK'], True )
#
#    res = self.client.getSitePresent( siteName=1,siteType=2,gridSiteName=3,
#                                   gridTier=4,statusType=5,status=6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,siteType=2,gridSiteName=3,
#                                   gridTier=4,statusType=5,status=6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,gridSiteName=3,
#                                   gridTier=4,statusType=5,status=6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,
#                                   gridTier=4,statusType=5,status=6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,statusType=5,status=6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,5,status=6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,dateEffective=7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,
#                                   reason=8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,8,lastCheckTime=9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,8,9,tokenOwner=10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,8,9,10,
#                                   tokenExpiration=11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#    res = self.client.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,formerStatus=12 )
#    self.assertEqual( res[ 'OK'], True )
#
#################################################################################
#
#  def test_insertService_nok( self ):
#    
#    res = self.client.insertService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.insertService( 1, 2, 3, serviceName = 1 )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.insertService( 1, 2, 3, serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.insertService( 1, 2, 3, a = 'a' )
#    self.assertEquals( res[ 'OK'], False )
#
#  def test_insertService_ok( self ):
#    
#    res = self.client.insertService( 1, 2, 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.insertService( serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.insertService( 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.insertService( 1, 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    
#################################################################################
#
#  def test_updateService_nok( self ):
#    
#    res = self.client.updateService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.updateService( 1, 2, 3, serviceName = 1 )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.updateService( 1, 2, 3, serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.updateService( 1, 2, 3, a = 'a' )
#    self.assertEquals( res[ 'OK'], False )
#
#  def test_updateService_ok( self ):
#
#    res = self.client.updateService( serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )    
#    res = self.client.updateService( 1, 2, 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.updateService( 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.updateService( 1, 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#
#################################################################################
#
#  def test_getService_nok( self ):
#    res = self.client.getService( 1, 2, 3, serviceName = 1 )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.getService( 1, 2, 3, serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], False )    
#    res = self.client.getService( 1, 2, 3, a = 'a' )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.getService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
#    self.assertEquals( res[ 'OK'], False )
#    
#  def test_getService_ok( self ):
#
#    res = self.client.getService( )
#    self.assertEquals( res[ 'OK'], True )    
#    res = self.client.getService( 1 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.getService( 1, 2 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.getService( 1, 2, 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.getService( serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.getService( 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.getService( 1, 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    
#################################################################################
#
#  def test_deleteService_nok( self ):
#    res = self.client.deleteService( 1, 2, 3, serviceName = 1 )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.deleteService( 1, 2, 3, serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], False )    
#    res = self.client.deleteService( 1, 2, 3, a = 'a' )
#    self.assertEquals( res[ 'OK'], False )
#    res = self.client.deleteService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
#    self.assertEquals( res[ 'OK'], False )
#    
#  def test_deleteService_ok( self ):
#
#    res = self.client.deleteService( )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.deleteService( 1 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.deleteService( 1, 2 )
#    self.assertEquals( res[ 'OK'], True )    
#    res = self.client.deleteService( 1, 2, 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.deleteService( serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.deleteService( 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#    res = self.client.deleteService( 1, 2, siteName = 3 )
#    self.assertEquals( res[ 'OK'], True )
#
#################################################################################
#
#  def test_getServicePresent_nok( self ):
#    
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, serviceName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, serviceName = 1, siteName = 2, siteType = 3,
#                                      serviceType = 4, statusType = 5, status = 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getServicePresent( serviceName = 1, siteName = 2, siteType = 3,
#                                      serviceType = 4, statusType = 5, status = 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12, a = 'a' )
#    self.assertEquals( res['OK'], False )  
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    
#  def test_getServicePresent_ok( self ):
#    
#    res = self.client.getServicePresent( )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( serviceName = 1, siteName = 2, siteType = 3,
#                                      serviceType = 4, statusType = 5, status = 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, siteName = 2, siteType = 3,
#                                      serviceType = 4, statusType = 5, status = 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )                                                            
#    res = self.client.getServicePresent( 1, 2, siteType = 3,
#                                      serviceType = 4, statusType = 5, status = 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3,
#                                      serviceType = 4, statusType = 5, status = 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )                                  
#    res = self.client.getServicePresent( 1, 2, 3, 4, statusType = 5, status = 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, status = 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 
#                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, reason = 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9,
#                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_insertResource_nok( self ):
#    
#    res = self.client.insertResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    
#    res = self.client.insertResource( 1, 2, 3, 4, 5, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertResource( 1, 2, 3, 4, 5, resourceName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertResource( 1, 2, 3, 4, 5, resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], False )
#
#  def test_insertResource_ok( self ):
#    
#    res = self.client.insertResource( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertResource( 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertResource( 1, 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertResource( 1, 2, 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertResource( 1, 2, 3, 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_updateResource_nok( self ):
#    
#    res = self.client.updateResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateResource( 1, 2, 3, 4, 5, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateResource( 1, 2, 3, 4, 5, resourceName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateResource( 1, 2, 3, 4, 5, resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], False )
#
#  def test_updateResource_ok( self ):
#    
#    res = self.client.updateResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                      siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateResource( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateResource( 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateResource( 1, 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateResource( 1, 2, 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateResource( 1, 2, 3, 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_getResource_nok( self ):
#
#    res = self.client.getResource( 1, 2, 3, 4, 5, resourceName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getResource( 1, 2, 3, 4, 5, resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getResource( 1, 2, 3, 4, 5, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5, a = 'a' )
#    self.assertEquals( res['OK'], False )
#   
#  def test_getResource_ok( self ):
#
#    res = self.client.getResource( )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1, 2, 3, 4 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1, 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1, 2, 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResource( 1, 2, 3, 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#     
#################################################################################
#
#  def test_deleteResource_nok( self ):
#
#    res = self.client.deleteResource( 1, 2, 3, 4, 5, resourceName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.deleteResource( 1, 2, 3, 4, 5, resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.deleteResource( 1, 2, 3, 4, 5, a = 'a' )
#    self.assertEquals( res['OK'], False ) 
#    res = self.client.deleteResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5, a = 'a' )
#    self.assertEquals( res['OK'], False )
#   
#  def test_deleteResource_ok( self ):
#
#    res = self.client.deleteResource( )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteResource( 1 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteResource( 1, 2 )
#    self.assertEquals( res['OK'], True )  
#    res = self.client.deleteResource( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )    
#    res = self.client.deleteResource( 1, 2, 3, 4 )
#    self.assertEquals( res['OK'], True )    
#    res = self.client.deleteResource( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )
#    
#    res = self.client.deleteResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    
#    res = self.client.deleteResource( 1, resourceType = 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteResource( 1, 2, serviceType = 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteResource( 1, 2, 3,
#                                   siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteResource( 1, 2, 3, 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getResourcePresent_nok( self ):
#    
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
#                                       13, 14, resourceName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
#                                       13, 14, resourceName = 1, siteName = 2, serviceType = 3,
#                                       gridSiteName = 4, siteType = 5, resourceType = 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getResourcePresent( resourceName = 1, siteName = 2, serviceType = 3,
#                                       gridSiteName = 4, siteType = 5, resourceType = 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14, a= 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
#                                       13, 14, a = 'a' )
#    self.assertEquals( res['OK'], False )
#        
#  def test_getResourcePresent_ok( self ):
#    
#    res = self.client.getResourcePresent( )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
#                                       13, 14 )
#    self.assertEquals( res['OK'], True )           
#    res = self.client.getResourcePresent( resourceName = 1, siteName = 2, serviceType = 3,
#                                       gridSiteName = 4, siteType = 5, resourceType = 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, siteName = 2, serviceType = 3,
#                                       gridSiteName = 4, siteType = 5, resourceType = 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )        
#    res = self.client.getResourcePresent( 1, 2, serviceType = 3,
#                                       gridSiteName = 4, siteType = 5, resourceType = 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3,
#                                       gridSiteName = 4, siteType = 5, resourceType = 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, siteType = 5, resourceType = 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, resourceType = 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6,
#                                       statusType = 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, status = 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, dateEffective = 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 
#                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, lastCheckTime = 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, tokenOwner = 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
#                                       tokenExpiration = 13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
#                                       13, formerStatus = 14 )
#    self.assertEquals( res['OK'], True )  
#            
#################################################################################
#    
#  def test_insertStorageElement_nok( self ):
#    
#    res = self.client.insertStorageElement( storageElementName = 1, resourceName = 2,
#                                         gridSiteName = 3, a = 'a' )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertStorageElement( 1, 2, 3, a = 'a' )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertStorageElement( 1, 2, 3, storageElementName = 1 )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertStorageElement( 1, 2, 3, storageElementName = 1, 
#                                         resourceName = 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], False )
#    
#  def test_insertStorageElement_ok( self ):
#    
#    res = self.client.insertStorageElement( storageElementName = 1, resourceName = 2,
#                                         gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertStorageElement( 1, resourceName = 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertStorageElement( 1, 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True ) 
#    res = self.client.insertStorageElement( 1, 2, 3 )     
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_updateStorageElement_nok( self ):
#    
#    res = self.client.updateStorageElement( storageElementName = 1, resourceName = 2,
#                                         gridSiteName = 3, a = 'a' )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateStorageElement( 1, 2, 3, a = 'a' )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateStorageElement( 1, 2, 3, storageElementName = 1 )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateStorageElement( 1, 2, 3, storageElementName = 1, 
#                                         resourceName = 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], False )    
#    
#  def test_updateStorageElement_ok( self ):
#    
#    res = self.client.updateStorageElement( storageElementName = 1, resourceName = 2,
#                                            gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )      
#    res = self.client.updateStorageElement( 1, 2, 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateStorageElement( 1, resourceName = 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateStorageElement( 1, 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getStorageElement_nok( self ):
#    res = self.client.getStorageElement( 1, 2, 3, storageElementName = 1 )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.getStorageElement( 1, 2, 3, storageElementName = 1, 
#                                         resourceName = 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], False )    
#    res = self.client.getStorageElement( 1, 2, 3, a = 'a' )     
#    self.assertEquals( res['OK'], False )    
#    res = self.client.getStorageElement( storageElementName = 1, resourceName = 2,
#                                         gridSiteName = 3, a = 'a' )     
#    self.assertEquals( res['OK'], False )
#        
#  def test_getStorageElement_ok( self ):
#
#    res = self.client.getStorageElement( )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElement( 1 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElement( 1, 2 )     
#    self.assertEquals( res['OK'], True )      
#    res = self.client.getStorageElement( 1, 2, 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElement( storageElementName = 1, resourceName = 2,
#                                         gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElement( 1, resourceName = 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElement( 1, 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#    
#  def test_deleteStorageElement_nok( self ):
#    
#    res = self.client.deleteStorageElement( 1, 2, 3, storageElementName = 1 )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.deleteStorageElement( 1, 2, 3, storageElementName = 1, 
#                                         resourceName = 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], False )    
#    res = self.client.deleteStorageElement( 1, 2, 3, a = 'a' )     
#    self.assertEquals( res['OK'], False )
#    res = self.client.deleteStorageElement( storageElementName = 1, resourceName = 2,
#                                         gridSiteName = 3, a = 'a' )     
#    self.assertEquals( res['OK'], False )
#        
#  def test_deleteStorageElement_ok( self ):
#    
#    res = self.client.deleteStorageElement( )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteStorageElement( 1 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteStorageElement( 1, 2 )     
#    self.assertEquals( res['OK'], True )  
#    res = self.client.deleteStorageElement( 1, 2, 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteStorageElement( storageElementName = 1, resourceName = 2,
#                                         gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteStorageElement( 1, resourceName = 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteStorageElement( 1, 2, gridSiteName = 3 )     
#    self.assertEquals( res['OK'], True )
#               
#################################################################################
#
#  def test_getStorageElementPresent_nok( self ):
#    
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, storageElementName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, storageElementName = 1, resourceName = 2,
#                                             gridSiteName = 3, siteType = 4, statusType = 5,
#                                             status = 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getStorageElementPresent( storageElementName = 1, resourceName = 2,
#                                             gridSiteName = 3, siteType = 4, statusType = 5,
#                                             status = 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12, a = 'a')
#    self.assertEquals( res['OK'], False )
#  
#  def test_getStorageElementPresent_ok( self ):
#    
#    res = self.client.getStorageElementPresent( storageElementName = 1, resourceName = 2,
#                                             gridSiteName = 3, siteType = 4, statusType = 5,
#                                             status = 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12)
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, resourceName = 2,
#                                             gridSiteName = 3, siteType = 4, statusType = 5,
#                                             status = 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2,
#                                             gridSiteName = 3, siteType = 4, statusType = 5,
#                                             status = 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, siteType = 4, statusType = 5,
#                                             status = 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, statusType = 5,
#                                             status = 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5,
#                                             status = 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, dateEffective = 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, reason = 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8,
#                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, tokenOwner = 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11,
#                                             formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, formerStatus = 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3, 4 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( 1 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementPresent( )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#    
#  def test_insertGridSite_nok( self ):
#    
#    res = self.client.insertGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
#    self.assertEquals( res['OK'], False )          
#    res = self.client.insertGridSite( 1, 2, a = 'a')
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertGridSite( 1, 2, gridSiteName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertGridSite( 1, 2, gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], False )
#
#  def test_insertGridSite_ok( self ):
#    
#    res = self.client.insertGridSite( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertGridSite( 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertGridSite( gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_updateGridSite_nok( self ):
#    
#    res = self.client.updateGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateGridSite( 1, 2, a = 'a')
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateGridSite( 1, 2, gridSiteName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.updateGridSite( 1, 2, gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], False )
#
#  def test_updateGridSite_ok( self ):
#    
#    res = self.client.updateGridSite( gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateGridSite( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.updateGridSite( 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True )          
#
#################################################################################
#
#  def test_getGridSite_nok( self ):       
#
#    res = self.client.getGridSite( 1, 2, gridSiteName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getGridSite( 1, 2, gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.getGridSite( 1, 2, a = 'a')
#    self.assertEquals( res['OK'], False )
#    res = self.client.getGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
#    self.assertEquals( res['OK'], False )
#
#  def test_getGridSite_ok( self ):
#    
#    res = self.client.getGridSite( )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGridSite( 1 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGridSite( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGridSite( gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGridSite( 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True ) 
#    
#################################################################################
#
#  def test_deleteGridSite_nok( self ):       
#
#    res = self.client.deleteGridSite( 1, 2, gridSiteName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.deleteGridSite( 1, 2, gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.deleteGridSite( 1, 2, a = 'a')
#    self.assertEquals( res['OK'], False )
#    res = self.client.deleteGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
#    self.assertEquals( res['OK'], False )
#
#  def test_deleteGridSite_ok( self ):
#    
#    res = self.client.deleteGridSite( )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteGridSite( 1 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteGridSite( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteGridSite( gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.deleteGridSite( 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True ) 
#
#################################################################################
#
#  def test_insertElementStatus_nok( self ):
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = self.client.insertElementStatus( element = '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
#                                        elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.insertElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#  
#  def test_insertElementStatus_ok( self ):
#    
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( element = '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )  
#        
#################################################################################
#
#  def test_updateElementStatus_nok( self ):
#    
#    f = self.client.updateElementStatus
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
#             elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#  
#  def test_updateElementStatus_ok( self ):
#
#    f = self.client.updateElementStatus
#
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )  
#        
#################################################################################
#
#  def test_getElementStatus_nok( self ):
#    
#    f = self.client.getElementStatus
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
#                                        elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )  
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#    res = f()
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', elementName = 2, statusType = 3,
#                                     status = 4, reason = 5, dateCreated = 6, 
#                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    
#  def test_getElementStatus_ok( self ):
#    
#    f = self.client.getElementStatus
#    
#    res = f( '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1' )
#    self.assertEquals( res['OK'], True )        
#                       
#################################################################################
#
#  def test_deleteElementStatus_nok( self ):
#    
#    f = self.client.deleteElementStatus
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
#                                        elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )  
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#    res = f()
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', elementName = 2, statusType = 3,
#                                     status = 4, reason = 5, dateCreated = 6, 
#                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    
#  def test_deleteElementStatus_ok( self ):
#    
#    f = self.client.deleteElementStatus
#    
#    res = f( '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1' )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#    
#  def test_insertElementScheduledStatus_nok( self ):
#    
#    f = self.client.insertElementScheduledStatus
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
#             elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#  
#  def test_insertElementScheduledStatus_ok( self ):
#    
#    f = self.client.insertElementScheduledStatus
#
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )       
#
#################################################################################
#
#  def test_updateElementScheduledStatus_nok( self ):
#    
#    f = self.client.updateElementScheduledStatus
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
#             elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#  
#  def test_updateElementScheduledStatus_ok( self ):
#    
#    f = self.client.updateElementScheduledStatus
#
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )  
#            
#################################################################################
#
#  def test_getElementScheduledStatus_nok( self ):
#    
#    f = self.client.getElementScheduledStatus
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
#                                        elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )  
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#    res = f()
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', elementName = 2, statusType = 3,
#                                     status = 4, reason = 5, dateCreated = 6, 
#                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#  
#  def test_getElementScheduledStatus_ok( self ):
#    
#    f = self.client.getElementScheduledStatus
#    
#    res = f( '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1' )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_deleteElementScheduledStatus_nok( self ):
#    
#    f = self.client.deleteElementScheduledStatus
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
#                                        elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )  
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#    res = f()
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', elementName = 2, statusType = 3,
#                                     status = 4, reason = 5, dateCreated = 6, 
#                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#  
#  def test_deleteElementScheduledStatus_ok( self ):
#    
#    f = self.client.deleteElementScheduledStatus
#    
#    res = f( '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1' )
#    self.assertEquals( res['OK'], True )
#    
#
#################################################################################
#
#  def test_insertElementHistory_nok( self ):
#    
#    f = self.client.insertElementHistory
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
#             elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#  
#  def test_insertElementHistory_ok( self ):
#    
#    f = self.client.insertElementHistory
#
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True ) 
#
#################################################################################
#
#  def test_updateElementHistory_nok( self ):
#
#    f = self.client.updateElementHistory
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
#             elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#  
#  def test_updateElementHistory_ok( self ):
#    
#    f = self.client.updateElementHistory
#
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( element = '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', elementName = 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#             status = 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#             dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#             tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True ) 
#        
################################################################################# 
#
#  def test_getElementHistory_nok( self ):
#    
#    f = self.client.getElementHistory
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
#                                        elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )  
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#    res = f()
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', elementName = 2, statusType = 3,
#                                     status = 4, reason = 5, dateCreated = 6, 
#                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#  
#  def test_getElementHistory_ok( self ):
#    
#    f = self.client.getElementHistory
#    
#    res = f( '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1' )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_deleteElementHistory_nok( self ):
#    
#    f = self.client.deleteElementHistory
#    
#    # element must be string, otherwise the decorator returns S_ERROR
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
#                                        elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )  
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )
#    res = f()
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', elementName = 2, statusType = 3,
#                                     status = 4, reason = 5, dateCreated = 6, 
#                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
#    self.assertEquals( res['OK'], False )
#
#  
#  def test_deleteElementHistory_ok( self ):
#    
#    f = self.client.deleteElementHistory
#    
#    res = f( '1', elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3,
#                                        status = 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, dateCreated = 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 
#                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9,
#                                        tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3, 4 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2, 3 )
#    self.assertEquals( res['OK'], True )    
#    res = f( '1', 2 )
#    self.assertEquals( res['OK'], True )
#    res = f( '1' )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_getValidElements_nok( self ):
#    pass
#  
#  def test_getValidElements_ok( self ):
#    
#    res = self.client.getValidElements()
#    self.assertEquals( res['OK'], True )
#
#################################################################################   
#
#  def test_getValidStatuses_nok( self ):
#    pass
#  
#  def test_getValidStatuses_ok( self ):
#    
#    res = self.client.getValidStatuses()
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getValidStatusTypes_nok( self ):
#    pass
#  
#  def test_getValidStatusTypes_ok( self ):
#    
#    res = self.client.getValidStatusTypes()
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getValidSiteTypes_nok( self ):
#    pass
#  
#  def test_getValidSiteTypes_ok( self ):
#    
#    res = self.client.getValidSiteTypes()
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getValidServiceTypes_nok( self ):
#    pass
#  
#  def test_getValidServiceTypes_ok( self ):
#    
#    res = self.client.getValidServiceTypes()
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getValidResourceTypes_nok( self ):
#    pass
#  
#  def test_getValidResourceTypes_ok( self ):
#    
#    res = self.client.getValidResourceTypes()
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_addOrModifySite_nok( self ):
#    
#    f = self.client.addOrModifySite
#    
#    rDict = { 'siteName' : 1, 'siteType' : 2, 'gridSiteName' : 3, 'a' : 'a' }
#    self.assertRaises( TypeError, f, **rDict)
#    
#    rTupl = ( 1, 2, 3, )
#    rDict = { 'a' : 'a' } 
#    self.assertRaises( TypeError, f, *rTupl, **rDict)
#
#  def test_addOrModifySite_ok( self ):
#    
#    res = self.client.addOrModifySite( siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifySite( 1, siteType = 2, gridSiteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifySite( 1, 2, gridSiteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifySite( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_addOrModifyService_nok( self ):
#
#    f = self.client.addOrModifyService
#    
#    rDict = { 'serviceName' : 1, 'serviceType' : 2, 'siteName' : 3, 'a' : 'a' }
#    self.assertRaises( TypeError, f, **rDict)
#    
#    rTupl = ( 1, 2, 3, )
#    rDict = { 'a' : 'a' } 
#    self.assertRaises( TypeError, f, *rTupl, **rDict)
#
#  def test_addOrModifyService_ok( self ):
#    
#    res = self.client.addOrModifyService( serviceName = 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyService( 1, serviceType = 2, siteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyService( 1, 2, siteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyService( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_addOrModifyResource_nok( self ):
#
#    f = self.client.addOrModifyService
#    
#    rDict = { 'resourceName' : 1, 'resourceType' : 2, 'serviceType' : 3,
#              'siteName' : 4, 'gridSiteName' : 5, 'a' : 'a' }
#    self.assertRaises( TypeError, f, **rDict)
#    
#    rTupl = ( 1, 2, 3, 4, 5 )
#    rDict = { 'a' : 'a' } 
#    self.assertRaises( TypeError, f, *rTupl, **rDict)
#
#  def test_addOrModifyResource_ok( self ):
#    
#    res = self.client.addOrModifyResource( resourceName = 1, resourceType = 2, serviceType = 3,
#                                        siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyResource( 1, resourceType = 2, serviceType = 3,
#                                        siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyResource( 1, 2, serviceType = 3,
#                                        siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyResource( 1, 2, 3, siteName = 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyResource( 1, 2, 3, 4, gridSiteName = 5 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyResource( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_addOrModifyStorageElement_nok( self ):
#
#    f = self.client.addOrModifyStorageElement
#    
#    rDict = { 'storageElementName' : 1, 'resourceName' : 2, 'gridSiteName' : 3, 'a' : 'a' }
#    self.assertRaises( TypeError, f, **rDict)
#    
#    rTupl = ( 1, 2, 3, )
#    rDict = { 'a' : 'a' } 
#    self.assertRaises( TypeError, f, *rTupl, **rDict)
#
#  def test_addOrModifyStorageElement_ok( self ):
#    
#    res = self.client.addOrModifyStorageElement( storageElementName = 1, resourceName = 2, gridSiteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyStorageElement( 1, resourceName = 2, gridSiteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyStorageElement( 1, 2, gridSiteName = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyStorageElement( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_addOrModifyGridSite_nok( self ):
#    
#    f = self.client.addOrModifyGridSite
#    
#    rDict = { 'gridSiteName' : 1, 'gridTier' : 2, 'a' : 'a' }
#    self.assertRaises( TypeError, f, **rDict)
#    
#    rTupl = ( 1, 2, )
#    rDict = { 'a' : 'a' } 
#    self.assertRaises( TypeError, f, *rTupl, **rDict)
#    
#  def test_addOrModifyGridSite_ok( self ):
#    
#    res = self.client.addOrModifyGridSite( gridSiteName = 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyGridSite( 1, gridTier = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.addOrModifyGridSite( 1, 2 )
#    self.assertEquals( res['OK'], True )  
#    
#################################################################################
#        
#  def test_modifyElementStatus_nok( self ):
#    
#    f = self.client.modifyElementStatus
#    
#    rTupl = ( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, )
#    rDict = { 'elementName' : 2 } 
#    self.assertRaises( TypeError, f, *rTupl, **rDict)
#    rDict = { 'elementName' : 2, 'statusType' : 3, 'status' : 4, 'reason' : 5, 
#              'dateCreated' : 6, 'dateEffective' : 7, 'dateEnd' : 8, 
#              'lastCheckTime' : 9, 'tokenOwner' : 10, 'tokenExpiration' : 11 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict)
#    rDict = { 'a' : 'a' }
#    self.assertRaises( TypeError, f, **rDict)
#    rTupl = ( 1, 2, )
#    self.assertRaises( TypeError, f, *rTupl)
#    rTupl = ( 1, )
#    self.assertRaises( TypeError, f, *rTupl)
#    rTupl = ()
#    self.assertRaises( TypeError, f, *rTupl)
#    res = self.client.modifyElementStatus( element = 1, elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
#                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#
#    res = self.client.modifyElementStatus( 1, elementName = 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
#                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, statusType = 3,
#                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
#                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3,
#                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
#                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, reason = 5, dateCreated = 6, dateEffective = 7,
#                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, dateCreated = 6, dateEffective = 7,
#                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, dateEffective = 7,
#                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7,
#                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, tokenOwner = 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
#    self.assertEquals( res['OK'], False )   
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5, 6 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4, 5 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3, 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.modifyElementStatus( 1, 2, 3 )
#    self.assertEquals( res['OK'], False )  
#      
#  def test_modifyElementStatus_ok( self ):
#    pass        
#        
#################################################################################
#
#  def test_removeElement_nok( self ):
#    
#    f = self.client.removeElement
#    
#    rDict = { 'element' : 1 }
#    rTupl = ( 1, 2, )
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'element' : 1, 'elementName' : 2 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'a' : 'a'}
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    res = self.client.removeElement( element = 1, elementName = 2 )
#    self.assertEquals( res['OK'], False )
#    
#  def test_removeElement_ok( self ):
#    
#    res = self.client.removeElement( element = 'Site', elementName = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.removeElement( 'Site', elementName = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.removeElement( 'Site', 2 )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################    
#
#  def test_getServiceStats_nok( self ):
#    
#    f = self.client.getServiceStats
#    
#    rTupl = ( 1, 2, )
#    rDict = { 'siteName' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'siteName' : 1, 'statusType' : 2 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'a' : 'a' }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    self.assertRaises( TypeError, f )
#  
#  def test_getServiceStats_ok( self ):
#    
#    res = self.client.getServiceStats( siteName = 1, statusType = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServiceStats( 1, statusType = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServiceStats( 1, 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getServiceStats( 1 )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_getStorageElementStats_nok( self ):
#    
#    f = self.client.getStorageElementStats
#    
#    rTupl = ( 1, 2, 3, )
#    rDict = { 'element' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )  
#    rDict = { 'element' : 1, 'name' : 2, 'statusType' : 3 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )  
#    rDict = { 'a' : 'a' }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )  
#    rTupl = ( 1,)
#    self.assertRaises( TypeError, f, *rTupl )
#    self.assertRaises( TypeError, f )
#    res = self.client.getStorageElementStats( 1, 2, 3 )
#    self.assertEquals( res['OK'], False )
#
#  def test_getStorageElementStats_ok( self ):
#    
#    res = self.client.getStorageElementStats( element = 'Site', name = 2, statusType = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementStats( 'Site', name = 2, statusType = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementStats( 'Site', 2, statusType = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementStats( 'Site', 2, 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getStorageElementStats( 'Site', 2 )
#    self.assertEquals( res['OK'], True )
#    
################################################################################# 
#
#  def test_getGeneralName_nok( self ):
#    
#    f = self.client.getGeneralName
#    
#    rTupl = ( 1,2,3,)
#    rDict = { 'from_element' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )    
#    rDict= { 'from_element' : 1, 'name' : 2, 'to_element' : 3 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )    
#    rDict = { 'a' : 'a' }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    res = self.client.getGeneralName( 1, 2, 3 )
#    self.assertEquals( res['OK'], False )
#
#  def test_getGeneralName_ok( self ):
#  
#    res = self.client.getGeneralName( from_element = 'Service', name = 2, to_element = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGeneralName( 'Service', name = 2, to_element = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGeneralName( 'Service', 2, to_element = 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGeneralName( 'Service', 2, 3 )
#    self.assertEquals( res['OK'], True )
#  
#################################################################################           
#
#  def test_getGridSiteName_nok( self ):
#    
#    f = self.client.getGridSiteName
#    
#    rTupl = ( 1,2, )
#    rDict = { 'granularity' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'granularity' : 1, 'name' : 2 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'a' : 'a' }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#
#  def test_getGridSiteName_ok( self ):
#    
#    res = self.client.getGridSiteName( granularity = 'Site', name = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGridSiteName( 'Site', name = 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getGridSiteName( 'Site', 2 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getTokens_nok( self ):
#    
#    f = self.client.getTokens
#    
#    rTupl = ( 1,2,3,4,)
#    rDict = { 'granularity' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'granularity' : 1, 'name' : 2, 'tokenExpiration' : 3, 'statusType' : 4 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    self.assertRaises( TypeError, f )
#    
#  def test_getTokens_ok( self ):
#      
#    res = self.client.getTokens( 'Site', 2, 3, 4 )
#    self.assertEquals( res['OK'], True )  
#    res = self.client.getTokens( 'Site', 2, 3, 4, a = 'a'  )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getTokens( 'Site', 2, 3, statusType = 4 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getTokens( 'Site', 2, tokenExpiration = 3, statusType = 4 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getTokens( 'Site', name = 2, tokenExpiration = 3, statusType = 4 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getTokens( granularity = 'Site', name = 2, tokenExpiration = 3, statusType = 4 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getTokens( 'Site', 2, 3 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getTokens( 'Site', 2 )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getTokens( 'Site' )
#    self.assertEquals( res['OK'], True )
#    res = self.client.getTokens( granularity = 'Site' )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_setToken_nok( self ):
#    
#    f = self.client.setToken
#    
#    rTupl = ( 1,2,3,4,5,6,)
#    rDict = { 'granularity' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict ) 
#    rDict = { 'granularity' : 1, 'name' : 2, 'statusType' : 3, 'reason' : 4,
#              'tokenOwner' : 5, 'tokenExpiration' : 6 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'a' : 'a' }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    self.assertRaises( TypeError, f )
#    rDict = { 'tokenExpiration' : 6}
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'tokenOwner' : 5, 'tokenExpiration' : 6}
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    res = self.client.setToken( 'Site', 2, 3, reason = 4, tokenOwner = 5, tokenExpiration = 6 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setToken( 'Site', 2, statusType = 3, reason = 4, tokenOwner = 5, tokenExpiration = 6 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setToken( 'Site', name = 2, statusType = 3, reason = 4, tokenOwner = 5, tokenExpiration = 6 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setToken( granularity = 'Site', name = 2, statusType = 3, reason = 4, tokenOwner = 5, tokenExpiration = 6 )
#    self.assertEquals( res['OK'], False )
#
#  def test_setToken_ok( self ):
#    pass    
#
#################################################################################   
#
#  def test_setReason_nok( self ):
#
#    f = self.client.setReason
#    
#    rTupl = ( 1,2,3,4,)
#    rDict = { 'granularity' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'granularity' : 1, 'name' : 2, 'statusType' : 3, 'reason' : 4} 
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'a' : 'a'}
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    self.assertRaises( TypeError, f )
#    res = self.client.setReason( 'Site', 2, 3, 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setReason( 'Site', 2, 3, reason = 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setReason( 'Site', 2, statusType = 3, reason = 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setReason( 'Site', name = 2, statusType = 3, reason = 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setReason( granularity = 'Site', name = 2, statusType = 3, reason = 4 )
#    self.assertEquals( res['OK'], False )
#        
#  def test_setReason_ok( self ):
#    pass
#    
#################################################################################               
#
#  def test_setDateEnd_nok( self ):
#    
#    f = self.client.setDateEnd
#    
#    rTupl = (1,2,3,4,)
#    rDict = {'granularity' : 1} 
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'granularity' : 1, 'name' : 2, 'statusType' : 3, 'dateEffective' : 4}
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'a' : 'a' }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    self.assertRaises( TypeError, f)
#    res = self.client.setDateEnd( 1, 2, 3, 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setDateEnd( 1, 2, 3, dateEffective = 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setDateEnd( 1, 2, statusType = 3, dateEffective = 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setDateEnd( 1, name = 2, statusType = 3, dateEffective = 4 )
#    self.assertEquals( res['OK'], False )
#    res = self.client.setDateEnd( granularity = 1, name = 2, statusType = 3, dateEffective = 4 )
#    self.assertEquals( res['OK'], False )
#    
#  def test_setDateEnd_ok( self ):
#    pass
#            
#################################################################################  
#      
#  def test_whatIs_nok( self ):
#    
#    f = self.client.whatIs
#    
#    rTupl = ( 1, )
#    rDict = { 'name' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    self.assertRaises( TypeError, f )  
#      
#  def test_whatIs_ok( self ):
#    
#    res = self.client.whatIs( 1 )
#    self.assertEquals( res['OK'], True )    
#      
#################################################################################   
#      
#  def test_getStuffToCheck_nok( self ):
#    
#    f = self.client.getStuffToCheck
#    
#    rTupl = (1,2,)
#    rDict = { 'granularity' : 1 }
#    self.assertRaises( AttributeError, f, *rTupl )
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'granularity' : 1, 'checkFrequency' : 2 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    res = self.client.getStuffToCheck( 'Site', {}, a = 'a' )
#    self.assertEquals( res['OK'], False)     
#    
#  def test_getStuffToCheck_ok( self ):
#    
#    res = self.client.getStuffToCheck( 'Site', {} )
#    self.assertEquals( res['OK'], True )
#          
#################################################################################   
#
#  def test_getTopology_nok( self ):
#    pass
#  
#  def test_getTopology_ok( self ):
#    
#    res = self.client.getTopology()
#    self.assertEquals( res['OK'], True )  
#
#################################################################################   
#      
#  def test_getMonitoredStatus_nok( self ):
#    
#    f = self.client.getMonitoredStatus
#    
#    rTupl = ( 1,2,)
#    rDict = { 'granularity' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'granularity' : 1, 'name' : 2 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'a' : 'a'}
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    self.assertRaises( TypeError, f )
#    
#  def test_getMonitoredStatus_ok( self ):
#    
#    res = self.client.getMonitoredStatus( 'Site', 2 )
#    self.assertEquals( res['OK'], True )    
#      
#################################################################################   
#            
#  def test_getMonitoredsStatusWeb_nok( self ):
#    
#    f = self.client.getMonitoredsStatusWeb
#    
#    rTupl = ( 1,2,3,4,)
#    rDict = { 'granularity' : 1 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'granularity' : 1, 'selectDict' : 2, 'startItem' : 3, 'maxItems' : 4 }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    rDict = { 'a' : 'a' }
#    self.assertRaises( TypeError, f, *rTupl, **rDict )
#    self.assertRaises( TypeError, f )
#    
#  def test_getMonitoredsStatusWeb_ok( self ):
#    
#    res = self.client.getMonitoredsStatusWeb( 'Site', {}, 3, 4 )
#    self.assertEqual( res['OK'], True )
#    
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   