import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):

  def test_insertSite_nok( self ):
    
    res = self.api.insertSite( siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], False )
    res = self.api.insertSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
    self.assertEqual ( res['OK'], False )
    res = self.api.insertSite( 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], False )
    res = self.api.insertSite( 1, 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], False )

  def test_insertSite_ok( self ):
    
    res = self.api.insertSite( 1, 2, 3 )
    self.assertEqual ( res['OK'], True )
    res = self.api.insertSite( 1, 2, 3, a = 'a' )
    self.assertEqual ( res['OK'], True )
    res = self.api.insertSite( 1, 2, 3, siteName = 1 )
    self.assertEqual ( res['OK'], True )
    res = self.api.insertSite( 1, 2, 3, siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], True )

################################################################################

  def test_updateSite_nok( self ):
    
    res = self.api.updateSite( siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], False )
    res = self.api.updateSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
    self.assertEqual ( res['OK'], False )
    res = self.api.updateSite( 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], False )
    res = self.api.updateSite( 1, 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], False )

  def test_updateSite_ok( self ):
    
    res = self.api.updateSite( 1, 2, 3 )
    self.assertEqual ( res['OK'], True )
    res = self.api.updateSite( 1, 2, 3, a = 'a' )
    self.assertEqual ( res['OK'], True )
    res = self.api.updateSite( 1, 2, 3, siteName = 1 )
    self.assertEqual ( res['OK'], True )
    res = self.api.updateSite( 1, 2, 3, siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], True )
    
################################################################################

  def test_getSite_nok( self ):
    res = self.api.getSite( 1, 2, 3, siteName = 1 )
    self.assertEqual ( res['OK'], False )
    res = self.api.getSite( 1, 2, 3, siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], False )
    
  def test_getSite_ok( self ):
    res = self.api.getSite()
    self.assertEqual ( res['OK'], True )
    res = self.api.getSite( 1 )
    self.assertEqual ( res['OK'], True )
    res = self.api.getSite( 1, 2 )
    self.assertEqual ( res['OK'], True )
    res = self.api.getSite( 1, 2, 3 )
    self.assertEqual ( res['OK'], True )
    res = self.api.getSite( 1, 2, 3, a = 'a' )
    self.assertEqual ( res['OK'], True )
    res = self.api.getSite( siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], True )
    res = self.api.getSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
    self.assertEqual ( res['OK'], True )
    res = self.api.getSite( 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], True )
    res = self.api.getSite( 1, 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], True )   

################################################################################

  def test_deleteSite_nok( self ):
    res = self.api.deleteSite( 1, 2, 3, siteName = 1 )
    self.assertEqual ( res['OK'], False )
    res = self.api.deleteSite( 1, 2, 3, siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], False )
    
  def test_deleteSite_ok( self ):
    res = self.api.deleteSite()
    self.assertEqual ( res['OK'], True )
    res = self.api.deleteSite( 1 )
    self.assertEqual ( res['OK'], True )
    res = self.api.deleteSite( 1, 2 )
    self.assertEqual ( res['OK'], True )
    res = self.api.deleteSite( 1, 2, 3 )
    self.assertEqual ( res['OK'], True )
    res = self.api.deleteSite( 1, 2, 3, a = 'a' )
    self.assertEqual ( res['OK'], True )
    res = self.api.deleteSite( siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], True )
    res = self.api.deleteSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
    self.assertEqual ( res['OK'], True )
    res = self.api.deleteSite( 1, siteType = 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], True )
    res = self.api.deleteSite( 1, 2, gridSiteName = 3 )
    self.assertEqual ( res['OK'], True )   

################################################################################

  def test_getSitePresent_nok( self ):
    
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,12, siteName = 1 )
    self.assertEqual( res[ 'OK'], False )
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,12, siteName=1,siteType=2,gridSiteName=3,
                                   gridTier=4,statusType=5,status=6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], False )
        
  def test_getSitePresent_ok( self ):

    res = self.api.getSitePresent( )
    self.assertEqual( res[ 'OK'], True )    
    res = self.api.getSitePresent( 1 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3, 4 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3, 4, 5 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3, 4, 5, 6 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,12, a = 'a' )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( siteName=1,siteType=2,gridSiteName=3,
                                   gridTier=4,statusType=5,status=6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( siteName=1,siteType=2,gridSiteName=3,
                                   gridTier=4,statusType=5,status=6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12, a = 'a' )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,siteType=2,gridSiteName=3,
                                   gridTier=4,statusType=5,status=6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,gridSiteName=3,
                                   gridTier=4,statusType=5,status=6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,
                                   gridTier=4,statusType=5,status=6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,statusType=5,status=6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,status=6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,6,dateEffective=7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,
                                   reason=8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,8,lastCheckTime=9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,8,9,tokenOwner=10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,8,9,10,
                                   tokenExpiration=11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )
    res = self.api.getSitePresent( 1,2,3,4,5,6,7,8,9,10,11,formerStatus=12 )
    self.assertEqual( res[ 'OK'], True )

################################################################################

  def test_insertService_nok( self ):
    
    res = self.api.insertService( serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], False )
    res = self.api.insertService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
    self.assertEquals( res[ 'OK'], False )
    res = self.api.insertService( 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], False )
    res = self.api.insertService( 1, 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], False )

  def test_insertService_ok( self ):
    
    res = self.api.insertService( 1, 2, 3 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.insertService( 1, 2, 3, a = 'a' )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.insertService( 1, 2, 3, serviceName = 1 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.insertService( 1, 2, 3, serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], True )
    
################################################################################

  def test_updateService_nok( self ):
    
    res = self.api.updateService( serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], False )
    res = self.api.updateService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
    self.assertEquals( res[ 'OK'], False )
    res = self.api.updateService( 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], False )
    res = self.api.updateService( 1, 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], False )

  def test_updateService_ok( self ):
    
    res = self.api.updateService( 1, 2, 3 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.updateService( 1, 2, 3, a = 'a' )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.updateService( 1, 2, 3, serviceName = 1 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.updateService( 1, 2, 3, serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], True )

################################################################################

  def test_getService_nok( self ):
    res = self.api.getService( 1, 2, 3, serviceName = 1 )
    self.assertEquals( res[ 'OK'], False )
    res = self.api.getService( 1, 2, 3, serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], False )    
    
  def test_getService_ok( self ):

    res = self.api.getService( )
    self.assertEquals( res[ 'OK'], True )    
    res = self.api.getService( 1 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.getService( 1, 2 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.getService( 1, 2, 3 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.getService( 1, 2, 3, a = 'a' )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.getService( serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.getService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.getService( 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.getService( 1, 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], True )
    
################################################################################

  def test_deleteService_nok( self ):
    res = self.api.deleteService( 1, 2, 3, serviceName = 1 )
    self.assertEquals( res[ 'OK'], False )
    res = self.api.deleteService( 1, 2, 3, serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], False )    
    
  def test_deleteService_ok( self ):

    res = self.api.deleteService( )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.deleteService( 1 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.deleteService( 1, 2 )
    self.assertEquals( res[ 'OK'], True )    
    res = self.api.deleteService( 1, 2, 3 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.deleteService( 1, 2, 3, a = 'a' )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.deleteService( serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.deleteService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.deleteService( 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], True )
    res = self.api.deleteService( 1, 2, siteName = 3 )
    self.assertEquals( res[ 'OK'], True )

################################################################################

  def test_getServicePresent_nok( self ):
    
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, serviceName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, serviceName = 1, siteName = 2, siteType = 3,
                                      serviceType = 4, statusType = 5, status = 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], False )  
    
  def test_getServicePresent_ok( self ):
    
    res = self.api.getServicePresent( )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( serviceName = 1, siteName = 2, siteType = 3,
                                      serviceType = 4, statusType = 5, status = 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( serviceName = 1, siteName = 2, siteType = 3,
                                      serviceType = 4, statusType = 5, status = 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, siteName = 2, siteType = 3,
                                      serviceType = 4, statusType = 5, status = 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )                                                            
    res = self.api.getServicePresent( 1, 2, siteType = 3,
                                      serviceType = 4, statusType = 5, status = 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3,
                                      serviceType = 4, statusType = 5, status = 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )                                  
    res = self.api.getServicePresent( 1, 2, 3, 4, statusType = 5, status = 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, status = 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 
                                      dateEffective = 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, reason = 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9,
                                      tokenOwner = 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServicePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    
################################################################################

  def test_insertResource_nok( self ):
    
    res = self.api.insertResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.insertResource( 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertResource( 1, 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertResource( 1, 2, 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertResource( 1, 2, 3, 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )

  def test_insertResource_ok( self ):
    
    res = self.api.insertResource( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertResource( 1, 2, 3, 4, 5, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.insertResource( 1, 2, 3, 4, 5, resourceName = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertResource( 1, 2, 3, 4, 5, resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_updateResource_nok( self ):
    
    res = self.api.updateResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.updateResource( 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateResource( 1, 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateResource( 1, 2, 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateResource( 1, 2, 3, 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )

  def test_updateResource_ok( self ):
    
    res = self.api.updateResource( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateResource( 1, 2, 3, 4, 5, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.updateResource( 1, 2, 3, 4, 5, resourceName = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateResource( 1, 2, 3, 4, 5, resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getResource_nok( self ):

    res = self.api.getResource( 1, 2, 3, 4, 5, resourceName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getResource( 1, 2, 3, 4, 5, resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
   
  def test_getResource_ok( self ):

    res = self.api.getResource( )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, 2, 3, 4, 5, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, 2, 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResource( 1, 2, 3, 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
     
################################################################################

  def test_deleteResource_nok( self ):

    res = self.api.deleteResource( 1, 2, 3, 4, 5, resourceName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteResource( 1, 2, 3, 4, 5, resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], False )
   
  def test_deleteResource_ok( self ):

    res = self.api.deleteResource( )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( 1, 2 )
    self.assertEquals( res['OK'], True )  
    res = self.api.deleteResource( 1, 2, 3 )
    self.assertEquals( res['OK'], True )    
    res = self.api.deleteResource( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )    
    res = self.api.deleteResource( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( 1, 2, 3, 4, 5, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( 1, resourceType = 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( 1, 2, serviceType = 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( 1, 2, 3,
                                   siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteResource( 1, 2, 3, 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getResourcePresent_nok( self ):
    
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                       13, 14, resourceName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                       13, 14, resourceName = 1, siteName = 2, serviceType = 3,
                                       gridSiteName = 4, siteType = 5, resourceType = 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], False )
    
        
  def test_getResourcePresent_ok( self ):
    
    res = self.api.getResourcePresent( )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                       13, 14 )
    self.assertEquals( res['OK'], True )           
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                       13, 14, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( resourceName = 1, siteName = 2, serviceType = 3,
                                       gridSiteName = 4, siteType = 5, resourceType = 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( resourceName = 1, siteName = 2, serviceType = 3,
                                       gridSiteName = 4, siteType = 5, resourceType = 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14, a= 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, siteName = 2, serviceType = 3,
                                       gridSiteName = 4, siteType = 5, resourceType = 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )        
    res = self.api.getResourcePresent( 1, 2, serviceType = 3,
                                       gridSiteName = 4, siteType = 5, resourceType = 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3,
                                       gridSiteName = 4, siteType = 5, resourceType = 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, siteType = 5, resourceType = 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, resourceType = 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6,
                                       statusType = 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, status = 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, dateEffective = 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 
                                       reason = 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, lastCheckTime = 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, tokenOwner = 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                       tokenExpiration = 13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )
    res = self.api.getResourcePresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                       13, formerStatus = 14 )
    self.assertEquals( res['OK'], True )  
            
################################################################################
    
  def test_insertStorageElement_nok( self ):
    
    res = self.api.insertStorageElement( storageElementName = 1, resourceName = 2,
                                         gridSiteName = 3 )     
    self.assertEquals( res['OK'], False )
    res = self.api.insertStorageElement( storageElementName = 1, resourceName = 2,
                                         gridSiteName = 3, a = 'a' )     
    self.assertEquals( res['OK'], False )
    res = self.api.insertStorageElement( 1, resourceName = 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], False )
    res = self.api.insertStorageElement( 1, 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], False )
    
  def test_insertStorageElement_ok( self ):
      
    res = self.api.insertStorageElement( 1, 2, 3 )     
    self.assertEquals( res['OK'], True )
    res = self.api.insertStorageElement( 1, 2, 3, a = 'a' )     
    self.assertEquals( res['OK'], True )
    res = self.api.insertStorageElement( 1, 2, 3, storageElementName = 1 )     
    self.assertEquals( res['OK'], True )
    res = self.api.insertStorageElement( 1, 2, 3, storageElementName = 1, 
                                         resourceName = 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], True )
    
################################################################################

  def test_updateStorageElement_nok( self ):
    
    res = self.api.updateStorageElement( storageElementName = 1, resourceName = 2,
                                         gridSiteName = 3 )     
    self.assertEquals( res['OK'], False )
    res = self.api.updateStorageElement( storageElementName = 1, resourceName = 2,
                                         gridSiteName = 3, a = 'a' )     
    self.assertEquals( res['OK'], False )
    res = self.api.updateStorageElement( 1, resourceName = 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], False )
    res = self.api.updateStorageElement( 1, 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], False )
    
  def test_updateStorageElement_ok( self ):
      
    res = self.api.updateStorageElement( 1, 2, 3 )     
    self.assertEquals( res['OK'], True )
    res = self.api.updateStorageElement( 1, 2, 3, a = 'a' )     
    self.assertEquals( res['OK'], True )
    res = self.api.updateStorageElement( 1, 2, 3, storageElementName = 1 )     
    self.assertEquals( res['OK'], True )
    res = self.api.updateStorageElement( 1, 2, 3, storageElementName = 1, 
                                         resourceName = 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], True )    

################################################################################

  def test_getStorageElement_nok( self ):
    res = self.api.getStorageElement( 1, 2, 3, storageElementName = 1 )     
    self.assertEquals( res['OK'], False )
    res = self.api.getStorageElement( 1, 2, 3, storageElementName = 1, 
                                         resourceName = 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], False )    
        
  def test_getStorageElement_ok( self ):

    res = self.api.getStorageElement( )     
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElement( 1 )     
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElement( 1, 2 )     
    self.assertEquals( res['OK'], True )      
    res = self.api.getStorageElement( 1, 2, 3 )     
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElement( 1, 2, 3, a = 'a' )     
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElement( storageElementName = 1, resourceName = 2,
                                         gridSiteName = 3 )     
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElement( storageElementName = 1, resourceName = 2,
                                         gridSiteName = 3, a = 'a' )     
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElement( 1, resourceName = 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElement( 1, 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], True )
    
################################################################################
    
  def test_deleteStorageElement_nok( self ):
    
    res = self.api.deleteStorageElement( 1, 2, 3, storageElementName = 1 )     
    self.assertEquals( res['OK'], False )
    res = self.api.deleteStorageElement( 1, 2, 3, storageElementName = 1, 
                                         resourceName = 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], False )    
        
  def test_deleteStorageElement_ok( self ):
    
    res = self.api.deleteStorageElement( )     
    self.assertEquals( res['OK'], True )
    res = self.api.deleteStorageElement( 1 )     
    self.assertEquals( res['OK'], True )
    res = self.api.deleteStorageElement( 1, 2 )     
    self.assertEquals( res['OK'], True )  
    res = self.api.deleteStorageElement( 1, 2, 3 )     
    self.assertEquals( res['OK'], True )
    res = self.api.deleteStorageElement( 1, 2, 3, a = 'a' )     
    self.assertEquals( res['OK'], True )
    res = self.api.deleteStorageElement( storageElementName = 1, resourceName = 2,
                                         gridSiteName = 3 )     
    self.assertEquals( res['OK'], True )
    res = self.api.deleteStorageElement( storageElementName = 1, resourceName = 2,
                                         gridSiteName = 3, a = 'a' )     
    self.assertEquals( res['OK'], True )
    res = self.api.deleteStorageElement( 1, resourceName = 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], True )
    res = self.api.deleteStorageElement( 1, 2, gridSiteName = 3 )     
    self.assertEquals( res['OK'], True )
               
################################################################################

  def test_getStorageElementPresent_nok( self ):
    
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, storageElementName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, storageElementName = 1, resourceName = 2,
                                             gridSiteName = 3, siteType = 4, statusType = 5,
                                             status = 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], False )
  
  def test_getStorageElementPresent_ok( self ):
    
    res = self.api.getStorageElementPresent( storageElementName = 1, resourceName = 2,
                                             gridSiteName = 3, siteType = 4, statusType = 5,
                                             status = 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12)
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( storageElementName = 1, resourceName = 2,
                                             gridSiteName = 3, siteType = 4, statusType = 5,
                                             status = 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12, a = 'a')
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, resourceName = 2,
                                             gridSiteName = 3, siteType = 4, statusType = 5,
                                             status = 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2,
                                             gridSiteName = 3, siteType = 4, statusType = 5,
                                             status = 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, siteType = 4, statusType = 5,
                                             status = 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, statusType = 5,
                                             status = 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5,
                                             status = 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, dateEffective = 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, reason = 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8,
                                             lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, tokenOwner = 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11,
                                             formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, formerStatus = 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementPresent( )
    self.assertEquals( res['OK'], True )
    
################################################################################
    
  def test_insertGridSite_nok( self ):
    
    res = self.api.insertGridSite( gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.insertGridSite( 1, gridTier = 2 )
    self.assertEquals( res['OK'], False )          

  def test_insertGridSite_ok( self ):
    
    res = self.api.insertGridSite( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertGridSite( 1, 2, a = 'a')
    self.assertEquals( res['OK'], True )
    res = self.api.insertGridSite( 1, 2, gridSiteName = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertGridSite( 1, 2, gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_updateGridSite_nok( self ):
    
    res = self.api.updateGridSite( gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.updateGridSite( 1, gridTier = 2 )
    self.assertEquals( res['OK'], False )          

  def test_updateGridSite_ok( self ):
    
    res = self.api.updateGridSite( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateGridSite( 1, 2, a = 'a')
    self.assertEquals( res['OK'], True )
    res = self.api.updateGridSite( 1, 2, gridSiteName = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateGridSite( 1, 2, gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getGridSite_nok( self ):       

    res = self.api.getGridSite( 1, 2, gridSiteName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getGridSite( 1, 2, gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], False )

  def test_getGridSite_ok( self ):
    
    res = self.api.getGridSite( )
    self.assertEquals( res['OK'], True )
    res = self.api.getGridSite( 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getGridSite( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getGridSite( 1, 2, a = 'a')
    self.assertEquals( res['OK'], True )
    res = self.api.getGridSite( gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getGridSite( 1, gridTier = 2 )
    self.assertEquals( res['OK'], True ) 
    
################################################################################

  def test_deleteGridSite_nok( self ):       

    res = self.api.deleteGridSite( 1, 2, gridSiteName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteGridSite( 1, 2, gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], False )

  def test_deleteGridSite_ok( self ):
    
    res = self.api.deleteGridSite( )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteGridSite( 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteGridSite( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteGridSite( 1, 2, a = 'a')
    self.assertEquals( res['OK'], True )
    res = self.api.deleteGridSite( gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteGridSite( 1, gridTier = 2 )
    self.assertEquals( res['OK'], True ) 


################################################################################

  def test_insertElementStatus_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.insertElementStatus( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
  
  def test_insertElementStatus_ok( self ):
    
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_updateElementStatus_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.updateElementStatus( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
  
  def test_updateElementStatus_ok( self ):
    
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
        
################################################################################

  def test_getElementStatus_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )  
    res = self.api.getElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.getElementStatus( )
    self.assertEquals( res['OK'], False )
  
  def test_getElementStatus_ok( self ):
    
    res = self.api.getElementStatus( '1', elementName = 2, statusType = 3,
                                     status = 4, reason = 5, dateCreated = 6, 
                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    #This is not a bug !
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )    
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )    
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )    
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )    
    res = self.api.getElementStatus( '1', 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )    
    res = self.api.getElementStatus( '1', 2, 3, 4 )
    self.assertEquals( res['OK'], True )    
    res = self.api.getElementStatus( '1', 2, 3 )
    self.assertEquals( res['OK'], True )    
    res = self.api.getElementStatus( '1', 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1' )
    self.assertEquals( res['OK'], True )        
                       
################################################################################

  def test_deleteElementStatus_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )  
    res = self.api.deleteElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteElementStatus( )
    self.assertEquals( res['OK'], False )
  
  def test_deleteElementStatus_ok( self ):
    
    res = self.api.deleteElementStatus( '1', elementName = 2, statusType = 3,
                                     status = 4, reason = 5, dateCreated = 6, 
                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    #This is not a bug !
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1' )
    self.assertEquals( res['OK'], True )
################################################################################
    
  def test_insertElementScheduledStatus_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.insertElementScheduledStatus( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementScheduledStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
  
  def test_insertElementScheduledStatus_ok( self ):
    
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )       

################################################################################

  def test_updateElementScheduledStatus_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.updateElementScheduledStatus( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementScheduledStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
  
  def test_updateElementScheduledStatus_ok( self ):
    
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
            
################################################################################

  def test_getElementScheduledStatus_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )  
    res = self.api.getElementScheduledStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.getElementScheduledStatus( )
    self.assertEquals( res['OK'], False )
  
  def test_getElementScheduledStatus_ok( self ):
    
    res = self.api.getElementScheduledStatus( '1', elementName = 2, statusType = 3,
                                     status = 4, reason = 5, dateCreated = 6, 
                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    #This is not a bug !
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1', 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementScheduledStatus( '1' )
    self.assertEquals( res['OK'], True )  
    
################################################################################

  def test_deleteElementScheduledStatus_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )  
    res = self.api.deleteElementScheduledStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteElementScheduledStatus( )
    self.assertEquals( res['OK'], False )
  
  def test_deleteElementScheduledStatus_ok( self ):
    
    res = self.api.deleteElementScheduledStatus( '1', elementName = 2, statusType = 3,
                                     status = 4, reason = 5, dateCreated = 6, 
                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    #This is not a bug !
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1', 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementScheduledStatus( '1' )
    self.assertEquals( res['OK'], True )
    

################################################################################

  def test_insertElementHistory_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.insertElementHistory( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertElementHistory( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
  
  def test_insertElementHistory_ok( self ):
    
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )        
################################################################################

  def test_updateElementHistory_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.updateElementHistory( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( element = '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateElementHistory( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
  
  def test_updateElementHistory_ok( self ):
    
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
        
################################################################################ 

  def test_getElementHistory_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )  
    res = self.api.getElementHistory( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.getElementHistory( )
    self.assertEquals( res['OK'], False )
  
  def test_getElementHistory_ok( self ):
    
    res = self.api.getElementHistory( '1', elementName = 2, statusType = 3,
                                     status = 4, reason = 5, dateCreated = 6, 
                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    #This is not a bug !
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1', 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getElementHistory( '1' )
    self.assertEquals( res['OK'], True )
    
################################################################################

  def test_deleteElementHistory_nok( self ):
    
    # element must be string, otherwise the decorator returns S_ERROR
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
                                        elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )  
    res = self.api.deleteElementHistory( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteElementHistory( )
    self.assertEquals( res['OK'], False )
  
  def test_deleteElementHistory_ok( self ):
    
    res = self.api.deleteElementHistory( '1', elementName = 2, statusType = 3,
                                     status = 4, reason = 5, dateCreated = 6, 
                                     dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                     tokenOwner = 10, tokenExpiration = 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, reason = 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementStatus( '1', 2, 3, 4, 5, dateCreated = 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 
                                        dateEffective = 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, dateEnd = 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9,
                                        tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    #This is not a bug !
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, element = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1', 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteElementHistory( '1' )
    self.assertEquals( res['OK'], True )
    
################################################################################

  def test_getValidElements_nok( self ):
    pass
  
  def test_getValidElements_ok( self ):
    
    res = self.api.getValidElements()
    self.assertEquals( res['OK'], True )

################################################################################   

  def test_getValidStatuses_nok( self ):
    pass
  
  def test_getValidStatuses_ok( self ):
    
    res = self.api.getValidStatuses()
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getValidStatusTypes_nok( self ):
    pass
  
  def test_getValidStatusTypes_ok( self ):
    
    res = self.api.getValidStatusTypes()
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getValidSiteTypes_nok( self ):
    pass
  
  def test_getValidSiteTypes_ok( self ):
    
    res = self.api.getValidSiteTypes()
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getValidServiceTypes_nok( self ):
    pass
  
  def test_getValidServiceTypes_ok( self ):
    
    res = self.api.getValidServiceTypes()
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getValidResourceTypes_nok( self ):
    pass
  
  def test_getValidResourceTypes_ok( self ):
    
    res = self.api.getValidResourceTypes()
    self.assertEquals( res['OK'], True )

################################################################################

  def test_addOrModifySite_nok( self ):
    
    res = self.api.addOrModifySite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.addOrModifySite( 1, 2, 3, a = 'a' )
    self.assertEquals( res['OK'], False )

  def test_addOrModifySite_ok( self ):
    
    res = self.api.addOrModifySite( siteName = 1, siteType = 2, gridSiteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifySite( 1, siteType = 2, gridSiteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifySite( 1, 2, gridSiteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifySite( 1, 2, 3 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_addOrModifyService_nok( self ):

    res = self.api.addOrModifyService( serviceName = 1, serviceType = 2, siteName = 3, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.addOrModifyService( 1, 2, 3, a = 'a' )
    self.assertEquals( res['OK'], False )

  def test_addOrModifyService_ok( self ):
    
    res = self.api.addOrModifyService( serviceName = 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyService( 1, serviceType = 2, siteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyService( 1, 2, siteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyService( 1, 2, 3 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_addOrModifyResource_nok( self ):

    res = self.api.addOrModifyResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                        siteName = 4, gridSiteName = 5, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.addOrModifyResource( 1, 2, 3, 4, 5, a = 'a' )
    self.assertEquals( res['OK'], False )

  def test_addOrModifyResource_ok( self ):
    
    res = self.api.addOrModifyResource( resourceName = 1, resourceType = 2, serviceType = 3,
                                        siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyResource( 1, resourceType = 2, serviceType = 3,
                                        siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyResource( 1, 2, serviceType = 3,
                                        siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyResource( 1, 2, 3, siteName = 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyResource( 1, 2, 3, 4, gridSiteName = 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyResource( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_addOrModifyStorageElement_nok( self ):

    res = self.api.addOrModifyStorageElement( storageElementName = 1, resourceName = 2, 
                                              gridSiteName = 3, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.addOrModifyStorageElement( 1, 2, 3, a = 'a' )
    self.assertEquals( res['OK'], False )

  def test_addOrModifyStorageElement_ok( self ):
    
    res = self.api.addOrModifyStorageElement( storageElementName = 1, resourceName = 2, gridSiteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyStorageElement( 1, resourceName = 2, gridSiteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyStorageElement( 1, 2, gridSiteName = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyStorageElement( 1, 2, 3 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_addOrModifyGridSite_nok( self ):
    
    res = self.api.addOrModifyGridSite( gridSiteName = 1, gridTier = 2, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.addOrModifyGridSite( 1, 2, a = 'a' )
    self.assertEquals( res['OK'], False )
    
  def test_addOrModifyGridSite_ok( self ):
    
    res = self.api.addOrModifyGridSite( gridSiteName = 1, gridTier = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyGridSite( 1, gridTier = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.addOrModifyGridSite( 1, 2 )
    self.assertEquals( res['OK'], True )  
    
################################################################################
        
  def test_modifyElementStatus_nok( self ):
    
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 2 )
    self.assertEquals( res['OK'], False )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], False )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.modifyElementStatus( 1, 2 )
    self.assertEquals( res['OK'], False )
    res = self.api.modifyElementStatus( 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.modifyElementStatus( )
    self.assertEquals( res['OK'], False )
      
  def test_modifyElementStatus_ok( self ):
  
    res = self.api.modifyElementStatus( element = 1, elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )      
    res = self.api.modifyElementStatus( 1, elementName = 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, statusType = 3,
                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3,
                                        status = 4, reason = 5, dateCreated = 6, dateEffective = 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, reason = 5, dateCreated = 6, dateEffective = 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, dateCreated = 6, dateEffective = 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, dateEffective = 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7,
                                        dateEnd = 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, lastCheckTime = 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, tokenOwner = 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, tokenExpiration = 11 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 )
    self.assertEquals( res['OK'], True )   
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4, 5 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.modifyElementStatus( 1, 2, 3 )
    self.assertEquals( res['OK'], True )          
        
################################################################################

  def test_removeElement_nok( self ):
    
    res = self.api.removeElement( 1, 2, element = 1)
    self.assertEquals( res['OK'], False )
    res = self.api.removeElement( 1, 2, element = 1, elementName = 2 )
    self.assertEquals( res['OK'], False )
    res = self.api.removeElement( 1, 2, a = 'a')
    self.assertEquals( res['OK'], False )
  
  def test_removeElement_ok( self ):
    
    res = self.api.removeElement( element = 1, elementName = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.removeElement( 1, elementName = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.removeElement( 1, 2 )
    self.assertEquals( res['OK'], True )
    
################################################################################    

  def test_getServiceStats_nok( self ):
    
    res = self.api.getServiceStats( 1, 2, siteName = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getServiceStats( 1, 2, siteName = 1, statusType = 2 )
    self.assertEquals( res['OK'], False )
    res = self.api.getServiceStats( 1, 2, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.getServiceStats( )
    self.assertEquals( res['OK'], False )
  
  def test_getServiceStats_ok( self ):
    
    res = self.api.getServiceStats( siteName = 1, statusType = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServiceStats( 1, statusType = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServiceStats( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getServiceStats( 1 )
    self.assertEquals( res['OK'], True )
    
################################################################################

  def test_getStorageElementStats_nok( self ):
    
    res = self.api.getStorageElementStats( 1, 2, 3, element = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getStorageElementStats( 1, 2, 3, element = 1, name = 2, statusType = 3 )
    self.assertEquals( res['OK'], False )
    res = self.api.getStorageElementStats( 1, 2, 3, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.getStorageElementStats( 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getStorageElementStats( )
    self.assertEquals( res['OK'], False )

  def test_getStorageElementStats_ok( self ):
    
    res = self.api.getStorageElementStats( element = 1, name = 2, statusType = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementStats( 1, name = 2, statusType = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementStats( 1, 2, statusType = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementStats( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStorageElementStats( 1, 2 )
    self.assertEquals( res['OK'], True )
    
################################################################################ 

  def test_getGeneralName_nok( self ):
    
    res = self.api.getGeneralName( 1, 2, 3, from_element = 1 )
    self.api.getGeneralName( res['OK'], False )
    res = self.api.getGeneralName( 1, 2, 3, from_element = 1, name = 2, to_element = 3 )
    self.api.getGeneralName( res['OK'], False )
    res = self.api.getGeneralName( 1, 2, 3, a = 'a' )
    self.api.getGeneralName( res['OK'], False )

  def test_getGeneralName_ok( self ):
  
    res = self.api.getGeneralName( from_element = 1, name = 2, to_element = 3 )
    self.api.getGeneralName( res['OK'], True )
    res = self.api.getGeneralName( 1, name = 2, to_element = 3 )
    self.api.getGeneralName( res['OK'], True )
    res = self.api.getGeneralName( 1, 2, to_element = 3 )
    self.api.getGeneralName( res['OK'], True )
    res = self.api.getGeneralName( 1, 2, 3 )
    self.api.getGeneralName( res['OK'], True )
  
################################################################################           

  def test_getGridSiteName_nok( self ):
    
    res = self.api.getGridSiteName( 1, 2, granularity = 1 )
    self.assertEquals( res['OK'], False ) 
    res = self.api.getGridSiteName( 1, 2, granularity = 1, name = 2 )
    self.assertEquals( res['OK'], False )
    res = self.api.getGridSiteName( 1, 2, a = 'a' )
    self.assertEquals( res['OK'], False )

  def test_getGridSiteName_ok( self ):
    
    res = self.api.getGridSiteName( granularity = 1, name = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getGridSiteName( 1, name = 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getGridSiteName( 1, 2 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getTokens_nok( self ):
    
    res = self.api.getTokens( 1, 2, 3, 4, granularity = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getTokens( 1, 2, 3, 4, granularity = 1, name = 2, tokenExpiration = 3, statusType = 4  )
    self.assertEquals( res['OK'], False )
    res = self.api.getTokens( )
    self.assertEquals( res['OK'], False )
    
  def test_getTokens_ok( self ):
      
    res = self.api.getTokens( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )  
    res = self.api.getTokens( 1, 2, 3, 4, a = 'a'  )
    self.assertEquals( res['OK'], True )
    res = self.api.getTokens( 1, 2, 3, statusType = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getTokens( 1, 2, tokenExpiration = 3, statusType = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getTokens( 1, name = 2, tokenExpiration = 3, statusType = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getTokens( granularity = 1, name = 2, tokenExpiration = 3, statusType = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.getTokens( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getTokens( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getTokens( 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.getTokens( granularity = 1 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_setToken_nok( self ):
    
    res = self.api.setToken( 1, 2, 3, 4, 5, 6, granularity = 1)
    self.assertEquals( res['OK'], False )
    res = self.api.setToken( 1, 2, 3, 4, 5, 6, granularity = 1, name = 2, 
                             statusType = 3, reason = 4, tokenOwner = 5, tokenExpiration = 6)
    self.assertEquals( res['OK'], False )
    res = self.api.setToken( 1, 2, 3, 4, 5, 6, a = 'a')
    self.assertEquals( res['OK'], False )
    res = self.api.setToken( )
    self.assertEquals( res['OK'], False )

  def test_setToken_ok( self ):
    
    res = self.api.setToken( 1, 2, 3, 4, 5, 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.setToken( 1, 2, 3, 4, 5, tokenExpiration = 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.setToken( 1, 2, 3, 4, tokenOwner = 5, tokenExpiration = 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.setToken( 1, 2, 3, reason = 4, tokenOwner = 5, tokenExpiration = 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.setToken( 1, 2, statusType = 3, reason = 4, tokenOwner = 5, tokenExpiration = 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.setToken( 1, name = 2, statusType = 3, reason = 4, tokenOwner = 5, tokenExpiration = 6 )
    self.assertEquals( res['OK'], True )
    res = self.api.setToken( granularity = 1, name = 2, statusType = 3, reason = 4, tokenOwner = 5, tokenExpiration = 6 )
    self.assertEquals( res['OK'], True )

################################################################################   

  def test_setReason_nok( self ):
    
    res = self.api.setReason( 1, 2, 3, 4, granularity = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.setReason( 1, 2, 3, 4, granularity = 1, name = 2, statusType = 3,
                              reason = 4 )
    self.assertEquals( res['OK'], False )
    res = self.api.setReason( 1, 2, 3, 4, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.setReason( )
    self.assertEquals( res['OK'], False )
    
  def test_setReason_ok( self ):
    
    res = self.api.setReason( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.setReason( 1, 2, 3, reason = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.setReason( 1, 2, statusType = 3, reason = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.setReason( 1, name = 2, statusType = 3, reason = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.setReason( granularity = 1, name = 2, statusType = 3, reason = 4 )
    self.assertEquals( res['OK'], True )

################################################################################               

  def test_setDateEnd_nok( self ):
    
    res = self.api.setDateEnd( 1, 2, 3, 4, granularity = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.setDateEnd( 1, 2, 3, 4, granularity = 1, name = 2, statusType = 3, dateEffective = 4 )
    self.assertEquals( res['OK'], False )
    res = self.api.setDateEnd( 1, 2, 3, 4, a = 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.setDateEnd( )
    self.assertEquals( res['OK'], False )
    
  def test_setDateEnd_ok( self ):
    
    res = self.api.setDateEnd( 1, 2, 3, 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.setDateEnd( 1, 2, 3, dateEffective = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.setDateEnd( 1, 2, statusType = 3, dateEffective = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.setDateEnd( 1, name = 2, statusType = 3, dateEffective = 4 )
    self.assertEquals( res['OK'], True )
    res = self.api.setDateEnd( granularity = 1, name = 2, statusType = 3, dateEffective = 4 )
    self.assertEquals( res['OK'], True )     
    
################################################################################  
      
  def test_whatIs_nok( self ):
    
    res = self.api.whatIs( 1, name = 1 )
    self.assertEquals( res['OK'], False )      
    res = self.api.whatIs( )
    self.assertEquals( res['OK'], False )
      
  def test_whatIs_ok( self ):
    
    res = self.api.whatIs( 1 )
    self.assertEquals( res['OK'], True )    
      
################################################################################   
      
  def test_getStuffToCheck_nok( self ):
    
    res = self.api.getStuffToCheck( 1, 2, granularity = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getStuffToCheck( 1, 2, granularity = 1, checkFrequency = 2 )
    self.assertEquals( res['OK'], False )      
    
  def test_getStuffToCheck_ok( self ):
    
    res = self.api.getStuffToCheck( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.api.getStuffToCheck( 1, 2, a = 'a' )
    self.assertEquals( res['OK'], True )
      
################################################################################   
      
  def test_getMonitoredStatus_nok( self ):
    
    res = self.api.getMonitoredStatus( 1, 2, granularity = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getMonitoredStatus( 1, 2, granularity = 1, name = 2 )
    self.assertEquals( res['OK'], False )
    res = self.api.getMonitoredStatus( 1, 2, a= 'a' )
    self.assertEquals( res['OK'], False )
    res = self.api.getMonitoredStatus( )
    self.assertEquals( res['OK'], False )        
      
  def test_getMonitoredStatus_ok( self ):
    
    res = self.api.getMonitoredStatus( 1, 2 )
    self.assertEquals( res['OK'], True )    
      
################################################################################   
            
  def test_getMonitoredsStatusWeb_nok( self ):
    
    res = self.api.getMonitoredsStatusWeb( 1, 2, 3, 4, granularity = 1 )
    self.assertEqual( res['OK'], False )
    res = self.api.getMonitoredsStatusWeb( 1, 2, 3, 4, granularity = 1, selectDict = 2,
                                           startItem = 3, maxItems = 4 )
    self.assertEqual( res['OK'], False )          
    res = self.api.getMonitoredsStatusWeb( 1, 2, 3, 4, a = 'a' )
    self.assertEqual( res['OK'], False )
    res = self.api.getMonitoredsStatusWeb( )
    self.assertEqual( res['OK'], False )
    
  def test_getMonitoredsStatusWeb_ok( self ):
    
    res = self.api.getMonitoredsStatusWeb( 1, 2, 3, 4 )
    self.assertEqual( res['OK'], True ) 
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF