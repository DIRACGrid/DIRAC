#import unittest
#
#class TestCase_Unit( unittest.TestCase ):
#  
#  def test_insertEnvironmentCache_nok( self ):
#
#    f = self.client.insertEnvironmentCache
#
#    res = f( hashEnv = 1, siteName = 2, environment = 3, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, environment = 3 )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, hashEnv = 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], False )
#    
#  def test_insertEnvironmentCache_ok( self ):
#
#    f = self.client.insertEnvironmentCache
#    
#    res = f( hashEnv = 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )    
#    res = f( 1, 2, 3 )
#    self.assertEqual( res[ 'OK' ], True )
#
#################################################################################
#    
#  def test_updateEnvironmentCache_nok( self ):
#
#    f = self.client.updateEnvironmentCache
#
#    res = f( hashEnv = 1, siteName = 2, environment = 3, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, environment = 3 )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, hashEnv = 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], False )
#    
#  def test_updateEnvironmentCache_ok( self ):
#    
#    f = self.client.updateEnvironmentCache
#    
#    res = f( hashEnv = 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )    
#    res = f( 1, 2, 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    
#################################################################################
#    
#  def test_getEnvironmentCache_nok( self ):
#    
#    f = self.client.getEnvironmentCache
#
#    res = f( hashEnv = 1, siteName = 2, environment = 3, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, environment = 3 )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, hashEnv = 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], False )
#
#  def test_getEnvironmentCache_ok( self ):
#
#    f = self.client.getEnvironmentCache
#    
#    res = f( hashEnv = 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )    
#    res = f( 1, 2, 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    
#################################################################################
#    
#  def test_deleteEnvironmentCache_nok( self ):
#    
#    f = self.client.deleteEnvironmentCache
#
#    res = f( hashEnv = 1, siteName = 2, environment = 3, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, environment = 3 )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, hashEnv = 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], False )
#
#  def test_deleteEnvironmentCache_ok( self ):
#
#    f = self.client.deleteEnvironmentCache
#    
#    res = f( hashEnv = 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, siteName = 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, environment = 3 )
#    self.assertEqual( res[ 'OK' ], True )    
#    res = f( 1, 2, 3 )
#    self.assertEqual( res[ 'OK' ], True )
#
#################################################################################
#    
#  def test_insertPolicyResult_nok( self ):
#    
#    f = self.client.insertPolicyResult
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( granularity = 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1 )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1, 
#                                       name = 2, policyName = 3, statusType = 4, 
#                                       status = 5, reason = 6, dateEffective = 7, 
#                                       lastCheckTime = 8 )
#    self.assertEqual( res[ 'OK' ], False )
#    
#  def test_insertPolicyResult_ok( self ):
#    
#    f = self.client.insertPolicyResult  
#    
#    res = f( 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, reason = 6,
#                                           dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, 6,
#                                           dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, 6, 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    
#    res = f( granularity = 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8 )
#    self.assertEqual( res[ 'OK' ], True )      
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEqual( res[ 'OK' ], True )  
#    
#################################################################################
#    
#  def test_updatePolicyResult_nok( self ):
#    
#    f = self.client.updatePolicyResult
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( granularity = 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1 )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1, 
#                                       name = 2, policyName = 3, statusType = 4, 
#                                       status = 5, reason = 6, dateEffective = 7, 
#                                       lastCheckTime = 8 )
#    self.assertEqual( res[ 'OK' ], False )
#    
#  def test_updatePolicyResult_ok( self ):
#      
#    f = self.client.updatePolicyResult
#    
#    res = f( 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, reason = 6,
#                                           dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, 6,
#                                           dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, 6, 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    
#    res = f( granularity = 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8 )
#    self.assertEqual( res[ 'OK' ], True )      
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEqual( res[ 'OK' ], True )  
#
#################################################################################
#
#  def test_getPolicyResult_nok( self ):
#
#    f = self.client.getPolicyResult
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( granularity = 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1 )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1, 
#                                       name = 2, policyName = 3, statusType = 4, 
#                                       status = 5, reason = 6, dateEffective = 7, 
#                                       lastCheckTime = 8 )
#    self.assertEqual( res[ 'OK' ], False )
#    
#  def test_getPolicyResult_ok( self ):
#      
#    f = self.client.getPolicyResult
#    
#    res = f( 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, reason = 6,
#                                           dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, 6,
#                                           dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, 6, 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    
#    res = f( granularity = 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8 )
#    self.assertEqual( res[ 'OK' ], True )      
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEqual( res[ 'OK' ], True )
#    
#################################################################################
#
#  def test_deletePolicyResult_nok( self ):
#
#    f = self.client.deletePolicyResult
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( granularity = 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8, a = 'a' )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1 )
#    self.assertEqual( res[ 'OK' ], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1, 
#                                       name = 2, policyName = 3, statusType = 4, 
#                                       status = 5, reason = 6, dateEffective = 7, 
#                                       lastCheckTime = 8 )
#    self.assertEqual( res[ 'OK' ], False )
#    
#  def test_deletePolicyResult_ok( self ):
#      
#    f = self.client.deletePolicyResult
#    
#    res = f( 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, reason = 6,
#                                           dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, 6,
#                                           dateEffective = 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    res = f( 1, 2, 3, 4, 5, 6, 7, lastCheckTime = 8)
#    self.assertEqual( res[ 'OK' ], True )
#    
#    res = f( granularity = 1, name = 2, policyName = 3,
#             statusType = 4, status = 5, reason = 6,
#             dateEffective = 7, lastCheckTime = 8 )
#    self.assertEqual( res[ 'OK' ], True )      
#    res = f( 1, 2, 3, 4, 5, 6, 7, 8 )
#    self.assertEqual( res[ 'OK' ], True )
#
#################################################################################
#
#  def test_insertClientCache_nok( self ):
#    
#    f = self.client.insertClientCache
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, name = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, name = 1, commandName = 2, 
#             opt_ID = 3, value = 4, result = 5, 
#             dateEffective = 6, lastCheckTime = 7 )
#    self.assertEquals( res['OK'], False )
#    res = f( name = 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7, a= 'a' )
#    self.assertEquals( res['OK'], False)
#    
#  def test_insertClientCache_ok( self ):
#    
#    f = self.client.insertClientCache
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )
#    res = f( name = 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7 )
#    self.assertEquals( res['OK'], True)
#    res = f( 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4, 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4, 5, 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#
#################################################################################
#    
#  def test_updateClientCache_nok( self ):
#     
#    f = self.client.updateClientCache
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, name = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, name = 1, commandName = 2, 
#             opt_ID = 3, value = 4, result = 5, 
#             dateEffective = 6, lastCheckTime = 7 )
#    self.assertEquals( res['OK'], False )
#    res = f( name = 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7, a= 'a' )
#    self.assertEquals( res['OK'], False)
#    
#  def test_updateClientCache_ok( self ):
#    
#    f = self.client.updateClientCache
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )
#    res = f( name = 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7 )
#    self.assertEquals( res['OK'], True)
#    res = f( 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4, 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4, 5, 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#
#################################################################################
#
#  def test_getClientCache_nok( self ):
#
#    f = self.client.getClientCache
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, name = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, name = 1, commandName = 2, 
#             opt_ID = 3, value = 4, result = 5, 
#             dateEffective = 6, lastCheckTime = 7 )
#    self.assertEquals( res['OK'], False )
#    res = f( name = 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7, a= 'a' )
#    self.assertEquals( res['OK'], False)
#        
#  def test_getClientCache_ok( self ):
#
#    f = self.client.getClientCache
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )
#    res = f( name = 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7 )
#    self.assertEquals( res['OK'], True)
#    res = f( 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4, 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4, 5, 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    
#################################################################################
#
#  def test_deleteClientCache_nok( self ):
#
#    f = self.client.deleteClientCache
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, name = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6, 7, name = 1, commandName = 2, 
#             opt_ID = 3, value = 4, result = 5, 
#             dateEffective = 6, lastCheckTime = 7 )
#    self.assertEquals( res['OK'], False )
#    res = f( name = 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7, a= 'a' )
#    self.assertEquals( res['OK'], False)
#        
#  def test_deleteClientCache_ok( self ):
#
#    f = self.client.deleteClientCache
#    
#    res = f( 1, 2, 3, 4, 5, 6, 7 )
#    self.assertEquals( res['OK'], True )
#    res = f( name = 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7 )
#    self.assertEquals( res['OK'], True)
#    res = f( 1, commandName = 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, opt_ID = 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, value = 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4,
#                                      result = 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4, 5, dateEffective = 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    res = f( 1, 2, 3, 4, 5, 6, lastCheckTime = 7)
#    self.assertEquals( res['OK'], True)
#    
#################################################################################
#    
#  def test_insertAccountingCache_nok( self ):
#    
#    f = self.client.insertAccountingCache
#    
#    res = f( name = 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6, a = 'a' )    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , a = 'a')    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , name = 1)    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , name = 1, plotType = 2, 
#             plotName = 3, result = 4, dateEffective = 5, lastCheckTime = 6 )    
#    self.assertEquals( res['OK'], False )
#    
#  def test_insertAccountingCache_ok( self ):  
#    
#    f = self.client.insertAccountingCache
#    
#    res = f( 1, 2, 3, 4, 5, 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( name = 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6 )    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3, 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3, 4, 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_updateAccountingCache_nok( self ):
#    
#    f = self.client.updateAccountingCache
#    
#    res = f( name = 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6, a = 'a' )    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , a = 'a')    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , name = 1)    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , name = 1, plotType = 2, 
#             plotName = 3, result = 4, dateEffective = 5, lastCheckTime = 6 )    
#    self.assertEquals( res['OK'], False )
#    
#  def test_updateAccountingCache_ok( self ):  
#    
#    f = self.client.updateAccountingCache
#    
#    res = f( 1, 2, 3, 4, 5, 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( name = 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6 )    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3, 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3, 4, 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getAccountingCache_nok( self ):
#    
#    f = self.client.getAccountingCache
#    
#    res = f( name = 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6, a = 'a' )    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , a = 'a')    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , name = 1)    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , name = 1, plotType = 2, 
#             plotName = 3, result = 4, dateEffective = 5, lastCheckTime = 6 )    
#    self.assertEquals( res['OK'], False )
#    
#  def test_getAccountingCache_ok( self ):  
#    
#    f = self.client.getAccountingCache
#    
#    res = f( 1, 2, 3, 4, 5, 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( name = 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6 )    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3, 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3, 4, 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_deleteAccountingCache_nok( self ):
#    
#    f = self.client.deleteAccountingCache
#    
#    res = f( name = 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6, a = 'a' )    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , a = 'a')    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , name = 1)    
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, 4, 5, 6 , name = 1, plotType = 2, 
#             plotName = 3, result = 4, dateEffective = 5, lastCheckTime = 6 )    
#    self.assertEquals( res['OK'], False )
#    
#  def test_deleteAccountingCache_ok( self ):  
#    
#    f = self.client.deleteAccountingCache
#    
#    res = f( 1, 2, 3, 4, 5, 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( name = 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6 )    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, plotType = 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, plotName = 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3,
#             result = 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3, 4, dateEffective = 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3, 4, 5, lastCheckTime = 6)    
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#    
#  def test_insertUserRegistryCache_nok( self ):
#    
#    f = self.client.insertUserRegistryCache
#    
#    res = f( login = 1, name = 2, email = 3, a = 'a' )
#    self.assertEquals( res['OK'], False )    
#    res = f( 1, 2, 3, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, login = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, login = 1, name = 2, email = 3 )
#    self.assertEquals( res['OK'], False )
#
#  def test_insertUserRegistryCache_ok( self ):
#    
#    f = self.client.insertUserRegistryCache
#    
#    res = f( login = 1, name = 2, email = 3 )
#    self.assertEquals( res['OK'], True )
#    res = f( 1, name = 2, email = 3)
#    self.assertEquals( res['OK'], True )    
#    res = f( 1, 2, email = 3)
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
#
#  def test_updateUserRegistryCache_nok( self ):
#    
#    f = self.client.updateUserRegistryCache
#    
#    res = f( login = 1, name = 2, email = 3, a = 'a' )
#    self.assertEquals( res['OK'], False )    
#    res = f( 1, 2, 3, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, login = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, login = 1, name = 2, email = 3 )
#    self.assertEquals( res['OK'], False )
#
#  def test_updateUserRegistryCache_ok( self ):
#    
#    f = self.client.updateUserRegistryCache
#    
#    res = f( login = 1, name = 2, email = 3 )
#    self.assertEquals( res['OK'], True )
#    res = f( 1, name = 2, email = 3)
#    self.assertEquals( res['OK'], True )    
#    res = f( 1, 2, email = 3)
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_getUserRegistryCache_nok( self ):
#    
#    f = self.client.getUserRegistryCache
#    
#    res = f( login = 1, name = 2, email = 3, a = 'a' )
#    self.assertEquals( res['OK'], False )    
#    res = f( 1, 2, 3, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, login = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, login = 1, name = 2, email = 3 )
#    self.assertEquals( res['OK'], False )
#
#  def test_getUserRegistryCache_ok( self ):
#    
#    f = self.client.getUserRegistryCache
#    
#    res = f( login = 1, name = 2, email = 3 )
#    self.assertEquals( res['OK'], True )
#    res = f( 1, name = 2, email = 3)
#    self.assertEquals( res['OK'], True )    
#    res = f( 1, 2, email = 3)
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#
#################################################################################
#
#  def test_deleteUserRegistryCache_nok( self ):
#
#    f = self.client.deleteUserRegistryCache
#    
#    res = f( login = 1, name = 2, email = 3, a = 'a' )
#    self.assertEquals( res['OK'], False )    
#    res = f( 1, 2, 3, a = 'a' )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, login = 1 )
#    self.assertEquals( res['OK'], False )
#    res = f( 1, 2, 3, login = 1, name = 2, email = 3 )
#    self.assertEquals( res['OK'], False )
#
#  def test_deleteUserRegistryCache_ok( self ):
#    
#    f = self.client.deleteUserRegistryCache
#    
#    res = f( login = 1, name = 2, email = 3 )
#    self.assertEquals( res['OK'], True )
#    res = f( 1, name = 2, email = 3)
#    self.assertEquals( res['OK'], True )    
#    res = f( 1, 2, email = 3)
#    self.assertEquals( res['OK'], True )
#    res = f( 1, 2, 3 )
#    self.assertEquals( res['OK'], True )
#    
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   