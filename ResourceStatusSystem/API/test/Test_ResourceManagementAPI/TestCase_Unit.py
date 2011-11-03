import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):
  
  def test_insertEnvironmentCache_nok( self ):

    res = self.api.insertEnvironmentCache( hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertEnvironmentCache( hashEnv = 1, siteName = 2, environment = 3, a = 'a' )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertEnvironmentCache( 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertEnvironmentCache( 1, 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_insertEnvironmentCache_ok( self ):
    
    res = self.api.insertEnvironmentCache( 1, 2, 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.insertEnvironmentCache( 1, 2, 3, a = 'a' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.insertEnvironmentCache( 1, 2, 3, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.insertEnvironmentCache( 1, 2, 3, hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )

################################################################################
    
  def test_updateEnvironmentCache_nok( self ):
    
    res = self.api.updateEnvironmentCache( hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updateEnvironmentCache( hashEnv = 1, siteName = 2, environment = 3, a = 'a' )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updateEnvironmentCache( 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updateEnvironmentCache( 1, 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )

  def test_updateEnvironmentCache_ok( self ):
    
    res = self.api.updateEnvironmentCache( 1, 2, 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.updateEnvironmentCache( 1, 2, 3, a = 'a' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.updateEnvironmentCache( 1, 2, 3, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.updateEnvironmentCache( 1, 2, 3, hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )

################################################################################
    
  def test_getEnvironmentCache_nok( self ):
    
    res = self.api.getEnvironmentCache( '1', '2', '3', a = 'a', environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.getEnvironmentCache( '1', '2', '3', a = 'a', siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.getEnvironmentCache( '1', '2', '3', hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )

  def test_getEnvironmentCache_ok( self ):

    res = self.api.getEnvironmentCache( siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getEnvironmentCache( '1', '2', a = 'a', environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getEnvironmentCache( '1', a = 'a', siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getEnvironmentCache( hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getEnvironmentCache( a = 'a', hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getEnvironmentCache( '1', '2', '3' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getEnvironmentCache( '1', '2', '3', a = 'a', b = 'b' )
    self.assertEqual( res[ 'OK' ], True )
    
################################################################################
    
  def test_deleteEnvironmentCache_nok( self ):
    
    res = self.api.deleteEnvironmentCache( '1', '2', '3', a = 'a', environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.deleteEnvironmentCache( '1', '2', '3', a = 'a', siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.deleteEnvironmentCache( '1', '2', '3', hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], False )

  def test_deleteEnvironmentCache_ok( self ):

    res = self.api.deleteEnvironmentCache( siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deleteEnvironmentCache( '1', '2', a = 'a', environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deleteEnvironmentCache( '1', a = 'a', siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deleteEnvironmentCache( hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deleteEnvironmentCache( a = 'a', hashEnv = 1, siteName = 2, environment = 3 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deleteEnvironmentCache( '1', '2', '3' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deleteEnvironmentCache( '1', '2', '3', a = 'a', b = 'b' )
    self.assertEqual( res[ 'OK' ], True )

################################################################################
    
  def test_insertPolicyResult_nok( self ):
    
    res = self.api.insertPolicyResult( granularity = 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertPolicyResult( granularity = 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8, a = 'a' )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertPolicyResult( 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertPolicyResult( 1, 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertPolicyResult( 1, 2, 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertPolicyResult( 1, 2, 3, 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertPolicyResult( 1, 2, 3, 4, 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertPolicyResult( 1, 2, 3, 4, 5, 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.insertPolicyResult( 1, 2, 3, 4, 5, 6, 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    
  def test_insertPolicyResult_ok( self ):
      
    res = self.api.insertPolicyResult( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEqual( res[ 'OK' ], True )  
    res = self.api.insertPolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, a = 'a' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.insertPolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.insertPolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1, 
                                       name = 2, policyName = 3, statusType = 4, 
                                       status = 5, reason = 6, dateEffective = 7, 
                                       lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], True )
    
################################################################################
    
  def test_updatePolicyResult_nok( self ):
    
    res = self.api.updatePolicyResult( granularity = 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updatePolicyResult( granularity = 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8, a = 'a' )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updatePolicyResult( 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8  )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updatePolicyResult( 1, 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updatePolicyResult( 1, 2, 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updatePolicyResult( 1, 2, 3, 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updatePolicyResult( 1, 2, 3, 4, 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updatePolicyResult( 1, 2, 3, 4, 5, 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.updatePolicyResult( 1, 2, 3, 4, 5, 6, 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], False )
    
  def test_updatePolicyResult_ok( self ):
      
    res = self.api.updatePolicyResult( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEqual( res[ 'OK' ], True )  
    res = self.api.updatePolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, a = 'a' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.updatePolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.updatePolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1, 
                                       name = 2, policyName = 3, statusType = 4, 
                                       status = 5, reason = 6, dateEffective = 7, 
                                       lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], True )

################################################################################

  def test_getPolicyResult_nok( self ):

    res = self.api.getPolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.getPolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1, 
                                       name = 2, policyName = 3, statusType = 4, 
                                       status = 5, reason = 6, dateEffective = 7, 
                                       lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_getPolicyResult_ok( self ):
      
    res = self.api.getPolicyResult( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEqual( res[ 'OK' ], True )  
    res = self.api.getPolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, a = 'a' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( granularity = 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( granularity = 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8, a = 'a' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( 1, 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( 1, 2, 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( 1, 2, 3, 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( 1, 2, 3, 4, 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( 1, 2, 3, 4, 5, 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.getPolicyResult( 1, 2, 3, 4, 5, 6, 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    
################################################################################

  def test_deletePolicyResult_nok( self ):

    res = self.api.deletePolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1 )
    self.assertEqual( res[ 'OK' ], False )
    res = self.api.deletePolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, granularity = 1, 
                                       name = 2, policyName = 3, statusType = 4, 
                                       status = 5, reason = 6, dateEffective = 7, 
                                       lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_deletePolicyResult_ok( self ):
      
    res = self.api.deletePolicyResult( 1, 2, 3, 4, 5, 6, 7, 8 )
    self.assertEqual( res[ 'OK' ], True )  
    res = self.api.deletePolicyResult( 1, 2, 3, 4, 5, 6, 7, 8, a = 'a' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( granularity = 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8 )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( granularity = 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8, a = 'a' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( 1, name = 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( 1, 2, policyName = 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( 1, 2, 3,
                                           statusType = 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( 1, 2, 3, 4, status = 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( 1, 2, 3, 4, 5, reason = 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( 1, 2, 3, 4, 5, 6,
                                           dateEffective = 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )
    res = self.api.deletePolicyResult( 1, 2, 3, 4, 5, 6, 7, lastCheckTime = 8)
    self.assertEqual( res[ 'OK' ], True )

################################################################################

  def test_insertClientCache_nok( self ):
    
    res = self.api.insertClientCache( name = 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7 )
    self.assertEquals( res['OK'], False)
    res = self.api.insertClientCache( name = 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7, a= 'a' )
    self.assertEquals( res['OK'], False)
    res = self.api.insertClientCache( 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.insertClientCache( 1, 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.insertClientCache( 1, 2, 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.insertClientCache( 1, 2, 3, 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.insertClientCache( 1, 2, 3, 4, 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.insertClientCache( 1, 2, 3, 4, 5, 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)

  def test_insertClientCache_ok( self ):
    
    res = self.api.insertClientCache( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertClientCache( 1, 2, 3, 4, 5, 6, 7, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.insertClientCache( 1, 2, 3, 4, 5, 6, 7, name = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertClientCache( 1, 2, 3, 4, 5, 6, 7, name = 1, commandName = 2, 
                                      opt_ID = 3, value = 4, result = 5, 
                                      dateEffective = 6, lastCheckTime = 7 )
    self.assertEquals( res['OK'], True )

################################################################################
    
  def test_updateClientCache_nok( self ):
    
    res = self.api.updateClientCache( name = 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7 )
    self.assertEquals( res['OK'], False)
    res = self.api.updateClientCache( name = 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7, a= 'a' )
    self.assertEquals( res['OK'], False)
    res = self.api.updateClientCache( 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.updateClientCache( 1, 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.updateClientCache( 1, 2, 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.updateClientCache( 1, 2, 3, 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.updateClientCache( 1, 2, 3, 4, 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)
    res = self.api.updateClientCache( 1, 2, 3, 4, 5, 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], False)

  def test_updateClientCache_ok( self ):
    
    res = self.api.updateClientCache( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateClientCache( 1, 2, 3, 4, 5, 6, 7, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.updateClientCache( 1, 2, 3, 4, 5, 6, 7, name = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateClientCache( 1, 2, 3, 4, 5, 6, 7, name = 1, commandName = 2, 
                                      opt_ID = 3, value = 4, result = 5, 
                                      dateEffective = 6, lastCheckTime = 7 )
    self.assertEquals( res['OK'], True )    

################################################################################

  def test_getClientCache_nok( self ):

    res = self.api.getClientCache( 1, 2, 3, 4, 5, 6, 7, name = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getClientCache( 1, 2, 3, 4, 5, 6, 7, name = 1, commandName = 2, 
                                      opt_ID = 3, value = 4, result = 5, 
                                      dateEffective = 6, lastCheckTime = 7 )
    self.assertEquals( res['OK'], False ) 
    
  def test_getClientCache_ok( self ):

    res = self.api.getClientCache( name = 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.getClientCache( name = 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7, a= 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getClientCache( 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.getClientCache( 1, 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.getClientCache( 1, 2, 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.getClientCache( 1, 2, 3, 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.getClientCache( 1, 2, 3, 4, 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.getClientCache( 1, 2, 3, 4, 5, 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    
    res = self.api.getClientCache( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.getClientCache( 1, 2, 3, 4, 5, 6, 7, a = 'a' )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_deleteClientCache_nok( self ):

    res = self.api.deleteClientCache( 1, 2, 3, 4, 5, 6, 7, name = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteClientCache( 1, 2, 3, 4, 5, 6, 7, name = 1, commandName = 2, 
                                      opt_ID = 3, value = 4, result = 5, 
                                      dateEffective = 6, lastCheckTime = 7 )
    self.assertEquals( res['OK'], False ) 
    
  def test_deleteClientCache_ok( self ):

    res = self.api.deleteClientCache( name = 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteClientCache( name = 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7, a= 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteClientCache( 1, commandName = 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.deleteClientCache( 1, 2, opt_ID = 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.deleteClientCache( 1, 2, 3, value = 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.deleteClientCache( 1, 2, 3, 4,
                                      result = 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.deleteClientCache( 1, 2, 3, 4, 5, dateEffective = 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    res = self.api.deleteClientCache( 1, 2, 3, 4, 5, 6, lastCheckTime = 7)
    self.assertEquals( res['OK'], True )
    
    res = self.api.deleteClientCache( 1, 2, 3, 4, 5, 6, 7 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteClientCache( 1, 2, 3, 4, 5, 6, 7, a = 'a' )
    self.assertEquals( res['OK'], True )

################################################################################
    
  def test_insertAccountingCache_nok( self ):
    
    res = self.api.insertAccountingCache( name = 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6 )    
    self.assertEquals( res['OK'], False )
    res = self.api.insertAccountingCache( name = 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6,
                                          a = 'a' )    
    self.assertEquals( res['OK'], False )
    res = self.api.insertAccountingCache( 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    res = self.api.insertAccountingCache( 1, 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    res = self.api.insertAccountingCache( 1, 2, 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    res = self.api.insertAccountingCache( 1, 2, 3, 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    res = self.api.insertAccountingCache( 1, 2, 3, 4, 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    
  def test_insertAccountingCache_ok( self ):  
    
    res = self.api.insertAccountingCache( 1, 2, 3, 4, 5, 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.insertAccountingCache( 1, 2, 3, 4, 5, 6 , a = 'a')    
    self.assertEquals( res['OK'], True )
    res = self.api.insertAccountingCache( 1, 2, 3, 4, 5, 6 , name = 1)    
    self.assertEquals( res['OK'], True )
    res = self.api.insertAccountingCache( 1, 2, 3, 4, 5, 6 , name = 1, plotType = 2, 
                                          plotName = 3, result = 4, 
                                          dateEffective = 5, lastCheckTime = 6 )    
    self.assertEquals( res['OK'], True )
    
################################################################################

  def test_updateAccountingCache_nok( self ):
    
    res = self.api.updateAccountingCache( name = 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6 )    
    self.assertEquals( res['OK'], False )
    res = self.api.updateAccountingCache( name = 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6,
                                          a = 'a' )    
    self.assertEquals( res['OK'], False )
    res = self.api.updateAccountingCache( 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    res = self.api.updateAccountingCache( 1, 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    res = self.api.updateAccountingCache( 1, 2, 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    res = self.api.updateAccountingCache( 1, 2, 3, 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    res = self.api.updateAccountingCache( 1, 2, 3, 4, 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], False )
    
  def test_updateAccountingCache_ok( self ):  
    
    res = self.api.updateAccountingCache( 1, 2, 3, 4, 5, 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.updateAccountingCache( 1, 2, 3, 4, 5, 6 , a = 'a')    
    self.assertEquals( res['OK'], True )
    res = self.api.updateAccountingCache( 1, 2, 3, 4, 5, 6 , name = 1)    
    self.assertEquals( res['OK'], True )
    res = self.api.updateAccountingCache( 1, 2, 3, 4, 5, 6 , name = 1, plotType = 2, 
                                          plotName = 3, result = 4, 
                                          dateEffective = 5, lastCheckTime = 6 )    
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getAccountingCache_nok( self ):
    
    res = self.api.getAccountingCache( 1, 2, 3, 4, 5, 6 , name = 1)    
    self.assertEquals( res['OK'], False )
    res = self.api.getAccountingCache( 1, 2, 3, 4, 5, 6 , name = 1, plotType = 2, 
                                          plotName = 3, result = 4, 
                                          dateEffective = 5, lastCheckTime = 6 )    
    self.assertEquals( res['OK'], False )

    
  def test_getAccountingCache_ok( self ):  
    
    res = self.api.getAccountingCache( name = 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6 )    
    self.assertEquals( res['OK'], True )
    res = self.api.getAccountingCache( name = 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6,
                                          a = 'a' )    
    self.assertEquals( res['OK'], True )
    res = self.api.getAccountingCache( 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.getAccountingCache( 1, 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.getAccountingCache( 1, 2, 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.getAccountingCache( 1, 2, 3, 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.getAccountingCache( 1, 2, 3, 4, 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    
    res = self.api.getAccountingCache( 1, 2, 3, 4, 5, 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.getAccountingCache( 1, 2, 3, 4, 5, 6 , a = 'a')    
    self.assertEquals( res['OK'], True )


################################################################################

  def test_deleteAccountingCache_nok( self ):
    
    res = self.api.deleteAccountingCache( 1, 2, 3, 4, 5, 6 , name = 1)    
    self.assertEquals( res['OK'], False )
    res = self.api.deleteAccountingCache( 1, 2, 3, 4, 5, 6 , name = 1, plotType = 2, 
                                          plotName = 3, result = 4, 
                                          dateEffective = 5, lastCheckTime = 6 )    
    self.assertEquals( res['OK'], False )
    
  def test_deleteAccountingCache_ok( self ):  
    
    res = self.api.deleteAccountingCache( name = 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6 )    
    self.assertEquals( res['OK'], True )
    res = self.api.deleteAccountingCache( name = 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6,
                                          a = 'a' )    
    self.assertEquals( res['OK'], True )
    res = self.api.deleteAccountingCache( 1, plotType = 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.deleteAccountingCache( 1, 2, plotName = 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.deleteAccountingCache( 1, 2, 3,
                                          result = 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.deleteAccountingCache( 1, 2, 3, 4, dateEffective = 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.deleteAccountingCache( 1, 2, 3, 4, 5, lastCheckTime = 6)    
    self.assertEquals( res['OK'], True )
    
    res = self.api.deleteAccountingCache( 1, 2, 3, 4, 5, 6)    
    self.assertEquals( res['OK'], True )
    res = self.api.deleteAccountingCache( 1, 2, 3, 4, 5, 6 , a = 'a')    
    self.assertEquals( res['OK'], True )
    
################################################################################
    
  def test_insertUserRegistryCache_nok( self ):
    
    res = self.api.insertUserRegistryCache( login = 1, name = 2, email = 3 )
    self.assertEquals( res['OK'], False )
    res = self.api.insertUserRegistryCache( login = 1, name = 2, email = 3, a = 'a' )
    self.assertEquals( res['OK'], False )    
    res = self.api.insertUserRegistryCache( 1, name = 2, email = 3)
    self.assertEquals( res['OK'], False )    
    res = self.api.insertUserRegistryCache( 1, 2, email = 3)
    self.assertEquals( res['OK'], False )

  def test_insertUserRegistryCache_ok( self ):
    
    res = self.api.insertUserRegistryCache( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertUserRegistryCache( 1, 2, 3, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.insertUserRegistryCache( 1, 2, 3, login = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.insertUserRegistryCache( 1, 2, 3, login = 1, name = 2, email = 3 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_updateUserRegistryCache_nok( self ):
    
    res = self.api.updateUserRegistryCache( login = 1, name = 2, email = 3 )
    self.assertEquals( res['OK'], False )
    res = self.api.updateUserRegistryCache( login = 1, name = 2, email = 3, a = 'a' )
    self.assertEquals( res['OK'], False )    
    res = self.api.updateUserRegistryCache( 1, name = 2, email = 3)
    self.assertEquals( res['OK'], False )    
    res = self.api.updateUserRegistryCache( 1, 2, email = 3)
    self.assertEquals( res['OK'], False )

  def test_updateUserRegistryCache_ok( self ):
    
    res = self.api.updateUserRegistryCache( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateUserRegistryCache( 1, 2, 3, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.updateUserRegistryCache( 1, 2, 3, login = 1 )
    self.assertEquals( res['OK'], True )
    res = self.api.updateUserRegistryCache( 1, 2, 3, login = 1, name = 2, email = 3 )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getUserRegistryCache_nok( self ):
    
    res = self.api.getUserRegistryCache( 1, 2, 3, login = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.getUserRegistryCache( 1, 2, 3, login = 1, name = 2, email = 3 )
    self.assertEquals( res['OK'], False )

  def test_getUserRegistryCache_ok( self ):
    
    res = self.api.getUserRegistryCache( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getUserRegistryCache( 1, 2, 3, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.getUserRegistryCache( login = 1, name = 2, email = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.getUserRegistryCache( login = 1, name = 2, email = 3, a = 'a' )
    self.assertEquals( res['OK'], True )    
    res = self.api.getUserRegistryCache( 1, name = 2, email = 3)
    self.assertEquals( res['OK'], True )    
    res = self.api.getUserRegistryCache( 1, 2, email = 3)
    self.assertEquals( res['OK'], True )

################################################################################

  def test_deleteUserRegistryCache_nok( self ):
    
    res = self.api.deleteUserRegistryCache( 1, 2, 3, login = 1 )
    self.assertEquals( res['OK'], False )
    res = self.api.deleteUserRegistryCache( 1, 2, 3, login = 1, name = 2, email = 3 )
    self.assertEquals( res['OK'], False )

  def test_deleteUserRegistryCache_ok( self ):
    
    res = self.api.deleteUserRegistryCache( 1, 2, 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteUserRegistryCache( 1, 2, 3, a = 'a' )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteUserRegistryCache( login = 1, name = 2, email = 3 )
    self.assertEquals( res['OK'], True )
    res = self.api.deleteUserRegistryCache( login = 1, name = 2, email = 3, a = 'a' )
    self.assertEquals( res['OK'], True )    
    res = self.api.deleteUserRegistryCache( 1, name = 2, email = 3)
    self.assertEquals( res['OK'], True )    
    res = self.api.deleteUserRegistryCache( 1, 2, email = 3)
    self.assertEquals( res['OK'], True )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   