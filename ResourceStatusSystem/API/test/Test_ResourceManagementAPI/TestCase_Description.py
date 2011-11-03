import unittest
import inspect

class TestCase_Description( unittest.TestCase ):
  
  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.api.__init__ )
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )  
  
  def test_insertEnvironmentCache_definition( self ):
    
    ins = inspect.getargspec( self.api.insertEnvironmentCache.f )   
    self.assertEqual( ins.args, [ 'self', 'hashEnv', 'siteName', 'environment' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  
    
  def test_updateEnvironmentCache_definition( self ):
    
    ins = inspect.getargspec( self.api.updateEnvironmentCache.f )   
    self.assertEqual( ins.args, [ 'self', 'hashEnv', 'siteName', 'environment' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )  

  def test_getEnvironmentCache_definition( self ):
    
    ins = inspect.getargspec( self.api.getEnvironmentCache.f )   
    self.assertEqual( ins.args, [ 'self', 'hashEnv', 'siteName', 'environment' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )  
   
  def test_deleteEnvironmentCache_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteEnvironmentCache.f )   
    self.assertEqual( ins.args, [ 'self', 'hashEnv', 'siteName', 'environment' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) )      
  
  def test_insertPolicyResult_definition( self ):
    
    ins = inspect.getargspec( self.api.insertPolicyResult.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'policyName', 'statusType',
                        'status', 'reason', 'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )      

  def test_updatePolicyResult_definition( self ):
    
    ins = inspect.getargspec( self.api.updatePolicyResult.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'policyName', 'statusType',
                        'status', 'reason', 'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )      

  def test_getPolicyResult_definition( self ):
    
    ins = inspect.getargspec( self.api.getPolicyResult.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'policyName', 'statusType',
                        'status', 'reason', 'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None ) )   
 
  def test_deletePolicyResult_definition( self ):
    
    ins = inspect.getargspec( self.api.deletePolicyResult.f )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'policyName', 'statusType',
                        'status', 'reason', 'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None ) )   

  def test_insertClientCache_definition( self ):
    
    ins = inspect.getargspec( self.api.insertClientCache.f )   
    self.assertEqual( ins.args, [ 'self', 'name', 'commandName', 'opt_ID', 'value',
                                 'result', 'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None ) 
   
  def test_updateClientCache_definition( self ):
    
    ins = inspect.getargspec( self.api.updateClientCache.f )   
    self.assertEqual( ins.args, [ 'self', 'name', 'commandName', 'opt_ID', 'value',
                                 'result', 'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None ) 
 
  def test_getClientCache_definition( self ):
    
    ins = inspect.getargspec( self.api.getClientCache.f )   
    self.assertEqual( ins.args, [ 'self', 'name', 'commandName', 'opt_ID', 'value',
                                 'result', 'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None ) ) 
 
  def test_deleteClientCache_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteClientCache.f )   
    self.assertEqual( ins.args, [ 'self', 'name', 'commandName', 'opt_ID', 'value',
                                 'result', 'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None ) ) 
  
  def test_insertAccountingCache_definition( self ):
    
    ins = inspect.getargspec( self.api.insertAccountingCache.f )   
    self.assertEqual( ins.args, [ 'self', 'name', 'plotType', 'plotName', 'result', 
                             'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None ) 
   
  def test_updateAccountingCache_definition( self ):
    
    ins = inspect.getargspec( self.api.updateAccountingCache.f )   
    self.assertEqual( ins.args, [ 'self', 'name', 'plotType', 'plotName', 'result', 
                             'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None ) 
   
  def test_getAccountingCache_definition( self ):
    
    ins = inspect.getargspec( self.api.getAccountingCache.f )   
    self.assertEqual( ins.args, [ 'self', 'name', 'plotType', 'plotName', 'result', 
                             'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None) ) 
   
  def test_deleteAccountingCache_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteAccountingCache.f )   
    self.assertEqual( ins.args, [ 'self', 'name', 'plotType', 'plotName', 'result', 
                             'dateEffective', 'lastCheckTime' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None) ) 
 
  def test_insertUserRegistryCache_definition( self ):
    
    ins = inspect.getargspec( self.api.insertUserRegistryCache.f )   
    self.assertEqual( ins.args, [ 'self', 'login', 'name', 'email' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None ) 
   
  def test_updateUserRegistryCache_definition( self ):
    
    ins = inspect.getargspec( self.api.updateUserRegistryCache.f )   
    self.assertEqual( ins.args, [ 'self', 'login', 'name', 'email' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None ) 
  
  def test_getUserRegistryCache_definition( self ):
    
    ins = inspect.getargspec( self.api.getUserRegistryCache.f )   
    self.assertEqual( ins.args, [ 'self', 'login', 'name', 'email' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) ) 
  
  def test_deleteUserRegistryCache_definition( self ):
    
    ins = inspect.getargspec( self.api.deleteUserRegistryCache.f )   
    self.assertEqual( ins.args, [ 'self', 'login', 'name', 'email' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, ( None, None, None ) ) 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   