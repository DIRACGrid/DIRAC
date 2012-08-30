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
#  def test_insertEnvironmentCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertEnvironmentCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'hashEnv', 'siteName', 'environment', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#    
#  def test_updateEnvironmentCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateEnvironmentCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'hashEnv', 'siteName', 'environment', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )  
#
#  def test_getEnvironmentCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getEnvironmentCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'hashEnv', 'siteName', 'environment', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) )  
#   
#  def test_deleteEnvironmentCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteEnvironmentCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'hashEnv', 'siteName', 'environment', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) )      
#  
#  def test_insertPolicyResult_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertPolicyResult.f )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'policyName', 'statusType',
#                        'status', 'reason', 'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )      
#
#  def test_updatePolicyResult_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updatePolicyResult.f )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'policyName', 'statusType',
#                        'status', 'reason', 'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) )      
#
#  def test_getPolicyResult_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getPolicyResult.f )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'policyName', 'statusType',
#                        'status', 'reason', 'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None, {} ) )   
# 
#  def test_deletePolicyResult_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deletePolicyResult.f )   
#    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'policyName', 'statusType',
#                        'status', 'reason', 'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, None, {} ) )   
#
#  def test_insertClientCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertClientCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'name', 'commandName', 'opt_ID', 'value',
#                                 'result', 'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) ) 
#   
#  def test_updateClientCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateClientCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'name', 'commandName', 'opt_ID', 'value',
#                                  'result', 'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) ) 
# 
#  def test_getClientCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getClientCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'name', 'commandName', 'opt_ID', 'value',
#                                  'result', 'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, {} ) ) 
# 
#  def test_deleteClientCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteClientCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'name', 'commandName', 'opt_ID', 'value',
#                                 'result', 'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, None, {} ) ) 
#  
#  def test_insertAccountingCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertAccountingCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'name', 'plotType', 'plotName', 'result', 
#                                  'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) ) 
#   
#  def test_updateAccountingCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateAccountingCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'name', 'plotType', 'plotName', 'result', 
#                                  'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) ) 
#   
#  def test_getAccountingCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getAccountingCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'name', 'plotType', 'plotName', 'result', 
#                                  'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, {} ) ) 
#   
#  def test_deleteAccountingCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteAccountingCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'name', 'plotType', 'plotName', 'result', 
#                                  'dateEffective', 'lastCheckTime', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, {} ) ) 
# 
#  def test_insertUserRegistryCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.insertUserRegistryCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'login', 'name', 'email', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) ) 
#   
#  def test_updateUserRegistryCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.updateUserRegistryCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'login', 'name', 'email', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ({},) ) 
#  
#  def test_getUserRegistryCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.getUserRegistryCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'login', 'name', 'email','meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) ) 
#  
#  def test_deleteUserRegistryCache_definition( self ):
#    
#    ins = inspect.getargspec( self.client.deleteUserRegistryCache.f )   
#    self.assertEqual( ins.args, [ 'self', 'login', 'name', 'email', 'meta' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( None, None, None, {} ) ) 
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   