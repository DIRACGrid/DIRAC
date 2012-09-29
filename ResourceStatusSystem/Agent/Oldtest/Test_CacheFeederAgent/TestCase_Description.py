#import unittest
#import inspect
#
#class TestCase_Description( unittest.TestCase ):
#
#  def test_init_description( self ):
#    
#    ins = inspect.getargspec( self.agent.__init__ )
#    self.assertEqual( ins.args, [ 'self', 'agentName', 'baseAgentName', 'properties' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, ( False, {}, ) )
#      
#  def test_initialize_description( self ):
#    
#    ins = inspect.getargspec( self.agent.initialize )
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#    
#  def test_execute_description( self ):
#    
#    ins = inspect.getargspec( self.agent.execute )
#    self.assertEqual( ins.args, [ 'self' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None ) 
#            
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF