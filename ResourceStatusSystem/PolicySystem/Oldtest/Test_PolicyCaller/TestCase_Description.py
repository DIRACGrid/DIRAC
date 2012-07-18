import unittest
import inspect

class TestCase_Description( unittest.TestCase ):

  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.pc.__init__ )   
    self.assertEqual( ins.args, [ 'self', 'commandCallerIn' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'clients' )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_policyInvocation_definition( self ):
    
    
    ins = inspect.getargspec( self.pc.policyInvocation )   
    self.assertEqual( ins.args, [ 'self', 'VOExtension', 'granularity', 'name', 
                                  'status', 'policy', 'args', 'pName', 'pModule', 
                                  'extraArgs', 'commandIn' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,None, ) )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   