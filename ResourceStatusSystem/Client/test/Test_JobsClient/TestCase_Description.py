import unittest
import inspect

class TestCase_Description( unittest.TestCase ):

  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.client.__init__ )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
  
  def test_getJobsSimpleEff_definition( self ):
    
    ins = inspect.getargspec( self.client.getJobsSimpleEff )   
    self.assertEqual( ins.args, [ 'self', 'name', 'RPCWMSAdmin' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   