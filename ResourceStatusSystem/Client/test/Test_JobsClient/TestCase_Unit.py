import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):
  
  def test_getJobsSimpleEff_definition( self ):
    
    ins = inspect.getargspec( self.client.getJobsSimpleEff )   
    self.assertEqual( ins.args, [ 'self', 'name', 'RPCWMSAdmin' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )
    
  def test_getJobsSimpleEff_nok( self ):
    
    self.assertRaises( TypeError, self.client.getJobsSimpleEff, *( 1, 2 ), **{ 'RPCWMSAdmin' : 2 } )  

  def test_getJobsSimpleEff_ok( self ):

    res = self.client.getJobsSimpleEff( 1, 2 )
    self.assertEquals( res, {} )
    res = self.client.getJobsSimpleEff( 1, RPCWMSAdmin = 2 )
    self.assertEquals( res, {} )
    res = self.client.getJobsSimpleEff( name = 1, RPCWMSAdmin = 2 )
    self.assertEquals( res, {} )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   