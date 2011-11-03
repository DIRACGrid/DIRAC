import unittest
import inspect

class TestCase_Description( unittest.TestCase ):

  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.client.__init__ )   
    self.assertEqual( ins.args, [ 'self', 'serviceIn' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )
  
  def test_insert_definition( self ):
    
    ins = inspect.getargspec( self.client.insert )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  'args' )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )

  def test_update_definition( self ):
    
    ins = inspect.getargspec( self.client.update )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  'args' )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
  def test_get_definition( self ):
    
    ins = inspect.getargspec( self.client.get )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  'args' )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
  def test_delete_definition( self ):
    
    ins = inspect.getargspec( self.client.delete )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  'args' )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
      
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   