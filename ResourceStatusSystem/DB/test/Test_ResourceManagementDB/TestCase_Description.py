import unittest
import inspect

class TestCase_Description( unittest.TestCase ):

  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.db.__init__ )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  'args' )
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
  
  def test_insert_definition( self ):
    
    ins = inspect.getargspec( self.db.insert.f.f )   
    self.assertEqual( ins.args, [ 'self', 'args', 'kwargs' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
  
  def test_update_definition( self ):
    
    ins = inspect.getargspec( self.db.update.f.f )   
    self.assertEqual( ins.args, [ 'self', 'args', 'kwargs' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
       
  def test_get_definition( self ):
    
    ins = inspect.getargspec( self.db.get.f.f )   
    self.assertEqual( ins.args, [ 'self', 'args', 'kwargs' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
  def test_delete_definition( self ):
    
    ins = inspect.getargspec( self.db.delete.f.f )   
    self.assertEqual( ins.args, [ 'self', 'args', 'kwargs' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
  def test_getSchema_definition( self ):
    
    ins = inspect.getargspec( self.db.getSchema.f )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_inspectSchema_definition( self ):
    
    ins = inspect.getargspec( self.db.inspectSchema.f )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   