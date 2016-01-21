#import unittest
#import inspect
#
#class TestCase_Description( unittest.TestCase ):
#
#  def test_init_definition( self ):
#    
#    ins = inspect.getargspec( self.handler.__init__ )   
#    self.assertEqual( ins.args, [ 'self', 'serviceInfoDict', 'trid', 'lockManager', 'msgBroker' ] )
#    self.assertEqual( ins.varargs,  None)
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )
#  
#  def test_export_insert_definition( self ):
#    
#    ins = inspect.getargspec( self.handler.export_insert.f.f )   
#    self.assertEqual( ins.args, [ 'self', 'params', 'meta' ] )
#    self.assertEqual( ins.varargs,  None)
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )
#
#  def test_export_update_definition( self ):
#    
#    ins = inspect.getargspec( self.handler.export_update.f.f )   
#    self.assertEqual( ins.args, [ 'self', 'params', 'meta' ] )
#    self.assertEqual( ins.varargs,  None)
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )
#    
#  def test_export_get_definition( self ):
#    
#    ins = inspect.getargspec( self.handler.export_get.f )   
#    self.assertEqual( ins.args, [ 'self', 'params', 'meta' ] )
#    self.assertEqual( ins.varargs,  None)
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )
#    
#  def test_export_delete_definition( self ):
#    
#    ins = inspect.getargspec( self.handler.export_delete.f.f )   
#    self.assertEqual( ins.args, [ 'self', 'params', 'meta' ] )
#    self.assertEqual( ins.varargs,  None)
#    self.assertEqual( ins.keywords, None )
#    self.assertEqual( ins.defaults, None )
#        
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   