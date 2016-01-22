#import unittest
#
#class TestCase_Unit_WithoutPerms( unittest.TestCase ):
#    
#  def test_export_insert_nok( self ):
#    
#    res = self.handler.export_insert( 1, 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( ( 1, ), 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( 1, ( 2, ) )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( 1, { 2: 2} )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( ( 1, ), ( 2, ) )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( ( 1, ), { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( [ 1, ], 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( 1, [ 2, ] )
#    self.assertEquals( res['OK'], False )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( [ 1, ], [ 2, ] )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( [ 1, ], { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( [ 1, ], { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_insert( [1,], {'2':2} )
#    self.assertEquals( res['OK'], False )
#
#    res = self.handler.export_insert( (1,), {'2':2} )
#    self.assertEquals( res[ 'OK' ], False )
#
#################################################################################
#    
#  def test_export_update_nok( self ):
#    
#    res = self.handler.export_update( 1, 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( ( 1, ), 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( 1, ( 2, ) )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( 1, { 2: 2} )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( ( 1, ), ( 2, ) )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( ( 1, ), { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( [ 1, ], 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( 1, [ 2, ] )
#    self.assertEquals( res['OK'], False )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( [ 1, ], [ 2, ] )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( [ 1, ], { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( [ 1, ], { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_update( [1,], {'2':2} )
#    self.assertEquals( res['OK'], False )
#
#    res = self.handler.export_update( (1,), {'2':2} )
#    self.assertEquals( res[ 'OK' ], False )
#    
#################################################################################
#    
#  def test_export_get_nok( self ):
#    
#    res = self.handler.export_get( 1, 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( ( 1, ), 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( 1, ( 2, ) )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( 1, { 2: 2} )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( ( 1, ), ( 2, ) )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( ( 1, ), { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( [ 1, ], 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( 1, [ 2, ] )
#    self.assertEquals( res['OK'], False )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( [ 1, ], [ 2, ] )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( [ 1, ], { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( [ 1, ], { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( [1,], {'2':2} )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_get( (1,), {'2':2} )
#    self.assertEquals( res[ 'OK' ], False )
#
#  def test_export_get_ok( self ):
#    
#    res = self.handler.export_get( { '1' : 1}, {'2':2} )
#    self.assertEquals( res[ 'OK' ], True )
#    
#################################################################################
#    
#  def test_export_delete_nok( self ):
#    
#    res = self.handler.export_delete( 1, 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( ( 1, ), 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( 1, ( 2, ) )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( 1, { 2: 2} )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( ( 1, ), ( 2, ) )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( ( 1, ), { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( [ 1, ], 2 )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( 1, [ 2, ] )
#    self.assertEquals( res['OK'], False )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( [ 1, ], [ 2, ] )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( [ 1, ], { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( [ 1, ], { 2: 2 } )
#    self.assertEquals( res['OK'], False )
#    res = self.handler.export_delete( [1,], {'2':2} )
#    self.assertEquals( res['OK'], False )
#
#    res = self.handler.export_delete( (1,), {'2':2} )
#    self.assertEquals( res[ 'OK' ], False )    
#    
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   