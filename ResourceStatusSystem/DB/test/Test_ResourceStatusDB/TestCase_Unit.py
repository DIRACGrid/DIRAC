import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):
     
  def test_insert_nok( self ):
    
    res = self.db.insert( 1, 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( ( 1, ), 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( 1, ( 2, ) )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( 1, { 2: 2} )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( ( 1, ), ( 2, ) )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( ( 1, ), { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( [ 1, ], 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( 1, [ 2, ] )
    self.assertEquals( res['OK'], False )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( [ 1, ], [ 2, ] )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( [ 1, ], { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( [ 1, ], { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.insert( [1,], {'2':2} )
    self.assertEquals( res['OK'], False )
    
  def test_insert_ok( self ):
    
    res = self.db.insert( (1,), {'2':2} )
    self.assertEquals( res['OK'], True )

################################################################################
       
  def test_update_nok( self ):
    
    res = self.db.update( 1, 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.update( ( 1, ), 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.update( 1, ( 2, ) )
    self.assertEquals( res['OK'], False )
    res = self.db.update( 1, { 2: 2} )
    self.assertEquals( res['OK'], False )
    res = self.db.update( ( 1, ), ( 2, ) )
    self.assertEquals( res['OK'], False )
    res = self.db.update( ( 1, ), { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.update( [ 1, ], 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.update( 1, [ 2, ] )
    self.assertEquals( res['OK'], False )
    self.assertEquals( res['OK'], False )
    res = self.db.update( [ 1, ], [ 2, ] )
    self.assertEquals( res['OK'], False )
    res = self.db.update( [ 1, ], { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.update( [ 1, ], { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.update( [1,], {'2':2} )
    self.assertEquals( res['OK'], False )

  def test_update_ok( self ):
    
    res = self.db.update( (1,), {'2':2} )
    self.assertEquals( res['OK'], True )

################################################################################
    
  def test_get_nok( self ):
    
    res = self.db.get( 1, 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.get( ( 1, ), 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.get( 1, ( 2, ) )
    self.assertEquals( res['OK'], False )
    res = self.db.get( 1, { 2: 2} )
    self.assertEquals( res['OK'], False )
    res = self.db.get( ( 1, ), ( 2, ) )
    self.assertEquals( res['OK'], False )
    res = self.db.get( ( 1, ), { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.get( [ 1, ], 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.get( 1, [ 2, ] )
    self.assertEquals( res['OK'], False )
    self.assertEquals( res['OK'], False )
    res = self.db.get( [ 1, ], [ 2, ] )
    self.assertEquals( res['OK'], False )
    res = self.db.get( [ 1, ], { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.get( [ 1, ], { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.update( [1,], {'2':2} )
    self.assertEquals( res['OK'], False )

  def test_get_ok( self ):
    
    res = self.db.get( (1,), {'2':2} )
    self.assertEquals( res['OK'], True )

################################################################################
    
  def test_delete_nok( self ):
    
    res = self.db.delete( 1, 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( ( 1, ), 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( 1, ( 2, ) )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( 1, { 2: 2} )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( ( 1, ), ( 2, ) )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( ( 1, ), { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( [ 1, ], 2 )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( 1, [ 2, ] )
    self.assertEquals( res['OK'], False )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( [ 1, ], [ 2, ] )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( [ 1, ], { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.delete( [ 1, ], { 2: 2 } )
    self.assertEquals( res['OK'], False )
    res = self.db.update( [1,], {'2':2} )
    self.assertEquals( res['OK'], False )

  def test_delete_ok( self ):

    res = self.db.delete( (1,), {'2':2} )
    self.assertEquals( res['OK'], True )

################################################################################

  def test_getSchema_nok( self ):
    pass
    
  def test_getSchema_ok( self ):

    res = self.db.getSchema()
    self.assertEquals( res['OK'], True )
    
################################################################################
    
  def test_inspectSchema_nok( self ):
    pass
    
  def test_inspectSchema_ok( self ):

    res = self.db.inspectSchema()
    self.assertEquals( res['OK'], True )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   