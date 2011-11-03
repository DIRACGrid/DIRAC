import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):
  
  def test_insert_nok( self ):
    pass

  def test_insert_ok( self ):

    res = self.client.insert( )
    self.assertEquals( res['OK'], True )
    res = self.client.insert( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.insert( a = 1, b = 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.insert( 1, 2, a = 1, b = 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.insert( *(1,), **{'2':2} )
    self.assertEquals( res[ 'OK' ], True )

################################################################################

  def test_update_nok( self ):
    pass    

  def test_update_ok( self ):
    
    res = self.client.update( )
    self.assertEquals( res['OK'], True )
    res = self.client.update( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.update( a = 1, b = 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.update( 1, 2, a = 1, b = 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.update( *(1,), **{'2':2} )
    self.assertEquals( res[ 'OK' ], True )
    
################################################################################

  def test_get_nok( self ):
    pass
  
  def test_get_ok( self ):
    
    res = self.client.get( )
    self.assertEquals( res['OK'], True )
    res = self.client.get( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.get( a = 1, b = 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.get( 1, 2, a = 1, b = 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.get( *(1,), **{'2':2} )
    self.assertEquals( res[ 'OK' ], True )

################################################################################

  def test_delete_nok( self ):
    pass
  
  def test_delete_ok( self ):
    
    res = self.client.delete( )
    self.assertEquals( res['OK'], True )
    res = self.client.delete( 1, 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.delete( a = 1, b = 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.delete( 1, 2, a = 1, b = 2 )
    self.assertEquals( res['OK'], True )
    res = self.client.delete( *(1,), **{'2':2} )
    self.assertEquals( res[ 'OK' ], True ) 
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   