import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):
      
  def test_getPilotsSimpleEff_nok( self ):
    
    self.assertRaises( TypeError, self.client.getPilotsSimpleEff, *( 1, 2, 3, 4 ), **{ 'granularity' : 1 } )
    self.assertRaises( TypeError, self.client.getPilotsSimpleEff, *( 1, 2, 3, 4 ), 
                       **{ 'granularity' : 1, 'name' : 2, 'siteName' : 3, 'RPCWMSAdmin' : 4 } )
   

  def test_getPilotsSimpleEff_ok( self ):

    res = self.client.getPilotsSimpleEff( 1, 2, 3, 4 )
    self.assertEquals( res, {} )
    res = self.client.getPilotsSimpleEff( 1, 2, 3 )
    self.assertEquals( res, {} )
    res = self.client.getPilotsSimpleEff( 1, 2 )
    self.assertEquals( res, {} )
    res = self.client.getPilotsSimpleEff( 1, 2, 3, RPCWMSAdmin = 4 )
    self.assertEquals( res, {} )
    res = self.client.getPilotsSimpleEff( 1, 2, RPCWMSAdmin = 4 )
    self.assertEquals( res, {} )
    res = self.client.getPilotsSimpleEff( 1, 2, siteName = 3 )
    self.assertEquals( res, {} )
    res = self.client.getPilotsSimpleEff( 1, 2, siteName = 3, RPCWMSAdmin = 4 )
    self.assertEquals( res, {} )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   