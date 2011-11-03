import unittest

class TestCase_Unit( unittest.TestCase ):

  def test_takeDecision_ok( self ):
    
    self.pdp.setup( 'LHCb', granularity = '' )
    print self.pdp.takeDecision()
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   