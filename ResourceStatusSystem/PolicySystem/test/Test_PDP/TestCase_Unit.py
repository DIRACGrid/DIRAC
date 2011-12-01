import unittest

class TestCase_Unit( unittest.TestCase ):

  def test_takeDecision_ok( self ):
    
    self.pdp.setup( 'LHCb', granularity = '' )
    res = self.pdp.takeDecision()
    self.assertEquals( res[ 'SinglePolicyResults' ], [])
    self.assertEquals( res[ 'PolicyCombinedResult'], {'Action': False, 'Reason': 'No policy results', 'PolicyType': []})
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   