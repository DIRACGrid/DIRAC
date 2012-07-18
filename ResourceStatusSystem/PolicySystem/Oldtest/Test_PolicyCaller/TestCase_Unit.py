import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):

  def test_policyInvocation_ok( self ):
    
    p = self._mockMods[ 'PolicyBase' ]()
    res = self.pc.policyInvocation( policy = p )
    self.assertEquals( res, {'Status': '', 'PolicyName': None, 'Reason': '' } )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   