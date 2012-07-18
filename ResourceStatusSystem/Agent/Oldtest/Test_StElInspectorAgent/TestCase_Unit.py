import unittest

class TestCase_Unit( unittest.TestCase ):
  
  def test_cycle( self ):
    
    #Step needed to initialize agent
    res = self.agent.initialize()
    self.assertEquals( res['OK'], True )
    
    res = self.agent.execute()
    self.assertEquals( res['OK'], True )

    res = self.agent.finalize()
    self.assertEquals( res['OK'], True )          
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF