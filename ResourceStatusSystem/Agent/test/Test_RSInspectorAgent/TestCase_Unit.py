import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):
  
  def test_initialize( self ):
    
    res = self.agent.initialize()
    self.assertEquals( res['OK'], True )
    
    self.agent.execute()
    self.agent.finalize()
    
  def test_execute( self ):
    
    #Step needed to initialize agent
    self.agent.initialize()
    
    res = self.agent.execute()
    self.assertEquals( res['OK'], True )

    self.agent.finalize()

  def test_finalize( self ):
    
    #Step needed to initialize agent
    self.agent.initialize()
    self.agent.execute()
    
    res = self.agent.finalize()
    self.assertEquals( res['OK'], True )        
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF