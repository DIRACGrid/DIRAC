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
  
#  def test_insertSite_definition( self ):
#    
#    ins = inspect.getargspec( self.api.insertSite.f )   
#    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
#    self.assertEqual( ins.varargs,  None )
#    self.assertEqual( ins.keywords, 'kwargs' )
#    self.assertEqual( ins.defaults, None )  
#
#  def test_insertSite_nok( self ):
#    
#    res = self.api.insertSite( siteName = 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], False )
#    res = self.api.insertSite( siteName = 1, siteType = 2, gridSiteName = 3, a = 'a' )
#    self.assertEqual ( res['OK'], False )
#    res = self.api.insertSite( 1, siteType = 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], False )
#    res = self.api.insertSite( 1, 2, gridSiteName = 3 )
#    self.assertEqual ( res['OK'], False ) 
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF