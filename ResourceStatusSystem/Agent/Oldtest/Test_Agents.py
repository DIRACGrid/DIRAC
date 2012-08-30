#import unittest
#import sys
#
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()
#
#import DIRAC.ResourceStatusSystem.test.fake_AgentModule
#import DIRAC.ResourceStatusSystem.test.fake_rsDB
#import DIRAC.ResourceStatusSystem.test.fake_rmDB
#import DIRAC.ResourceStatusSystem.test.fake_Logger
#
#class AgentsTestCase( unittest.TestCase ):
#  """ Base class for the Agents test cases
#  """
#  def setUp( self ):
#
#    sys.modules["DIRAC.LoggingSystem.Client.Logger"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.Core.Base.AgentModule"] = DIRAC.ResourceStatusSystem.test.fake_AgentModule
#    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"] = DIRAC.ResourceStatusSystem.test.fake_rsDB
#    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceManagementDB"] = DIRAC.ResourceStatusSystem.test.fake_rmDB
#    sys.modules["DIRAC.Interfaces.API.DiracAdmin"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.ConfigurationSystem.Client.CSAPI"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#
#    from DIRAC.ResourceStatusSystem.Agent.ClientsCacheFeederAgent import ClientsCacheFeederAgent
#    self.ccFeeder = ClientsCacheFeederAgent( "", "" )
#
#    from DIRAC.ResourceStatusSystem.Agent.CleanerAgent import CleanerAgent
#    self.clAgent = CleanerAgent( "", "" )
#
#    from DIRAC.ResourceStatusSystem.Agent.TokenAgent import TokenAgent
#    self.tokenAgent = TokenAgent( "", "" )
#
#    from DIRAC.ResourceStatusSystem.Agent.RSInspectorAgent import RSInspectorAgent
#    self.rsIAgent = RSInspectorAgent( "", "" )
#
#    from DIRAC.ResourceStatusSystem.Agent.SSInspectorAgent import SSInspectorAgent
#    self.ssIAgent = SSInspectorAgent( "", "" )
#
#    from DIRAC.ResourceStatusSystem.Agent.SeSInspectorAgent import SeSInspectorAgent
#    self.sesIAgent = SeSInspectorAgent( "", "" )
#
#    from DIRAC.ResourceStatusSystem.Agent.StElReadInspectorAgent import StElReadInspectorAgent
#    self.stelReadIAgent = StElReadInspectorAgent( "", "" )
#
#    from DIRAC.ResourceStatusSystem.Agent.StElWriteInspectorAgent import StElWriteInspectorAgent
#    self.stelWriteIAgent = StElWriteInspectorAgent( "", "" )
#
#class ClientsCacheFeederSuccess( AgentsTestCase ):
#
#  def test_initialize( self ):
#    res = self.ccFeeder.initialize()
#    self.assert_( res['OK'] )
#
#  def test_execute( self ):
#    self.ccFeeder.initialize()
#    res = self.ccFeeder.execute()
#    self.assert_( res['OK'] )
#
#class CleanerSuccess( AgentsTestCase ):
#
#  def test_initialize( self ):
#    res = self.clAgent.initialize()
#    self.assert_( res['OK'] )
#
#  def test_execute( self ):
#    self.clAgent.initialize()
#    res = self.clAgent.execute()
#    self.assert_( res['OK'] )
#
#class TokenSuccess( AgentsTestCase ):
#
#  def test_initialize( self ):
#    res = self.tokenAgent.initialize()
#    self.assert_( res['OK'] )
#
#  def test_execute( self ):
#    self.tokenAgent.initialize()
#    res = self.tokenAgent.execute()
#    self.assert_( res['OK'] )
#
#class RSInspectorSuccess( AgentsTestCase ):
#
#  def test_initialize( self ):
#    res = self.rsIAgent.initialize()
#    self.assert_( res['OK'] )
#
#  def test_execute( self ):
#    self.rsIAgent.initialize()
#    res = self.rsIAgent.execute()
#    self.assert_( res['OK'] )
#
#class SSInspectorSuccess( AgentsTestCase ):
#
#  def test_initialize( self ):
#    res = self.ssIAgent.initialize()
#    self.assert_( res['OK'] )
#
#  def test_execute( self ):
#    self.ssIAgent.initialize()
#    res = self.ssIAgent.execute()
#    self.assert_( res['OK'] )
#
#class SeSInspectorSuccess( AgentsTestCase ):
#
#  def test_initialize( self ):
#    res = self.sesIAgent.initialize()
#    self.assert_( res['OK'] )
#
#  def test_execute( self ):
#    self.sesIAgent.initialize()
#    res = self.sesIAgent.execute()
#    self.assert_( res['OK'] )
#
#
#class StElReadInspectorSuccess( AgentsTestCase ):
#
#  def test_initialize( self ):
#    res = self.stelReadIAgent.initialize()
#    self.assert_( res['OK'] )
#
#  def test_execute( self ):
#    self.stelReadIAgent.initialize()
#    res = self.stelReadIAgent.execute()
#    self.assert_( res['OK'] )
#
#class StElWriteInspectorSuccess( AgentsTestCase ):
#
#  def test_initialize( self ):
#    res = self.stelWriteIAgent.initialize()
#    self.assert_( res['OK'] )
#
#  def test_execute( self ):
#    self.stelWriteIAgent.initialize()
#    res = self.stelWriteIAgent.execute()
#    self.assert_( res['OK'] )
#
#
#if __name__ == '__main__':
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AgentsTestCase )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientsCacheFeederSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( CleanerSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TokenSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RSInspectorSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SSInspectorSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SeSInspectorSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StElReadInspectorSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StElReadInspectorSuccess ) )
#  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
