#import unittest, sys
#
#from DIRAC.ResourceStatusSystem.Client.mock.ResourceManagementClient import ResourceManagementClient
#from DIRAC.ResourceStatusSystem.Command.mock.ClientsInvoker          import ClientsInvoker
#from DIRAC.ResourceStatusSystem.Agent.mock.AgentModule               import AgentModule
#
#class UnitFixture( unittest.TestCase ):
#
#  def setUp( self ):
#    
#    import DIRAC.ResourceStatusSystem.Agent.CacheFeederAgent as mockedModule
#    mockedModule.ResourceManagementClient = ResourceManagementClient
#    mockedModule.ClientsInvoker           = ClientsInvoker
#    
#    mockedModule.CacheFeederAgent.__bases__ = ( AgentModule, )
#    
#    self.agent = mockedModule.CacheFeederAgent( '', '')
#
#  def tearDown( self ):  
#    
#    #sys.modules = self._modulesBkup
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Agent.CacheFeederAgent' ]
