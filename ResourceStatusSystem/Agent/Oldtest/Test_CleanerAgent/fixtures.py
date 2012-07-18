import unittest, sys

from DIRAC.ResourceStatusSystem.Client.mock.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.mock.ResourceManagementClient import ResourceManagementClient 
from DIRAC.ResourceStatusSystem.mock                                 import ValidRes
from DIRAC.ResourceStatusSystem.Agent.mock.AgentModule               import AgentModule

class UnitFixture( unittest.TestCase ):

  def setUp( self ):
       
    import DIRAC.ResourceStatusSystem.Agent.CleanerAgent as mockedModule
    mockedModule.ResourceStatusClient     = ResourceStatusClient
    mockedModule.ResourceManagementClient = ResourceManagementClient
    mockedModule.ValidRes                 = ValidRes
    
    mockedModule.CleanerAgent.__bases__ = ( AgentModule, )
    
    self.agent = mockedModule.CleanerAgent( '', '')

  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Agent.CleanerAgent' ]

