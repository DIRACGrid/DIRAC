import unittest, sys

from DIRAC.ResourceStatusSystem.API.mock.ResourceStatusAPI import ResourceStatusAPI
from DIRAC.ResourceStatusSystem.API.mock.ResourceManagementAPI import ResourceManagementAPI 
from DIRAC.ResourceStatusSystem.mock import ValidRes
from DIRAC.ResourceStatusSystem.Agent.mock.AgentModule import AgentModule

class UnitFixture( unittest.TestCase ):

  def setUp( self ):
       
    import DIRAC.ResourceStatusSystem.Agent.CleanerAgent as mockedModule
    mockedModule.ResourceStatusAPI = ResourceStatusAPI
    mockedModule.ResourceManagementAPI = ResourceManagementAPI
    mockedModule.ValidRes = ValidRes
    
    mockedModule.CleanerAgent.__bases__ = ( AgentModule, )
    
    self.agent = mockedModule.CleanerAgent( '', '')

  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Agent.CleanerAgent' ]

