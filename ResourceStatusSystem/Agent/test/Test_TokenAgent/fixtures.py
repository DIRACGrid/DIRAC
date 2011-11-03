import unittest, sys

from DIRAC.ResourceStatusSystem.PolicySystem.mock.PDP import PDP
from DIRAC.ResourceStatusSystem.API.mock.ResourceStatusAPI import ResourceStatusAPI 
from DIRAC.ResourceStatusSystem.mock import ValidRes
from DIRAC.ResourceStatusSystem.Utilities.mock import CS
from DIRAC.ResourceStatusSystem.Client.mock.NotificationClient import NotificationClient
from DIRAC.ResourceStatusSystem.Agent.mock.AgentModule import AgentModule

class UnitFixture( unittest.TestCase ):

  def setUp( self ):
    
    import DIRAC.ResourceStatusSystem.Agent.TokenAgent as mockedModule
    mockedModule.PDP = PDP
    mockedModule.ResourceStatusAPI = ResourceStatusAPI
    mockedModule.ValidRes = ValidRes
    mockedModule.CS = CS
    mockedModule.NotificationClient = NotificationClient
    
    mockedModule.TokenAgent.__bases__ = ( AgentModule, )
    
    self.agent = mockedModule.TokenAgent( '', '')

  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Agent.TokenAgent' ]

