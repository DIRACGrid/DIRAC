import unittest, sys

from DIRAC.ResourceStatusSystem.PolicySystem.mock.PEP      import PEP
from DIRAC.ResourceStatusSystem.API.mock.ResourceStatusAPI import ResourceStatusAPI
from DIRAC.ResourceStatusSystem.mock                       import CheckingFreqs
from DIRAC.ResourceStatusSystem.Utilities.mock             import CS    
from DIRAC.ResourceStatusSystem.Command.knownAPIs          import initAPIs 
from DIRAC.ResourceStatusSystem.Agent.mock.AgentModule     import AgentModule

class UnitFixture( unittest.TestCase ):

  def setUp( self ):
    
    import DIRAC.ResourceStatusSystem.Agent.SSInspectorAgent as mockedModule
    mockedModule.PEP = PEP
    mockedModule.ResourceStausAPI = ResourceStatusAPI
    mockedModule.CheckingFreqs = CheckingFreqs
    mockedModule.CS = CS
    mockedModule.initAPIs = initAPIs
    
    mockedModule.SSInspectorAgent.__bases__ = ( AgentModule, )
    
    self.agent = mockedModule.SSInspectorAgent( '', '')

  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Agent.SSInspectorAgent' ]

