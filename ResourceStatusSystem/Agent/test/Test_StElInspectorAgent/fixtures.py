import unittest, sys

from DIRAC.ResourceStatusSystem.PolicySystem.mock.PEP            import PEP
from DIRAC.ResourceStatusSystem.Client.mock.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.mock                             import CheckingFreqs
from DIRAC.ResourceStatusSystem.Utilities.mock                   import CS    
from DIRAC.ResourceStatusSystem.Agent.mock.AgentModule           import AgentModule
from DIRAC.ResourceStatusSystem.Command.mock                     import knownAPIs

class UnitFixture( unittest.TestCase ):

  def setUp( self ):
    
    import DIRAC.ResourceStatusSystem.Agent.StElInspectorAgent as mockedModule
    mockedModule.PEP                  = PEP
    mockedModule.ResourceStatusClient = ResourceStatusClient
    mockedModule.CheckingFreqs        = CheckingFreqs
    mockedModule.CS                   = CS
    mockedModule.knownAPIs            = knownAPIs
    
    mockedModule.StElInspectorAgent.__bases__ = ( AgentModule, )
    
    self.agent = mockedModule.StElInspectorAgent( '', '')

  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Agent.StElInspectorAgent' ]

