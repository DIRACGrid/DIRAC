import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.CommandCaller   import CommandCaller
from DIRAC.ResourceStatusSystem.PolicySystem.mock.PolicyBase import PolicyBase

class DescriptionFixture( unittest.TestCase ):
 
  def setUp( self ):
       
    import DIRAC.ResourceStatusSystem.PolicySystem.PolicyCaller as mockedModule
    
    mockedModule.CommandCaller = CommandCaller
    
    self.pc        = mockedModule.PolicyCaller()
    
    self._mockMods = {}
    self._mockMods[ 'PolicyBase' ] = PolicyBase
    
    
  def tearDown( self ):  

    del sys.modules[ 'DIRAC.ResourceStatusSystem.PolicySystem.PolicyCaller' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.mock.CommandCaller' ]