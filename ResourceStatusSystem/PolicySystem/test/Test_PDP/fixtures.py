import unittest, sys

from DIRAC.ResourceStatusSystem.Utilities.mock.InfoGetter      import InfoGetter
from DIRAC.ResourceStatusSystem.PolicySystem.mock.PolicyCaller import PolicyCaller
from DIRAC.ResourceStatusSystem.Command.mock.CommandCaller     import CommandCaller

class UnitFixture( unittest.TestCase ):
  
  def setUp( self ):   
       
    import DIRAC.ResourceStatusSystem.PolicySystem.PDP as mockedModule
    
    mockedModule.InfoGetter    = InfoGetter
    mockedModule.PolicyCaller  = PolicyCaller
    mockedModule.CommandCaller = CommandCaller
    
    self.pdp = mockedModule.PDP()
    
  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.PolicySystem.PDP' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Utilities.mock.InfoGetter' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.PolicySystem.mock.PolicyCaller' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.mock.CommandCaller' ]
    