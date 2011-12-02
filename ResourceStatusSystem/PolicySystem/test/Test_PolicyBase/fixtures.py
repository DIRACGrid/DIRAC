import unittest, sys

from DIRAC.ResourceStatusSystem.Utilities.mock.Exceptions   import InvalidRes, RSSException
from DIRAC.ResourceStatusSystem.mock                        import ValidRes
from DIRAC.ResourceStatusSystem.Command.mock.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Command.mock.CommandCaller  import CommandCaller

from DIRAC.ResourceStatusSystem.Utilities.Utils             import where

class DescriptionFixture( unittest.TestCase ):
 
  def setUp( self ):
       
    import DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase as mockedModule
    
    mockedModule.InvalidRes     = InvalidRes
    mockedModule.RSSException   = RSSException
    mockedModule.ValidRes       = ValidRes
    mockedModule.ClientsInvoker = ClientsInvoker
    mockedModule.CommandCaller  = CommandCaller
    
    self.pb        = mockedModule.PolicyBase()
    self._mockMods = {}
    self._mockMods[ 'InvalidRes' ]   = InvalidRes
    self._mockMods[ 'RSSException' ] = RSSException
    
  def tearDown( self ):  

    del sys.modules[ 'DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase' ]
