import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Command.mock.CommandCaller  import CommandCaller

from DIRAC.ResourceStatusSystem.Utilities.Utils             import where

class DescriptionFixture( unittest.TestCase ):
 
  def setUp( self ):
       
    import DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase as mockedModule
    
    mockedModule.ValidRes       = ValidRes
    mockedModule.ClientsInvoker = ClientsInvoker
    mockedModule.CommandCaller  = CommandCaller
    
    self.pb        = mockedModule.PolicyBase()
    self._mockMods = {}
    
  def tearDown( self ):  

    del sys.modules[ 'DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase' ]
