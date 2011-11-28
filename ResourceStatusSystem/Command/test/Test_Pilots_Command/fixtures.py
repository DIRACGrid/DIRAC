import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.Command import Command

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Command.Pilots_Command as mockedModule
    mockedModule.Command = Command
    
    self.clients= {}
    self.clients[ 'PilotsStats_Command' ]           = mockedModule.PilotsStats_Command()
    self.clients[ 'PilotsEff_Command' ]             = mockedModule.PilotsEff_Command()
    self.clients[ 'PilotsEffSimple_Command' ]       = mockedModule.PilotsEffSimple_Command()
    self.clients[ 'PilotsEffSimpleCached_Command' ] = mockedModule.PilotsEffSimpleCached_Command()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.Pilots_Command' ]
  
  