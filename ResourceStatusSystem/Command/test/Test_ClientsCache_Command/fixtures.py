import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.Command import Command

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Command.ClientsCache_Command as mockedModule
    mockedModule.Command = Command
    
    self.clients= {}
    self.clients[ 'JobsEffSimpleEveryOne_Command' ]     = mockedModule.JobsEffSimpleEveryOne_Command()
    self.clients[ 'PilotsEffSimpleEverySites_Command' ] = mockedModule.PilotsEffSimpleEverySites_Command()
    self.clients[ 'TransferQualityEverySEs_Command' ]   = mockedModule.TransferQualityEverySEs_Command()
    self.clients[ 'DTEverySites_Command' ]              = mockedModule.DTEverySites_Command()
    self.clients[ 'DTEveryResources_Command' ]          = mockedModule.DTEveryResources_Command()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.ClientsCache_Command' ]
  
  