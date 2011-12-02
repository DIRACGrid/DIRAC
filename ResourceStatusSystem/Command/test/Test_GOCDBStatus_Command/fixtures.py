import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.Command import Command

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Command.GOCDBStatus_Command as mockedModule
    mockedModule.Command = Command
    
    self.clients= {}
    self.clients[ 'GOCDBStatus_Command' ]   = mockedModule.GOCDBStatus_Command()
    self.clients[ 'DTCached_Command' ]      = mockedModule.DTCached_Command()
    self.clients[ 'DTInfo_Cached_Command' ] = mockedModule.DTInfo_Cached_Command()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.GOCDBStatus_Command' ]
  
  