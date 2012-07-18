import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.Command import Command

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Command.RS_Command as mockedModule
    mockedModule.Command = Command
    
    self.clients= {}
    self.clients[ 'RSPeriods_Command' ]            = mockedModule.RSPeriods_Command()
    self.clients[ 'ServiceStats_Command' ]         = mockedModule.ServiceStats_Command()
    self.clients[ 'ResourceStats_Command' ]        = mockedModule.ResourceStats_Command()
    self.clients[ 'StorageElementsStats_Command' ] = mockedModule.StorageElementsStats_Command()   
    self.clients[ 'MonitoredStatus_Command' ]      = mockedModule.MonitoredStatus_Command()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.RS_Command' ]
  
  