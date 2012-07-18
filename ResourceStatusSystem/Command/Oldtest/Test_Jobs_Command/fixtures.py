import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.Command import Command

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Command.Jobs_Command as mockedModule
    mockedModule.Command = Command
    
    self.clients= {}
    self.clients[ 'JobsStats_Command' ]           = mockedModule.JobsStats_Command()
    self.clients[ 'JobsEff_Command' ]             = mockedModule.JobsEff_Command()
    self.clients[ 'SystemCharge_Command' ]        = mockedModule.SystemCharge_Command()
    self.clients[ 'JobsEffSimple_Command' ]       = mockedModule.JobsEffSimple_Command()
    self.clients[ 'JobsEffSimpleCached_Command' ] = mockedModule.JobsEffSimpleCached_Command()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.Jobs_Command' ]
  
  