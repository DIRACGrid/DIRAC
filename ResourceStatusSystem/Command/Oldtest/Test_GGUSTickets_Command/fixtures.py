import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.Command import Command

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Command.GGUSTickets_Command as mockedModule
    mockedModule.Command = Command
    
    self.clients= {}
    self.clients[ 'GGUSTickets_Open' ] = mockedModule.GGUSTickets_Open()
    self.clients[ 'GGUSTickets_Link' ] = mockedModule.GGUSTickets_Link()
    self.clients[ 'GGUSTickets_Info' ] = mockedModule.GGUSTickets_Info()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.GGUSTickets_Command' ]
  
  