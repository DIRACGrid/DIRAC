import unittest, sys

from DIRAC.ResourceStatusSystem.Client.mock.PilotsClient import PrivatePilotsClient

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
    
    import DIRAC.ResourceStatusSystem.Client.PilotsClient as mockedModule
    mockedModule.PrivatePilotsClient = PrivatePilotsClient
    
    self.client = mockedModule.PilotsClient()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Client.PilotsClient' ]
   
