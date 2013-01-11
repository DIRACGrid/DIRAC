import unittest, sys

from DIRAC.ResourceStatusSystem.Command.mock.Command import Command

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Command.DIRACAccounting_Command as mockedModule
    mockedModule.Command = Command
    
    self.clients= {}
    self.clients[ 'DIRACAccounting_Command' ]               = mockedModule.DIRACAccounting_Command()
    self.clients[ 'TransferQuality_Command' ]               = mockedModule.TransferQuality_Command()
    self.clients[ 'TransferQualityCached_Command' ]         = mockedModule.TransferQualityCached_Command()
    self.clients[ 'CachedPlot_Command' ]                    = mockedModule.CachedPlot_Command()
    self.clients[ 'TransferQualityFromCachedPlot_Command' ] = mockedModule.TransferQualityFromCachedPlot_Command()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.DIRACAccounting_Command' ]
  
  