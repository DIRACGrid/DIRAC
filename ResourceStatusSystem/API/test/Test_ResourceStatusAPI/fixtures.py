import unittest, sys

from DIRAC.ResourceStatusSystem.API.mock.ResourceStatusExtendedBaseAPI import ResourceStatusExtendedBaseAPI

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
    
    import DIRAC.ResourceStatusSystem.API.ResourceStatusAPI as mockedModule
    mockedModule.ResourceStatusExtendedBaseAPI = ResourceStatusExtendedBaseAPI

    self.api = mockedModule.ResourceStatusAPI()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.API.ResourceStatusAPI' ]
