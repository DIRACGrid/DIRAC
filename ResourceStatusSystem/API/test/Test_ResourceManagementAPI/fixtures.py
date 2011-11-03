import unittest, sys

from DIRAC.ResourceStatusSystem.API.mock.ResourceManagementExtendedBaseAPI import ResourceManagementExtendedBaseAPI

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
 
    import DIRAC.ResourceStatusSystem.API.ResourceManagementAPI as mockedModule
    mockedModule.ResourceManagementExtendedBaseAPI = ResourceManagementExtendedBaseAPI

    self.api = mockedModule.ResourceManagementAPI()
    
  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.API.ResourceManagementAPI' ]
