import unittest, sys

from DIRAC.ResourceStatusSystem.Service.mock.RequestHandler     import RequestHandler
from DIRAC.ResourceStatusSystem.DB.mock.ResourceManagementDB    import ResourceManagementDB

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
    
    import DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler as mockedModule
    
    mockedModule.RequestHandler             = RequestHandler
    mockedModule.RequestHandler.credentials = { 'group' : 'diracAdmin' }
    mockedModule.ResourceManagementDB       = ResourceManagementDB
    
    mockedModule.ResourceManagementHandler.__bases__ = ( mockedModule.RequestHandler, )
    
    mockedModule.initializeResourceManagementHandler( 1 )
    self.handler = mockedModule.ResourceManagementHandler( '', '', '', '' )
    
  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler' ]
    
class DescriptionFixture_WithoutPerms( unittest.TestCase ):
  
  def setUp( self ):   
    
    import DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler as mockedModule
    
    mockedModule.RequestHandler             = RequestHandler
    mockedModule.RequestHandler.credentials = {}
    mockedModule.ResourceManagementDB       = ResourceManagementDB
    
    mockedModule.ResourceManagementHandler.__bases__ = ( mockedModule.RequestHandler, )
    
    mockedModule.initializeResourceManagementHandler( 1 )
    self.handler = mockedModule.ResourceManagementHandler( '', '', '', '' )
    
  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler' ]    