#import unittest, sys
#
#from DIRAC.ResourceStatusSystem.Service.mock.RequestHandler import RequestHandler
#from DIRAC.ResourceStatusSystem.DB.mock.ResourceStatusDB    import ResourceStatusDB
#from DIRAC.ResourceStatusSystem.Utilities.mock.Synchronizer import Synchronizer
#
#class DescriptionFixture( unittest.TestCase ):
#  
#  def setUp( self ):   
#    
#    import DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler as mockedModule
#    
#    mockedModule.RequestHandler             = RequestHandler
#    mockedModule.RequestHandler.credentials = { 'group' : 'diracAdmin' }
#    mockedModule.ResourceStatusDB           = ResourceStatusDB
#    mockedModule.Synchronizer               = Synchronizer
#    
#    mockedModule.ResourceStatusHandler.__bases__ = ( mockedModule.RequestHandler, )
#    
#    mockedModule.initializeResourceStatusHandler( 1 )
#    self.handler = mockedModule.ResourceStatusHandler( '', '', '', '' )
#    
#  def tearDown( self ):  
#    
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler' ]
#
#class DescriptionFixture_WithoutPerms( unittest.TestCase ):
#  
#  def setUp( self ):   
#    
#    import DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler as mockedModule
#    
#    mockedModule.RequestHandler             = RequestHandler
#    mockedModule.RequestHandler.credentials = {}
#    mockedModule.ResourceStatusDB           = ResourceStatusDB
#    mockedModule.Synchronizer               = Synchronizer
#    
#    mockedModule.ResourceStatusHandler.__bases__ = ( mockedModule.RequestHandler, )
#    
#    mockedModule.initializeResourceStatusHandler( 1 )
#    self.handler = mockedModule.ResourceStatusHandler( '', '', '', '' )
#    
#  def tearDown( self ):  
#    
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler' ]    