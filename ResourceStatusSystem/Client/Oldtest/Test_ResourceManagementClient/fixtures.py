#import unittest, sys
#
#from DIRAC.ResourceStatusSystem.DB.mock      import ResourceManagementDB
#from DIRAC.ResourceStatusSystem.Service.mock import ResourceManagementHandler    
#    
#class Description_withDB( unittest.TestCase ):
#  
#  def setUp( self ):      
#   
#    import DIRAC.ResourceStatusSystem.Client.ResourceManagementClient as mockedModule
#
#    _serviceIn = ResourceManagementDB.ResourceManagementDB()
#    self.client = mockedModule.ResourceManagementClient( serviceIn = _serviceIn )
#    
#  def tearDown( self ):  
#    
#    #sys.modules = self._modulesBkup
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Client.ResourceManagementClient' ]
#    
#class Description_withHandler( unittest.TestCase ):
#  
#  def setUp( self ):   
#           
#    import DIRAC.ResourceStatusSystem.Client.ResourceManagementClient as mockedModule
#
#    _serviceIn = ResourceManagementHandler.ResourceManagementHandler()
#    self.client = mockedModule.ResourceManagementClient( serviceIn = _serviceIn )
#    
#  def tearDown( self ):  
#    
#    #sys.modules = self._modulesBkup
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Client.ResourceManagementClient' ]
#        