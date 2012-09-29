#import unittest, sys
#
#from DIRAC.ResourceStatusSystem.DB.mock      import ResourceStatusDB
#from DIRAC.ResourceStatusSystem.Service.mock import ResourceStatusHandler
#
#class Description_withDB( unittest.TestCase ):
#  
#  def setUp( self ):   
#        
#    import DIRAC.ResourceStatusSystem.Client.ResourceStatusClient as mockedModule
#
#    _serviceIn = ResourceStatusDB.ResourceStatusDB()
#    self.client = mockedModule.ResourceStatusClient( serviceIn = _serviceIn )
#    
#  def tearDown( self ):  
#    
#    #sys.modules = self._modulesBkup
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Client.ResourceStatusClient' ]
#    
#class Description_withHandler( unittest.TestCase ):
#  
#  def setUp( self ):   
#        
#    import DIRAC.ResourceStatusSystem.Client.ResourceStatusClient as mockedModule
#
#    _serviceIn = ResourceStatusHandler.ResourceStatusHandler()
#    self.client = mockedModule.ResourceStatusClient( serviceIn = _serviceIn )
#    
#  def tearDown( self ):  
#    
#    #sys.modules = self._modulesBkup
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Client.ResourceStatusClient' ]