import unittest, sys

from DIRAC.ResourceStatusSystem.Client.mock.JobsClient import PrivateJobsClient

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Client.JobsClient as mockedModule
    mockedModule.PrivateJobsClient = PrivateJobsClient
    
    #_module = "DIRAC.ResourceStatusSystem.Client.JobsClient"         
    #sys.modules[ _module ].PrivateJobsClient = PrivateJobsClient

    self.client = mockedModule.JobsClient()
    
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Client.JobsClient' ]
   
