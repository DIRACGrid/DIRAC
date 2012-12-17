import unittest, sys

#from DIRAC.ResourceStatusSystem.Client.mock.JobsClient import PrivateJobsClient

from DIRAC.ResourceStatusSystem.Command.mock.Command import Command

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   
     
    import DIRAC.ResourceStatusSystem.Command.AccountingCache_Command as mockedModule
    mockedModule.Command = Command
    
    #_module = "DIRAC.ResourceStatusSystem.Client.JobsClient"         
    #sys.modules[ _module ].PrivateJobsClient = PrivateJobsClient

    self.clients= {}
    self.clients[ 'TransferQualityByDestSplitted_Command' ]       = mockedModule.TransferQualityByDestSplitted_Command()
    self.clients[ 'TransferQualityByDestSplittedSite_Command' ]   = mockedModule.TransferQualityByDestSplittedSite_Command()
    self.clients[ 'TransferQualityBySourceSplittedSite_Command' ] = mockedModule.TransferQualityBySourceSplittedSite_Command()
    self.clients[ 'FailedTransfersBySourceSplitted_Command' ]     = mockedModule.FailedTransfersBySourceSplitted_Command()
    self.clients[ 'SuccessfullJobsBySiteSplitted_Command' ]       = mockedModule.SuccessfullJobsBySiteSplitted_Command()
    self.clients[ 'FailedJobsBySiteSplitted_Command' ]            = mockedModule.FailedJobsBySiteSplitted_Command()
    self.clients[ 'SuccessfullPilotsBySiteSplitted_Command' ]     = mockedModule.SuccessfullPilotsBySiteSplitted_Command()
    self.clients[ 'FailedPilotsBySiteSplitted_Command' ]          = mockedModule.FailedPilotsBySiteSplitted_Command()
    self.clients[ 'SuccessfullPilotsByCESplitted_Command' ]       = mockedModule.SuccessfullPilotsByCESplitted_Command()
    self.clients[ 'FailedPilotsByCESplitted_Command' ]            = mockedModule.FailedPilotsByCESplitted_Command()
    self.clients[ 'RunningJobsBySiteSplitted_Command' ]           = mockedModule.RunningJobsBySiteSplitted_Command() 
  
  def tearDown( self ):  
    
    #sys.modules = self._modulesBkup
    del sys.modules[ 'DIRAC.ResourceStatusSystem.Command.AccountingCache_Command' ]
  
  