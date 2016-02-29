""" This is a test of the chain
    FTSClient -> FTSManagerHandler -> FTSDB

    It supposes that the DB is present, and that the service is running
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest, mock
import uuid

from DIRAC import gLogger
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
# from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
# # # SUT
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient


class FTSDBTestCase( unittest.TestCase ):
  """
  .. class:: FTSDBTests

  """

  def setUp( self ):
    """ test case set up """

    gLogger.setLevel( 'NOTICE' )

    self.ftsSites = [ FTSSite( ftsServer = 'https://fts22-t0-export.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer', name = 'CERN.ch' ),
                      FTSSite( ftsServer = 'https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer', name = 'PIC.es' ),
                      FTSSite( ftsServer = 'https://lcgfts.gridpp.rl.ac.uk:8443/glite-data-transfer-fts/services/FileTransfer', name = 'RAL.uk' ),
                    ]

    self.ses = [ 'CERN-USER', 'RAL-USER' ]
    self.statuses = [ 'Submitted', 'Finished', 'FinishedDirty', 'Active', 'Ready' ]

    self.submitted = 0
    self.numberOfJobs = 10
    self.opIDs = []

    self.ftsJobs = []
    for i in xrange( self.numberOfJobs ):

      opID = i % 3
      if opID not in self.opIDs:
        self.opIDs.append( opID )

      ftsJob = FTSJob()
      ftsJob.FTSGUID = str( uuid.uuid4() )
      ftsJob.FTSServer = self.ftsSites[0].FTSServer
      ftsJob.Status = self.statuses[ i % len( self.statuses ) ]
      ftsJob.OperationID = opID
      if ftsJob.Status in FTSJob.FINALSTATES:
        ftsJob.Completeness = 100
      if ftsJob.Status == 'Active':
        ftsJob.Completeness = 90
      ftsJob.SourceSE = self.ses[ i % len( self.ses ) ]
      ftsJob.TargetSE = 'PIC-USER'
      ftsJob.RequestID = 12345

      ftsFile = FTSFile()
      ftsFile.FileID = i + 1
      ftsFile.OperationID = i + 1
      ftsFile.LFN = '/a/b/c/%d' % i
      ftsFile.Size = 1000000
      ftsFile.OperationID = opID
      ftsFile.SourceSE = ftsJob.SourceSE
      ftsFile.TargetSE = ftsJob.TargetSE
      ftsFile.SourceSURL = 'foo://source.bar.baz/%s' % ftsFile.LFN
      ftsFile.TargetSURL = 'foo://target.bar.baz/%s' % ftsFile.LFN
      ftsFile.Status = 'Waiting' if ftsJob.Status != 'FinishedDirty' else 'Failed'
      ftsFile.RequestID = 12345
      ftsFile.Checksum = 'addler'
      ftsFile.ChecksumType = 'adler32'

      ftsFile.FTSGUID = ftsJob.FTSGUID
      if ftsJob.Status == 'FinishedDirty':
        ftsJob.FailedFiles = 1
        ftsJob.FailedSize = ftsFile.Size

      ftsJob.addFile( ftsFile )
      self.ftsJobs.append( ftsJob )

    self.submitted = len( [ i for i in self.ftsJobs if i.Status == 'Submitted' ] )

    self.ftsClient = FTSClient()
    self.ftsClient.replicaManager = mock.Mock()
    self.ftsClient.replicaManager.getActiveReplicas.return_value = {'OK': True,
                                                                    'Value': {'Successful': {'/a/b/c/1':{'CERN-USER':'/aa/a/b/c/1d',
                                                                                                         'RAL-USER':'/bb/a/b/c/1d'},
                                                                                             '/a/b/c/2':{'CERN-USER':'/aa/a/b/c/2d',
                                                                                                         'RAL-USER':'/bb/a/b/c/2d'},
                                                                                             '/a/b/c/3':{'CERN-USER':'/aa/a/b/c/3d',
                                                                                                         'RAL-USER':'/bb/a/b/c/3d'}
                                                                                             },
                                                                              'Failed': {'/a/b/c/4':'/aa/a/b/c/4d',
                                                                                         '/a/b/c/5':'/aa/a/b/c/5d'}
                                                                              }
                                                                    }

  def tearDown( self ):
    """ clean up """
    del self.ftsJobs
    del self.ftsSites


class FTSClientChain( FTSDBTestCase ):

  def test_addAndRemoveJobs( self ):
    """ put, get, peek, delete jobs methods """

    print 'putJob'
    for ftsJob in self.ftsJobs:
      put = self.ftsClient.putFTSJob( ftsJob )
      self.assertEqual( put['OK'], True )

    print 'getFTSJobIDs'
    res = self.ftsClient.getFTSJobIDs( self.statuses )
    self.assertEqual( res['OK'], True )
    self.assertEqual( len( res['Value'] ), self.numberOfJobs )
    FTSjobIDs = res['Value']

    print 'getFTSJobList'
    self.ftsClient.getFTSJobList( self.statuses, self.numberOfJobs )
    self.assertEqual( res['OK'], True )
    self.assertEqual( len( res['Value'] ), self.numberOfJobs )

    print 'peekJob'
    for i in FTSjobIDs:
      peek = self.ftsClient.peekFTSJob( i )
      self.assertEqual( peek['OK'], True )
      self.assertEqual( len( peek['Value']['FTSFiles'] ), 1 )

    print 'getJob'
    for i in FTSjobIDs:
      get = self.ftsClient.getFTSJob( i )
      self.assertEqual( get['OK'], True )
      self.assertEqual( len( get['Value']['FTSFiles'] ), 1 )

    print 'getFTSFileIDs'
    res = self.ftsClient.getFTSFileIDs()
    self.assertEqual( res['OK'], True )
    FTSfileIDs = res['Value']

    print 'getFTSFileList'
    res = self.ftsClient.getFTSFileList()
    self.assertEqual( res['OK'], True )

    print 'peekFTSFile'
    for i in FTSfileIDs:
      peek = self.ftsClient.peekFTSFile( i )
      self.assertEqual( peek['OK'], True )

    print 'getFTSFile'
    for i in FTSfileIDs:
      res = self.ftsClient.getFTSFile( i )
      self.assertEqual( res['OK'], True )

    print 'deleteJob'
    for i in FTSjobIDs:
      delete = self.ftsClient.deleteFTSJob( i )
      self.assertEqual( delete['OK'], True )

    print 'deleteFiles'
    for i in self.opIDs:
      res = self.ftsClient.deleteFTSFiles( i )
      self.assert_( res['OK'] )

class FTSClientMix( FTSDBTestCase ):

  def test_mix( self ):
    """ all the other tests"""
    opFileList = []
    for ftsJob in self.ftsJobs:
      self.ftsClient.putFTSJob( ftsJob )
      opFileList.append( ( ftsJob[0].toJSON()["Value"], self.ses, self.ses ) )

#    ftsSchedule can't work since the FTSStrategy object is refreshed in the service so it can't be mocked
#    for opID in self.opIDs:
#      res = self.ftsClient.ftsSchedule( 12345, opID, opFileList )
#      self.assert_( res['OK'] )

    print 'setFTSFilesWaiting'
    for operationID in self.opIDs:
      for sourceSE in self.ses:
        res = self.ftsClient.setFTSFilesWaiting( operationID, sourceSE )
        self.assertEqual( res['OK'], True )

    print 'getFTSHistory'
    res = self.ftsClient.getFTSHistory()
    self.assertEqual( res['OK'], True )
    self.assert_( type( res['Value'] ) == type( [] ) )

    print 'getFTSJobsForRequest'
    res = self.ftsClient.getFTSJobsForRequest( 12345 )
    self.assertEqual( res['OK'], True )

    print 'getFTSFilesForRequest'
    res = self.ftsClient.getFTSFilesForRequest( 12345 )
    self.assertEqual( res['OK'], True )

    print 'getDBSummary'
    res = self.ftsClient.getDBSummary()
    self.assertEqual( res['OK'], True )

    FTSjobIDs = self.ftsClient.getFTSJobIDs( self.statuses )['Value']
    print 'deleteJob'
    for i in FTSjobIDs:
      delete = self.ftsClient.deleteFTSJob( i )
      self.assertEqual( delete['OK'], True )

    print 'deleteFiles'
    for i in self.opIDs:
      res = self.ftsClient.deleteFTSFiles( i )
      self.assert_( res['OK'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( FTSDBTestCase )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FTSClientChain ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FTSClientMix ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
