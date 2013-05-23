########################################################################
# $HeadURL $
# File: FTSDBTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/18 15:22:52
########################################################################

""" :mod: FTSDBTests
    ================

    .. module: FTSDBTests
    :synopsis: unittests for FTSDB
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittests for FTSDB
"""

__RCSID__ = "$Id $"

# #
# @file FTSDBTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/18 15:23:03
# @brief Definition of FTSDBTests class.

# # imports
import unittest
import uuid
import random
from DIRAC import gConfig
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
# # SUT
from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB

########################################################################
class FTSDBTests( unittest.TestCase ):
  """
  .. class:: FTSDBTests

  """

  def setUp( self ):
    """ test case set up """
    # ## set some defaults
    gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
    gConfig.setOptionValue( '/DIRAC/Setups/Test/DataManagement', 'Test' )
    gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/FTSDB/Host', 'localhost' )
    gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/FTSDB/DBName', 'FTSDB' )
    gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/FTSDB/User', 'Dirac' )

    self.ftsSites = [ FTSSite( { "FTSServer": "https://fts22-t0-export.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer", "Name": "CERN.ch" } ),
                      FTSSite( { "FTSServer": "https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer", "Name": "PIC.es" } ),
                      FTSSite( { "FTSServer": "https://lcgfts.gridpp.rl.ac.uk:8443/glite-data-transfer-fts/services/FileTransfer", "Name": "RAL.uk" } ),
                      FTSSite( { "FTSServer": "https://fts.grid.sara.nl:8443/glite-data-transfer-fts/services/FileTransfer", "Name": "SARA.nl" } ),
                      FTSSite( { "FTSServer": "https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer", "Name": "CNAF.it" } ),
                      FTSSite( { "FTSServer": "https://fts.grid.sara.nl:8443/glite-data-transfer-fts/services/FileTransfer", "Name": "NIKHEF.nl" } ),
                      FTSSite( { "FTSServer": "https://fts-fzk.gridka.de:8443/glite-data-transfer-fts/services/FileTransfer", "Name": "GRIDKA.de" } ),
                      FTSSite( { "FTSServer": "https://cclcgftsprod.in2p3.fr:8443/glite-data-transfer-fts/services/FileTransfer", "Name": "IN2P3.fr" } ) ]


    self.ftsFiles = []
    for i in range ( 100 ):
      ftsFile = FTSFile()
      ftsFile.FileID = i + 1
      ftsFile.OperationID = 9999
      ftsFile.LFN = "/a/b/c/%d" % i
      ftsFile.Size = 10
      ftsFile.SourceSE = "CERN-USER"
      ftsFile.TargetSE = "PIC-USER"
      ftsFile.SourceSURL = "foo://source.bar.baz/%s" % ftsFile.LFN
      ftsFile.TargetSURL = "foo://target.bar.baz/%s" % ftsFile.LFN
      ftsFile.Status = "Waiting"
      self.ftsFiles.append( ftsFile )

    ses = [ "CERN-USER", "RAL-USER" ]
    statuses = [ "Submitted", "Finished", "FisnihedDirty" ]

    self.submitted = 0

    self.ftsJobs = []
    for i in range( 7200 ):

      ftsJob = FTSJob()
      ftsJob.FTSGUID = str( uuid.uuid4() )
      ftsJob.FTSServer = self.ftsSites[0].FTSServer
      ftsJob.Status = statuses[ i % len( statuses ) ]

      if ftsJob.Status in FTSJob.FINALSTATES:
        ftsJob.Completeness = 100
      if ftsJob.Status == "Active":
        ftsJob.Completeness = 90

      ftsJob.SourceSE = ses[ i % len( ses ) ]
      ftsJob.TargetSE = "PIC-USER"

      ftsFile = FTSFile()
      ftsFile.FileID = i + 1
      ftsFile.OperationID = i + 1
      ftsFile.LFN = "/a/b/c/%d" % i
      ftsFile.Size = 1000000
      ftsFile.SourceSE = ftsJob.SourceSE
      ftsFile.TargetSE = ftsJob.TargetSE
      ftsFile.SourceSURL = "foo://source.bar.baz/%s" % ftsFile.LFN
      ftsFile.TargetSURL = "foo://target.bar.baz/%s" % ftsFile.LFN
      ftsFile.Status = "Waiting" if ftsJob.Status != "FinishedDirty" else "Failed"

      ftsFile.FTSGUID = ftsJob.FTSGUID
      if ftsJob.Status == "FinishedDirty":
        ftsJob.FailedFiles = 1
        ftsJob.FailedSize = ftsFile.Size

      ftsJob.addFile( ftsFile )
      self.ftsJobs.append( ftsJob )

    self.submitted = len( [ i for i in self.ftsJobs if i.Status == "Submitted" ] )

  def tearDown( self ):
    """ clean up """
    del self.ftsFiles
    del self.ftsJobs
    del self.ftsSites

  def test01Create( self ):
    """ test create tables and views """
    db = FTSDB()
    createTables = db.createTables( ["FTSSite", "FTSFile", "FTSJob"], True )
    self.assertEqual( createTables["OK"], True, "tables creation error: %s" % createTables.get( "Message", "" ) )
    createViews = db.createViews( True )
    self.assertEqual( createViews["OK"], True, "views creation error: %s" % createViews.get( "Message" , "" ) )

  def test02PutGetDelete( self ):
    """ put, get, peek methods """

    db = FTSDB()

    print "put site"
    for ftsSite in self.ftsSites:
      put = db.putFTSSite( ftsSite )
      self.assertEqual( put["OK"], True, "putFTSSite failed" )
      put = db.putFTSSite( ftsSite )
      self.assertEqual( put["OK"], False, "re-putFTSSite failed" )

    print "getFTSSite"
    for i, ftsSite in enumerate( self.ftsSites ):
      get = db.getFTSSite( i + 1 )
      self.assertEqual( get["OK"], True, "getFTSSite failed" )
      self.assertEqual( isinstance( get["Value"], FTSSite ), True, "getFTSSite wrong value returned" )

    print "getFTSSiteList"
    getFTSSitesList = db.getFTSSitesList()
    self.assertEqual( getFTSSitesList["OK"], True, "getFTSSitesList failed" )
    for item in getFTSSitesList["Value"]:
      self.assertEqual( isinstance( item, FTSSite ), True, "getFTSSitesList wrong value returned" )

    print "putFile"
    for ftsFile in self.ftsFiles:
      put = db.putFTSFile( ftsFile )
      self.assertEqual( put["OK"], True, "putFTSFile failed" )

    print "peekFile"
    for i in range( 1, 101 ):
      peek = db.peekFTSFile( i )
      self.assertEqual( peek["OK"], True, "peekFTSFile failed" )
      self.assertEqual( isinstance( peek["Value"], FTSFile ), True, "peekFTSFile wrong value" )

    print "getFile"
    for i in range( 1, 101 ):
      get = db.getFTSFile( i )
      self.assertEqual( get["OK"], True, "getFTSFile failed" )
      self.assertEqual( isinstance( get["Value"], FTSFile ), True, "getFTSFile wrong value" )

    print "putJob"
    for ftsJob in self.ftsJobs:
      put = db.putFTSJob( ftsJob )
      self.assertEqual( put["OK"], True, "putFTSJob failed" )

    print "peekJob"
    for i in range( 1, 101 ):
      peek = db.peekFTSJob( i )
      self.assertEqual( peek["OK"], True, "peekFTSJob failed" )
      self.assertEqual( isinstance( peek["Value"], FTSJob ), True, "peekFTSJob wrong value returned" )
      self.assertEqual( len( peek["Value"] ), 1, "peekFTSJob wrong number of files " )

    print "getJob"
    for i in range( 1, 101 ):
      get = db.getFTSJob( i )
      self.assertEqual( get["OK"], True, "getFTSJob failed" )
      self.assertEqual( isinstance( get["Value"], FTSJob ), True, "getFTSJob wrong value returned" )
      self.assertEqual( len( get["Value"] ), 1, "getFTSJob wrong number of files" )

    print "duSummary"
    summary = db.getDBSummary()
    self.assertEqual( summary["OK"], True, "getDBSummary failed" )
    self.assertEqual( "FTSJob" in summary["Value"], True, "getDBSummary FTSJob missing" )
    self.assertEqual( "FTSFile" in summary["Value"], True, "getDBSummary FTSFile missing" )
    self.assertEqual( "FTSHistory" in summary["Value"], True, "getDBSummary FTSHistory missing" )
    self.assertEqual( "FTSSite" in summary["Value"], True, "getDBSummary FTSSite missing" )

  def test03FTSHistory( self ):
    """ history view """
    db = FTSDB()
    ret = db.getFTSHistory()
    self.assertEqual( ret["OK"], True, "getFTSHistory failed" )
    for ftsHistory in ret["Value"]:
      self.assertEqual( isinstance( ftsHistory, FTSHistoryView ), True, "getFTSHistory wrong instance" )

  def test04GetFTSIDs( self ):
    """ get ids """
    db = FTSDB()

    ftsJobIDs = db.getFTSJobIDs( [ "Submitted" ] )
    self.assertEqual( ftsJobIDs["OK"], True, "getFTSJobIDs error" )
    self.assertEqual( len( ftsJobIDs["Value"] ), self.submitted, "getFTSJobIDs wrong value returned" )

    ftsFileIDs = db.getFTSFileIDs( ["Waiting"] )
    self.assertEqual( ftsFileIDs["OK"], True, "getFTSFileIDs error" )
    self.assertEqual( type( ftsFileIDs["Value"] ), list, "getFTSFileIDs wrong value returned" )

    ftsFileList = db.getFTSFileList( ["Waiting.*" ] )
    self.assertEqual( ftsFileList["OK"], True, "getFTSFileList error" )
    self.assertEqual( type( ftsFileList["Value"] ), list, "fgetFTSFileList wrong value returned" )
    print "read ", len( ftsFileList["Value"] ), " records"

  def test05Delete( self ):
    """ delete files and jobs """

    db = FTSDB()

    for i in range( 1, 301 ):
      delete = db.deleteFTSFile( i )
      self.assertEqual( delete["OK"], True, "deleleFTSFile failed" )

    # for i in range( 1, 201 ):
    #  delete = db.deleteFTSJob( i )
    #  self.assertEqual( delete["OK"], True, "deleleFTSJob failed" )

    summary = db.getDBSummary()
    self.assertEqual( summary["OK"], True, "getDBSummary failed" )
    self.assertEqual( "FTSJob" in summary["Value"], True, "getDBSummary FTSJob missing" )
    self.assertEqual( "FTSFile" in summary["Value"], True, "getDBSummary FTSFile missing" )
    self.assertEqual( "FTSHistory" in summary["Value"], True, "getDBSummary FTSHistory missing" )
    self.assertEqual( "FTSSite" in summary["Value"], True, "getDBSummary FTSSite missing" )


# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSDBTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )


