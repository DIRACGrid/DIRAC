########################################################################
# $HeadURL $
# File: RequestDBTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/19 20:22:16
########################################################################

""" :mod: RequestDBTests
    =======================

    .. module: RequestDBTests
    :synopsis: unittest for RequestDB
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for RequestDB
"""

__RCSID__ = "$Id $"

# #
# @file RequestDBTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/19 20:22:29
# @brief Definition of RequestDBTests class.

# # imports
import unittest
import datetime
# # from DIRAC
from DIRAC import gConfig
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
# # SUT
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB

########################################################################
class RequestDBTests( unittest.TestCase ):
  """
  .. class:: RequestDBTests
  unittest for RequestDB
  """

  def setUp( self ):
    """ test case setup """
    self.request = Request( { "RequestName" : "test1", "JobID" : 1  } )
    self.operation1 = Operation( { "Type" : "ReplicateAndRegister", "TargetSE" : "CERN-USER" } )
    self.file = File( { "LFN" : "/a/b/c", "ChecksumType" : "ADLER32", "Checksum" : "123456" } )
    self.request.addOperation( self.operation1 )
    self.operation1.addFile( self.file )
    self.operation2 = Operation()
    self.operation2.Type = "RemoveFile"
    self.operation2.addFile( File( { "LFN" : "/c/d/e" } ) )
    self.request.addOperation( self.operation2 )

    # ## set some defaults
    gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
    gConfig.setOptionValue( '/DIRAC/Setups/Test/RequestManagement', 'Test' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/Host', 'localhost' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/DBName', 'ReqDB' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/User', 'Dirac' )

    self.i = 1000

  def tearDown( self ):
    """ test case tear down """
    del self.file
    del self.operation1
    del self.operation2
    del self.request

  def test01TableDesc( self ):
    """ table description """
    tableDict = RequestDB.getTableMeta()
    self.assertEqual( "Request" in tableDict, True )
    self.assertEqual( "Operation" in tableDict, True )
    self.assertEqual( "File" in tableDict, True )
    self.assertEqual( tableDict["Request"], Request.tableDesc() )
    self.assertEqual( tableDict["Operation"], Operation.tableDesc() )
    self.assertEqual( tableDict["File"], File.tableDesc() )

  def test03RequestRW( self ):
    """ db r/w requests """
    db = RequestDB()
    db._checkTables( True )

    # # empty DB at that stage
    ret = db.getDBSummary()
    self.assertEqual( ret,
                      { 'OK': True,
                        'Value': { 'Operation': {}, 'Request': {}, 'File': {} } } )

    # # insert
    ret = db.putRequest( self.request )
    self.assertEqual( ret, {'OK': True, 'Value': ''} )

    # # get digest -> JSON
    ret = db.getDigest( self.request.RequestName )
    self.assertEqual( ret["OK"], True )
    self.assertEqual( bool( ret["Value"] ), True )

    # # db summary
    ret = db.getDBSummary()
    self.assertEqual( ret,
                      { 'OK': True,
                        'Value': { 'Operation': { 'RemoveFile': { 'Queued': 1L },
                                                  'ReplicateAndRegister': { 'Waiting': 1L } },
                                   'Request': { 'Waiting': 1L },
                                   'File': {'Waiting': 2L } } } )

    # # get request for jobs
    ret = db.getRequestNamesForJobs( [ 1 ] )
    self.assertEqual( ret["OK"], True )
    self.assertEqual( ret["Value"], { 1 : 'test1'} )

    # # read requests
    ret = db.readRequestsForJobs( [1] )
    self.assertEqual( ret["OK"], True )
    self.assertEqual( ret["Value"][1]["OK"], True )

    # # select
    ret = db.getRequest()
    self.assertEqual( ret["OK"], True )
    request = ret["Value"]
    self.assertEqual( isinstance( request, Request ), True )

    # # summary
    ret = db.getDBSummary()
    self.assertEqual( ret,
                      { 'OK': True,
                        'Value': { 'Operation': { 'RemoveFile': { 'Queued': 1L },
                                                  'ReplicateAndRegister': { 'Waiting': 1L } },
                                   'Request': { 'Assigned': 1L },
                                   'File': { 'Waiting': 2L} } } )
    # # update
    ret = db.putRequest( request )
    self.assertEqual( ret, {'OK': True, 'Value': ''} )

    # # get summary again
    ret = db.getDBSummary()
    self.assertEqual( ret,
                      { 'OK': True,
                        'Value': { 'Operation': { 'RemoveFile': { 'Queued': 1L },
                                                  'ReplicateAndRegister': {'Waiting': 1L } },
                                   'Request': { 'Waiting': 1L },
                                   'File': { 'Waiting': 2L} } } )


    # # delete
    ret = db.deleteRequest( self.request.RequestName )
    self.assertEqual( ret, {'OK': True, 'Value': ''} )

    # # should be empty now
    ret = db.getDBSummary()
    self.assertEqual( ret,
                      { 'OK': True,
                        'Value': { 'Operation': {}, 'Request': {}, 'File': {} } } )

  def test04Stress( self ):
    """ stress test """

    db = RequestDB()

    for i in range( self.i ):
      request = Request( { "RequestName": "test-%d" % i } )
      op = Operation( { "Type": "RemoveReplica", "TargetSE": "CERN-USER" } )
      op += File( { "LFN": "/lhcb/user/c/cibak/foo" } )
      request += op
      put = db.putRequest( request )
      self.assertEqual( put["OK"], True, "put failed" )


    for i in range( self.i ):
      get = db.getRequest( "test-%s" % i, False )
      if "Message" in get:
        print get["Message"]
      self.assertEqual( get["OK"], True, "get failed" )

    for i in range( self.i ):
      delete = db.deleteRequest( "test-%s" % i )
      self.assertEqual( delete["OK"], True, "delete failed" )


  def test05Scheduled( self ):
    """ scheduled request r/w """

    db = RequestDB()

    req = Request( {"RequestName": "FTSTest"} )
    op = Operation( { "Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"} )
    op += File( {"LFN": "/a/b/c", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32" } )
    req += op

    put = db.putRequest( req )
    self.assertEqual( put["OK"], True, "putRequest failed" )

    peek = db.peekRequest( req.RequestName )
    self.assertEqual( peek["OK"], True, "peek failed " )

    peek = peek["Value"]
    for op in peek:
      opId = op.OperationID

    getFTS = db.getScheduledRequest( opId )
    self.assertEqual( getFTS["OK"], True, "getScheduled failed" )
    self.assertEqual( getFTS["Value"].RequestName, "FTSTest", "wrong request selected" )


# # test suite execution
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( RequestDBTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( gSuite )
