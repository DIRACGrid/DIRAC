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



    # # read requests
    ret = db.readRequestsForJobs( [1] )
    self.assertEqual( ret["OK"], True )
    self.assertEqual( ret["Value"][1]["OK"], True )



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


  def test06Dirty( self ):
    """ dirty records """
    db = RequestDB()

    r = Request()
    r.RequestName = "dirty"

    op1 = Operation( { "Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"} )
    op1 += File( {"LFN": "/a/b/c/1", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32" } )

    op2 = Operation( { "Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"} )
    op2 += File( {"LFN": "/a/b/c/2", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32" } )

    op3 = Operation( { "Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"} )
    op3 += File( {"LFN": "/a/b/c/3", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32" } )

    r += op1
    r += op2
    r += op3

    put = db.putRequest( r )
    self.assertEqual( put["OK"], True, "1. putRequest failed: %s" % put.get( "Message", "" ) )


    r = db.getRequest( "dirty" )
    self.assertEqual( r["OK"], True, "1. getRequest failed: %s" % r.get( "Message", "" ) )
    r = r["Value"]

    del r[0]
    self.assertEqual( len( r ), 2, "1. len wrong" )

    put = db.putRequest( r )
    self.assertEqual( put["OK"], True, "2. putRequest failed: %s" % put.get( "Message", "" ) )

    r = db.getRequest( "dirty" )
    self.assertEqual( r["OK"], True, "2. getRequest failed: %s" % r.get( "Message", "" ) )

    r = r["Value"]
    self.assertEqual( len( r ), 2, "2. len wrong" )

    op4 = Operation( { "Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"} )
    op4 += File( {"LFN": "/a/b/c/4", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32" } )

    r[0] = op4
    put = db.putRequest( r )
    self.assertEqual( put["OK"], True, "3. putRequest failed: %s" % put.get( "Message", "" ) )

    r = db.getRequest( "dirty" )
    self.assertEqual( r["OK"], True, "3. getRequest failed: %s" % r.get( "Message", "" ) )
    r = r["Value"]

    self.assertEqual( len( r ), 2, "3. len wrong" )



# # test suite execution
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( RequestDBTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( gSuite )
