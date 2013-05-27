########################################################################
# $HeadURL$
# File: RequestTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 10:23:40
########################################################################

""" :mod: RequestTests
    =======================

    .. module: RequestTests
    :synopsis: test cases for Request class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for Request class
"""

__RCSID__ = "$Id$"

# #
# @file RequestTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 10:23:52
# @brief Definition of RequestTests class.

# # imports
import unittest
import datetime
# # from DIRAC
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
# # SUT
from DIRAC.RequestManagementSystem.Client.Request import Request

########################################################################
class RequestTests( unittest.TestCase ):
  """
  .. class:: RequestTests

  """

  def setUp( self ):
    """ set up """
    self.fromDict = { "RequestName" : "test", "JobID" : 12345 }

  def tearDown( self ):
    """ tear down """
    del self.fromDict

  def test01CtorSerilization( self ):
    """ c'tor and serialization """
    # # empty c'tor
    req = Request()
    self.assertEqual( isinstance( req, Request ), True )
    self.assertEqual( req.JobID, 0 )
    self.assertEqual( req.Status, "Waiting" )

    req = Request( self.fromDict )
    self.assertEqual( isinstance( req, Request ), True )
    self.assertEqual( req.RequestName, "test" )
    self.assertEqual( req.JobID, 12345 )
    self.assertEqual( req.Status, "Waiting" )

    toJSON = req.toJSON()
    self.assertEqual( toJSON["OK"], True, "JSON serialization failed" )

    fromJSON = toJSON["Value"]
    req = Request( fromJSON )

    toSQL = req.toSQL()
    self.assertEqual( toSQL["OK"], True )
    toSQL = toSQL["Value"]
    self.assertEqual( toSQL.startswith( "INSERT" ), True )

    req.RequestID = 1

    toSQL = req.toSQL()
    self.assertEqual( toSQL["OK"], True )
    toSQL = toSQL["Value"]
    self.assertEqual( toSQL.startswith( "UPDATE" ), True )

  def test02Props( self ):
    """ props """
    # # valid values
    req = Request()

    req.RequestID = 1
    self.assertEqual( req.RequestID, 1 )

    req.RequestName = "test"
    self.assertEqual( req.RequestName, "test" )

    req.JobID = 1
    self.assertEqual( req.JobID, 1 )
    req.JobID = "1"
    self.assertEqual( req.JobID, 1 )

    req.CreationTime = "1970-01-01 00:00:00"
    self.assertEqual( req.CreationTime, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )
    req.CreationTime = datetime.datetime( 1970, 1, 1, 0, 0, 0 )
    self.assertEqual( req.CreationTime, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )

    req.SubmitTime = "1970-01-01 00:00:00"
    self.assertEqual( req.SubmitTime, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )
    req.SubmitTime = datetime.datetime( 1970, 1, 1, 0, 0, 0 )
    self.assertEqual( req.SubmitTime, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )

    req.LastUpdate = "1970-01-01 00:00:00"
    self.assertEqual( req.LastUpdate, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )
    req.LastUpdate = datetime.datetime( 1970, 1, 1, 0, 0, 0 )
    self.assertEqual( req.LastUpdate, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )

    req.Error = ""

  def test04Operations( self ):
    """ operations arithmetic and state machine """
    req = Request()
    self.assertEqual( len( req ), 0 )

    transfer = Operation()
    transfer.Type = "ReplicateAndRegister"
    transfer.addFile( File( { "LFN" : "/a/b/c", "Status" : "Waiting" } ) )

    getWaiting = req.getWaiting()
    self.assertEqual( getWaiting["OK"], True )
    self.assertEqual( getWaiting["Value"], None )

    req.addOperation( transfer )
    self.assertEqual( len( req ), 1 )
    self.assertEqual( transfer.Order, req.Order )
    self.assertEqual( transfer.Status, "Waiting" )

    getWaiting = req.getWaiting()
    self.assertEqual( getWaiting["OK"], True )
    self.assertEqual( getWaiting["Value"], transfer )

    removal = Operation( { "Type" : "RemoveFile" } )
    removal.addFile( File( { "LFN" : "/a/b/c", "Status" : "Waiting" } ) )

    req.insertBefore( removal, transfer )

    getWaiting = req.getWaiting()
    self.assertEqual( getWaiting["OK"], True )
    self.assertEqual( getWaiting["Value"], removal )

    self.assertEqual( len( req ), 2 )
    self.assertEqual( [ op.Status for op in req ], ["Waiting", "Queued"] )
    self.assertEqual( req.subStatusList() , ["Waiting", "Queued"] )


    self.assertEqual( removal.Order, 0 )
    self.assertEqual( removal.Order, req.Order )

    self.assertEqual( transfer.Order, 1 )

    self.assertEqual( removal.Status, "Waiting" )
    self.assertEqual( transfer.Status, "Queued" )

    for subFile in removal:
      subFile.Status = "Done"
    removal.Status = "Done"

    self.assertEqual( removal.Status, "Done" )

    self.assertEqual( transfer.Status, "Waiting" )
    self.assertEqual( transfer.Order, req.Order )

    # # len, looping
    self.assertEqual( len( req ), 2 )
    self.assertEqual( [ op.Status for op in req ], ["Done", "Waiting"] )
    self.assertEqual( req.subStatusList() , ["Done", "Waiting"] )

    digest = req.toJSON()
    self.assertEqual( digest["OK"], True )

    getWaiting = req.getWaiting()
    self.assertEqual( getWaiting["OK"], True )
    self.assertEqual( getWaiting["Value"], transfer )

  def test05FTS( self ):
    """ FTS state machine """

    req = Request()
    req.RequestName = "FTSTest"

    ftsTransfer = Operation()
    ftsTransfer.Type = "ReplicateAndRegister"
    ftsTransfer.TargetSE = "CERN-USER"

    ftsFile = File()
    ftsFile.LFN = "/a/b/c"
    ftsFile.Checksum = "123456"
    ftsFile.ChecksumType = "Adler32"

    ftsTransfer.addFile( ftsFile )
    req.addOperation( ftsTransfer )

    self.assertEqual( req.Status, "Waiting", "1. wrong request status: %s" % req.Status )
    self.assertEqual( ftsTransfer.Status, "Waiting", "1. wrong ftsStatus status: %s" % ftsTransfer.Status )

    # # scheduled
    ftsFile.Status = "Scheduled"

    self.assertEqual( ftsTransfer.Status, "Scheduled", "2. wrong status for ftsTransfer: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Scheduled", "2. wrong status for request: %s" % req.Status )

    # # add new operation before FTS
    insertBefore = Operation()
    insertBefore.Type = "RegisterReplica"
    insertBefore.TargetSE = "CERN-USER"
    insertFile = File()
    insertFile.LFN = "/a/b/c"
    insertFile.PFN = "http://foo/bar"
    insertBefore.addFile( insertFile )
    req.insertBefore( insertBefore, ftsTransfer )

    self.assertEqual( insertBefore.Status, "Waiting", "3. wrong status for insertBefore: %s" % insertBefore.Status )
    self.assertEqual( ftsTransfer.Status, "Scheduled", "3. wrong status for ftsStatus: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Waiting", "3. wrong status for request: %s" % req.Status )

    # # prev done
    insertFile.Status = "Done"

    self.assertEqual( insertBefore.Status, "Done", "4. wrong status for insertBefore: %s" % insertBefore.Status )
    self.assertEqual( ftsTransfer.Status, "Scheduled", "4. wrong status for ftsStatus: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Scheduled", "4. wrong status for request: %s" % req.Status )

    # # reschedule
    ftsFile.Status = "Waiting"

    self.assertEqual( insertBefore.Status, "Done", "5. wrong status for insertBefore: %s" % insertBefore.Status )
    self.assertEqual( ftsTransfer.Status, "Waiting", "5. wrong status for ftsStatus: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Waiting", "5. wrong status for request: %s" % req.Status )

    # # fts done
    ftsFile.Status = "Done"

    self.assertEqual( insertBefore.Status, "Done", "5. wrong status for insertBefore: %s" % insertBefore.Status )
    self.assertEqual( ftsTransfer.Status, "Done", "5. wrong status for ftsStatus: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Done", "5. wrong status for request: %s" % req.Status )


  def test06StateMachine( self ):
    """ state machine tests """
    r = Request( {"RequestName": "SMT"} )
    self.assertEqual( r.Status, "Waiting", "1. wrong status %s" % r.Status )

    r.addOperation( Operation( {"Status": "Queued"} ) )
    self.assertEqual( r.Status, "Waiting", "2. wrong status %s" % r.Status )

    r.addOperation( Operation( {"Status": "Queued"} ) )
    self.assertEqual( r.Status, "Waiting", "3. wrong status %s" % r.Status )

    r[0].Status = "Done"
    self.assertEqual( r.Status, "Waiting", "4. wrong status %s" % r.Status )

    r[1].Status = "Done"
    self.assertEqual( r.Status, "Done", "5. wrong status %s" % r.Status )

    r[0].Status = "Failed"
    self.assertEqual( r.Status, "Failed", "6. wrong status %s" % r.Status )

    r[0].Status = "Queued"
    self.assertEqual( r.Status, "Waiting", "7. wrong status %s" % r.Status )

    r.insertBefore( Operation( {"Status": "Queued"} ), r[0] )
    self.assertEqual( r.Status, "Waiting", "8. wrong status %s" % r.Status )

    r.insertBefore( Operation( {"Status": "Queued"} ), r[0] )
    self.assertEqual( r.Status, "Waiting", "9. wrong status %s" % r.Status )

    r.insertBefore( Operation( {"Status": "Scheduled"} ), r[0] )
    self.assertEqual( r.Status, "Scheduled", "10. wrong status %s" % r.Status )

    r.insertBefore( Operation( {"Status": "Queued" } ), r[0] )
    self.assertEqual( r.Status, "Waiting", "11. wrong status %s" % r.Status )

    r[0].Status = "Failed"
    self.assertEqual( r.Status, "Failed", "12. wrong status %s" % r.Status )

    r[0].Status = "Done"
    self.assertEqual( r.Status, "Scheduled", "13. wrong status %s" % r.Status )

    r[1].Status = "Failed"
    self.assertEqual( r.Status, "Failed", "14. wrong status %s" % r.Status )

    r[1].Status = "Done"
    self.assertEqual( r.Status, "Waiting", "15. wrong status %s" % r.Status )


    r[2].Status = "Scheduled"
    print r.Status, r.subStatusList()

    r[2].Status = "Queued"
    print r.Status, r.subStatusList()

    r[2].Status = "Scheduled"
    print r.Status, r.subStatusList()


    r = Request()
    for i in range( 5 ):
      r.addOperation( Operation( {"Status": "Queued" } ) )

    r[0].Status = "Done"

    print r.Status, r.subStatusList()

    r[1].Status = "Done"
    
    
    print r.Status, r.subStatusList()


    r[2].Status = "Scheduled"
    

    print r.Status, r.subStatusList()


    r[2].Status = "Done"


    print r.Status, r.subStatusList()


  # def test07JSON( self ):
  #  """ """
  #  fromDict = {'Operations': [{'Files': [{'Status': 'Waiting', 'Attempt': '', 'ChecksumType': '', 'Checksum': '', 'LFN': '/lhcb/user/c/cibak/testPutAndRegister', 'PFN': '', 'Size': '', 'Error': '', 'GUID': '', 'OperationID': '0', 'FileID': 0}], 'LastUpdate': '2013-05-24 08:04:48', 'Status': 'Waiting', 'TargetSE': '', 'RequestID': '0', 'CreationTime': '2013-05-24 08:04:48', 'SourceSE': '', 'SubmitTime': '2013-05-24 08:04:48', 'Catalog': '', 'Arguments': '', 'Error': '', 'Type': 'RemoveFile', 'Order': '0', 'OperationID': ''}, {'Files': [{'Status': 'Waiting', 'Attempt': '', 'ChecksumType': 'ADLER32', 'Checksum': '90d520ff', 'LFN': '/lhcb/user/c/cibak/testPutAndRegister', 'PFN': '/tmp/testPutAndRegister', 'Size': '164', 'Error': '', 'GUID': 'C8E0DBDA-75AB-283F-7603-EC3ED629752E', 'OperationID': '0', 'FileID': 0}], 'LastUpdate': '2013-05-24 08:04:48', 'Status': 'Queued', 'TargetSE': 'RAL-USER', 'RequestID': '0', 'CreationTime': '2013-05-24 08:04:48', 'SourceSE': '', 'SubmitTime': '2013-05-24 08:04:48', 'Catalog': '', 'Arguments': '', 'Error': '', 'Type': 'PutAndRegister', 'Order': '1', 'OperationID': ''}, {'Files': [{'Status': 'Waiting', 'Attempt': '', 'ChecksumType': 'ADLER32', 'Checksum': '90d520ff', 'LFN': '/lhcb/user/c/cibak/testPutAndRegister', 'PFN': '', 'Size': '164', 'Error': '', 'GUID': '', 'OperationID': '0', 'FileID': 0}], 'LastUpdate': '2013-05-24 08:04:48', 'Status': 'Queued', 'TargetSE': 'RAL-USER,PIC-USER', 'RequestID': '0', 'CreationTime': '2013-05-24 08:04:48', 'SourceSE': '', 'SubmitTime': '2013-05-24 08:04:48', 'Catalog': '', 'Arguments': '', 'Error': '', 'Type': 'ReplicateAndRegister', 'Order': '2', 'OperationID': ''}, {'Files': [{'Status': 'Waiting', 'Attempt': '', 'ChecksumType': '', 'Checksum': '', 'LFN': '/lhcb/user/c/cibak/testPutAndRegister', 'PFN': '', 'Size': '', 'Error': '', 'GUID': '', 'OperationID': '0', 'FileID': 0}], 'LastUpdate': '2013-05-24 08:04:48', 'Status': 'Queued', 'TargetSE': 'RAL-USER', 'RequestID': '0', 'CreationTime': '2013-05-24 08:04:48', 'SourceSE': '', 'SubmitTime': '2013-05-24 08:04:48', 'Catalog': '', 'Arguments': '', 'Error': '', 'Type': 'RemoveReplica', 'Order': '3', 'OperationID': ''}, {'Files': [{'Status': 'Waiting', 'Attempt': '', 'ChecksumType': '', 'Checksum': '', 'LFN': '/lhcb/user/c/cibak/testPutAndRegister', 'PFN': '', 'Size': '', 'Error': '', 'GUID': '', 'OperationID': '0', 'FileID': 0}], 'LastUpdate': '2013-05-24 08:04:48', 'Status': 'Queued', 'TargetSE': '', 'RequestID': '0', 'CreationTime': '2013-05-24 08:04:48', 'SourceSE': '', 'SubmitTime': '2013-05-24 08:04:48', 'Catalog': '', 'Arguments': '', 'Error': '', 'Type': 'RemoveFile', 'Order': '4', 'OperationID': ''}], 'LastUpdate': '2013-05-24 08:04:48', 'Status': 'Waiting', 'OwnerGroup': 'dirac_user', 'DIRACSetup': '', 'CreationTime': '2013-05-24 08:04:48', 'JobID': '', 'OwnerDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba', 'SubmitTime': '2013-05-24 08:04:48', 'RequestID': 0, 'Error': '', 'SourceComponent': '', 'RequestName': 'fullChain'}
  #
  #  r = Request( fromDict )
  #  print r.subStatusList()
  #  for op in r:
  #    print op
  #    for f in op:
  #      print f


# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( RequestTests )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
