########################################################################
# $HeadURL $
# File: OperationTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/14 14:30:20
########################################################################

""" :mod: OperationTests
    ====================

    .. module: OperationTests
    :synopsis: Operation test cases
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Operation test cases
"""

__RCSID__ = "$Id $"

# #
# @file OperationTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/14 14:30:34
# @brief Definition of OperationTests class.

# # imports
import unittest
# # from DIRAC
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.File import File
# # SUT
from DIRAC.RequestManagementSystem.Client.Operation import Operation

########################################################################
class OperationTests( unittest.TestCase ):
  """
  .. class:: OperationTests

  """

  def setUp( self ):
    """ test set up """
    self.fromDict = { "Type" : "replicateAndRegister",
                      "TargetSE" : "CERN-USER,PIC-USER",
                      "SourceSE" : "" }
    self.subFile = File( { "LFN" : "/lhcb/user/c/cibak/testFile",
                           "Checksum" : "1234567",
                           "ChecksumType" : "ADLER32",
                           "Size" : 1024,
                           "Status" : "Waiting" } )
    self.operation = None

  def tearDown( self ):
    """ test case tear down """
    del self.fromDict
    del self.subFile

  def test01ctor( self ):
    """ test constructors and (de)serialisation """
    # # empty ctor
    self.assertEqual( isinstance( Operation(), Operation ), True, "empty ctor failed" )

    # # using fromDict
    operation = Operation( self.fromDict )
    self.assertEqual( isinstance( operation, Operation ), True, "fromDict ctor failed" )
    for key, value in self.fromDict.items():

      self.assertEqual( getattr( operation, key ), value, "wrong attr value %s (%s) %s" % ( key,
                                                                                            getattr( operation, key ),
                                                                                            value ) )

    # # same with file
    operation = Operation( self.fromDict )
    operation.addFile( self.subFile )

    for key, value in self.fromDict.items():
      self.assertEqual( getattr( operation, key ), value, "wrong attr value %s (%s) %s" % ( key,
                                                                                            getattr( operation, key ),
                                                                                            value ) )

    toJSON = operation.toJSON()
    self.assertEqual( toJSON["OK"], True, "JSON serialization failed" )

  def test02props( self ):
    """ test properties """

    # # valid values
    operation = Operation()
    operation.OperationID = 1
    self.assertEqual( operation.OperationID, 1, "wrong OperationID" )
    operation.OperationID = "1"
    self.assertEqual( operation.OperationID, 1, "wrong OperationID" )

    operation.Arguments = "foobar"
    self.assertEqual( operation.Arguments, "foobar", "wrong Arguments" )

    operation.SourceSE = "CERN-RAW"
    self.assertEqual( operation.SourceSE, "CERN-RAW", "wrong SourceSE" )

    operation.TargetSE = "CERN-RAW"
    self.assertEqual( operation.TargetSE, "CERN-RAW", "wrong TargetSE" )

    operation.Catalog = ""
    self.assertEqual( operation.Catalog, "", "wrong Catalog" )

    operation.Catalog = "BookkeepingDB"
    self.assertEqual( operation.Catalog, "BookkeepingDB", "wrong Catalog" )

    operation.Error = "error"
    self.assertEqual( operation.Error, "error", "wrong Error" )

    # # wrong props
    try:
      operation.RequestID = "foo"
    except Exception, error:
      self.assertEqual( type( error ), AttributeError, "wrong exc raised" )
      self.assertEqual( str( error ), "can't set attribute", "wrong exc reason" )

    try:
      operation.OperationID = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError, "wrong exc raised" )

    # # timestamps
    try:
      operation.SubmitTime = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError, "wrong exp raised" )
      self.assertEqual( str( error ), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'", "wrong exc reason" )

    try:
      operation.LastUpdate = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError, "wrong exc raised" )
      self.assertEqual( str( error ), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'", "wrong exc reason" )

    # # Status
    operation = Operation()
    try:
      operation.Status = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError, "wrong exc raised" )
      self.assertEqual( str( error ), "unknown Status 'foo'", "wrong exc reason" )
    operation.addFile( File( { "Status" : "Waiting", "LFN": "/a" } ) )
    oldStatus = operation.Status

    # # won't modify - there are Waiting files
    operation.Status = "Done"
    self.assertEqual( operation.Status, oldStatus, "waiting file but status == Done" )

    # # won't modify - there are Scheduled files
    for subFile in operation:
      subFile.Status = "Scheduled"
    operation.Status = "Done"
    self.assertEqual( operation.Status, "Scheduled", "scheduled files but operation status %s" % operation.Status )

    # # will modify - all files are Done now
    for subFile in operation:
      subFile.Status = "Done"
    operation.Status = "Done"
    self.assertEqual( operation.Status, "Done", "all files done but operation status %s" % operation.Status )

    operation = Operation()
    operation.addFile( File( { "Status" : "Done", "LFN": "/b" } ) )
    self.assertEqual( operation.Status, "Done", "all files done but operation status %s" % operation.Status )

    operation.addFile ( File( { "Status" : "Waiting", "LFN": "/c" } ) )
    self.assertEqual( operation.Status, "Queued", "all files waiting but operation status %s" % operation.Status )


  def test03sql( self ):
    """ sql insert or update """
    operation = Operation()
    operation.Type = "ReplicateAndRegister"

    request = Request()
    request.RequestName = "testRequest"
    request.RequestID = 1

    # # no parent request set
    try:
      operation.toSQL()
    except Exception, error:
      self.assertEqual( isinstance( error, AttributeError ), True, "wrong exc raised" )
      self.assertEqual( str( error ), "RequestID not set", "wrong exc reason" )

    # # parent set, no OperationID, INSERT
    request.addOperation( operation )
    toSQL = operation.toSQL()
    self.assertEqual( toSQL["OK"], True, "toSQL error" )
    self.assertEqual( toSQL["Value"].startswith( "INSERT" ), True, "OperationID not set, but SQL start with UPDATE" )

    op2 = Operation()
    op2.Type = "RemoveReplica"

    request.insertBefore( op2, operation )

    # # OperationID set = UPDATE
    operation.OperationID = 1
    toSQL = operation.toSQL()
    self.assertEqual( toSQL["OK"], True, "toSQL error" )
    self.assertEqual( toSQL["Value"].startswith( "UPDATE" ), True, "OperationID set, but SQL starts with INSERT" )

  def test04StateMachine( self ):
    """ state machine """
    op = Operation()
    self.assertEqual( op.Status, "Queued", "1. wrong status" )

    op.addFile( File( {"Status": "Waiting"} ) )
    self.assertEqual( op.Status, "Queued", "2. wrong status" )

    op.addFile( File( {"Status": "Scheduled" } ) )
    self.assertEqual( op.Status, "Queued", "3. wrong status" )

    op.addFile( File( {"Status": "Done" } ) )
    self.assertEqual( op.Status, "Queued", "4. wrong status" )

    op.addFile( File( { "Status": "Failed" } ) )
    self.assertEqual( op.Status, "Failed", "5. wrong status" )

    op[3].Status = "Scheduled"
    self.assertEqual( op.Status, "Queued", "6. wrong status" )

    op[0].Status = "Scheduled"
    self.assertEqual( op.Status, "Scheduled", "7. wrong status" )

    op[0].Status = "Waiting"
    self.assertEqual( op.Status, "Queued", "8. wrong status" )

    for f in op:
      f.Status = "Done"
    self.assertEqual( op.Status, "Done", "9. wrong status " )



# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  operationTests = testLoader.loadTestsFromTestCase( OperationTests )
  suite = unittest.TestSuite( [ operationTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

