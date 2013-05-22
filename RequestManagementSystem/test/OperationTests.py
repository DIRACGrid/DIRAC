########################################################################
# $HeadURL $
# File: OperationTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/14 14:30:20
########################################################################

""" :mod: OperationTests
    =======================

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

  def test_ctor( self ):
    """ test constructors and (de)serialisation """
    # # empty ctor
    self.assertEqual( isinstance( Operation(), Operation ), True )

    # # using fromDict
    operation = Operation( self.fromDict )
    self.assertEqual( isinstance( operation, Operation ), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( operation, key ), value )
    # # from and to  XML
    opXML = operation.toXML()
    self.assertEqual( opXML["OK"], True )
    opXML = opXML["Value"]
    operation = Operation.fromXML( opXML )
    self.assertEqual( operation["OK"], True )
    operation = operation["Value"]
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( operation, key ), value )

    # # same with file
    operation = Operation( self.fromDict )
    operation.addFile( self.subFile )

    opXML = operation.toXML()

    self.assertEqual( opXML["OK"], True )

    opXML = opXML["Value"]

    operation = Operation.fromXML( opXML )
    self.assertEqual( operation["OK"], True )

    operation = operation["Value"]
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( operation, key ), value )

  def test_props( self ):
    """ test properties """
    # # valid values
    operation = Operation()
    operation.OperationID = 1
    self.assertEqual( operation.OperationID, 1 )
    operation.OperationID = "1"
    self.assertEqual( operation.OperationID, 1 )

    operation.Arguments = "foobar"
    self.assertEqual( operation.Arguments, "foobar" )

    operation.SourceSE = "CERN-RAW"
    self.assertEqual( operation.SourceSE, "CERN-RAW" )

    operation.TargetSE = "CERN-RAW"
    self.assertEqual( operation.TargetSE, "CERN-RAW" )

    operation.Catalog = ""
    self.assertEqual( operation.Catalog, "" )

    operation.Catalog = "BookkeepingDB"
    self.assertEqual( operation.Catalog, "BookkeepingDB" )

    operation.Error = "error"
    self.assertEqual( operation.Error, "error" )

    # # wrong props
    try:
      operation.RequestID = "foo"
    except Exception, error:
      self.assertEqual( type( error ), AttributeError )
      self.assertEqual( str( error ), "can't set attribute" )

    try:
      operation.OperationID = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError )

    operation = Operation()
    try:
      operation.Type = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError )
      self.assertEqual( str( error ), "'foo' in not valid Operation!" )


    # # timestamps
    try:
      operation.SubmitTime = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError )
      self.assertEqual( str( error ), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'" )

    try:
      operation.LastUpdate = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError )
      self.assertEqual( str( error ), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'" )

    # # Status
    operation = Operation()
    try:
      operation.Status = "foo"
    except Exception, error:
      self.assertEqual( type( error ), ValueError )
      self.assertEqual( str( error ), "unknown Status 'foo'" )

    operation.addFile( File( { "Status" : "Waiting", "LFN": "/a" } ) )
    oldStatus = operation.Status

    # # won't modify - there are Waiting files
    operation.Status = "Done"
    self.assertEqual( operation.Status, oldStatus )

    # # won't modify - there are Scheduled files
    for subFile in operation:
      subFile.Status = "Scheduled"
    operation.Status = "Done"
    self.assertEqual( operation.Status, oldStatus )

    # # will modify - all files are Done now
    for subFile in operation:
      subFile.Status = "Done"
    operation.Status = "Done"
    self.assertEqual( operation.Status, "Done" )

    operation = Operation()
    operation.addFile( File( { "Status" : "Done", "LFN": "/b" } ) )
    self.assertEqual( operation.Status, "Done" )

    operation.addFile ( File( { "Status" : "Waiting", "LFN": "/c" } ) )
    self.assertEqual( operation.Status, oldStatus )


  def test_sql( self ):
    """ insert or update """
    operation = Operation()
    operation.Type = "ReplicateAndRegister"

    request = Request()
    request.RequestName = "testRequest"
    request.RequestID = 1

    # # no parent request set
    try:
      operation.toSQL()
    except Exception, error:
      self.assertEqual( isinstance( error, AttributeError ), True )
      self.assertEqual( str( error ), "RequestID not set" )

    # # parent set, no OperationID, INSERT
    request.addOperation( operation )
    toSQL = operation.toSQL()
    self.assertEqual( toSQL["OK"], True, "toSQL error" )
    self.assertEqual( toSQL["Value"].startswith( "INSERT" ), True )

    op2 = Operation()
    op2.Type = "RemoveReplica"

    request.insertBefore( op2, operation )

    # # OperationID set = UPDATE
    operation.OperationID = 1
    toSQL = operation.toSQL()
    self.assertEqual( toSQL["OK"], True, "toSQL error" )
    self.assertEqual( toSQL["Value"].startswith( "UPDATE" ), True )


# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  operationTests = testLoader.loadTestsFromTestCase( OperationTests )
  suite = unittest.TestSuite( [ operationTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

