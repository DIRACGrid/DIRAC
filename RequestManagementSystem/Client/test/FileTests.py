########################################################################
# $HeadURL$
# File: FileTest.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/06 13:48:54
########################################################################

""" :mod: FileTest
    =======================

    .. module: FileTest
    :synopsis: test cases for Files
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for Files
"""

__RCSID__ = "$Id$"

# #
# @file FileTest.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/06 13:49:05
# @brief Definition of FileTest class.

# # imports
import unittest
# # from DIRAC
from DIRAC.RequestManagementSystem.Client.Operation import Operation
# # SUT
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class FileTests( unittest.TestCase ):
  """
  .. class:: FileTest

  """

  def setUp( self ):
    """ test setup """
    self.fromDict = { "Size" : 1, "LFN" : "/test/lfn", "ChecksumType" : "ADLER32", "Checksum" : "123456", "Status" : "Waiting" }

  def tearDown( self ):
    """ test tear down """
    del self.fromDict

  def test01ctors( self ):
    """ File construction and (de)serialisation """
    # # empty default ctor
    theFile = File()
    self.assertEqual( isinstance( theFile, File ), True )

    # # fromDict
    try:
      theFile = File( self.fromDict )
    except AttributeError, error:
      print "AttributeError: %s" % str( error )

    self.assertEqual( isinstance( theFile, File ), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( theFile, key ), value )

    toJSON = theFile.toJSON()
    self.assertEqual( toJSON["OK"], True, "JSON serialization error" )


  def test02props( self ):
    """ test props and attributes  """
    theFile = File()

    # valid props
    theFile.FileID = 1
    self.assertEqual( theFile.FileID, 1 )
    theFile.Status = "Done"
    self.assertEqual( theFile.Status, "Done" )
    theFile.LFN = "/some/path/somewhere"
    self.assertEqual( theFile.LFN, "/some/path/somewhere" )
    theFile.PFN = "/some/path/somewhere"
    self.assertEqual( theFile.PFN, "/some/path/somewhere" )
    theFile.Attempt = 1
    self.assertEqual( theFile.Attempt, 1 )
    theFile.Size = 1
    self.assertEqual( theFile.Size, 1 )
    theFile.GUID = "2bbabe80-e2f1-11e1-9b23-0800200c9a66"
    self.assertEqual( theFile.GUID, "2bbabe80-e2f1-11e1-9b23-0800200c9a66" )
    theFile.ChecksumType = "adler32"
    self.assertEqual( theFile.ChecksumType, "ADLER32" )
    theFile.Checksum = "123456"
    self.assertEqual( theFile.Checksum, "123456" )

    # #
    theFile.Checksum = None
    theFile.ChecksumType = None
    self.assertEqual( theFile.Checksum, "" )
    self.assertEqual( theFile.ChecksumType, "" )

    # # invalid props

    # FileID
    try:
      theFile.FileID = "foo"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )

    # parent
    parent = Operation( { "OperationID" : 99999 } )
    parent += theFile

    theFile.FileID = 0

    self.assertEqual( parent.OperationID, theFile.OperationID )
    try:
      theFile.OperationID = 111111
    except Exception, error:
      self.assertEqual( isinstance( error, AttributeError ), True )
      self.assertEqual( str( error ), "can't set attribute" )

    # LFN
    try:
      theFile.LFN = 1
    except Exception, error:
      self.assertEqual( isinstance( error, TypeError ), True )
      self.assertEqual( str( error ), "LFN has to be a string!" )
    try:
      theFile.LFN = "../some/path"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str( error ), "LFN should be an absolute path!" )

    # PFN
    try:
      theFile.PFN = 1
    except Exception, error:
      self.assertEqual( isinstance( error, TypeError ), True )
      self.assertEqual( str( error ), "PFN has to be a string!" )
    try:
      theFile.PFN = "snafu"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str( error ), "Wrongly formatted PFN!" )

    # Size
    try:
      theFile.Size = "snafu"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
    try:
      theFile.Size = -1
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str( error ), "Size should be a positive integer!" )

    # GUID
    try:
      theFile.GUID = "snafuu-uuu-uuu-uuu-uuu-u"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str( error ), "'snafuu-uuu-uuu-uuu-uuu-u' is not a valid GUID!" )
    try:
      theFile.GUID = 2233345
    except Exception, error:
      self.assertEqual( isinstance( error, TypeError ), True )
      self.assertEqual( str( error ), "GUID should be a string!" )

    # Attempt
    try:
      theFile.Attempt = "snafu"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
    try:
      theFile.Attempt = -1
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str( error ), "Attempt should be a positive integer!" )

    # Status
    try:
      theFile.Status = None
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str( error ), "Unknown Status: None!" )

    # Error
    try:
      theFile.Error = Exception( "test" )
    except Exception, error:
      self.assertEqual( isinstance( error, TypeError ), True )
      self.assertEqual( str( error ), "Error has to be a string!" )

# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  fileTests = testLoader.loadTestsFromTestCase( FileTests )
  suite = unittest.TestSuite( [ fileTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

