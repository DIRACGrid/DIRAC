########################################################################
# File: FTSFileTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/16 06:45:16
########################################################################
""" :mod: FTSFileTests
    ==================

    .. module: FTSFileTests
    :synopsis: unittests for FTSFile
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittests for FTSFile
"""
# #
# @file FTSFileTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/16 06:45:31
# @brief Definition of FTSFileTests class.

# # imports
import unittest
# # SUT
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile

########################################################################
class FTSFileTests( unittest.TestCase ):
  """
  .. class:: FTSFileTests

  """
  def setUp( self ):
    """ test set up   """
    self.fromDict = { "FileID": 123456,
                      "OperationID": 123,
                      "LFN": "/a/b/c",
                      "ChecksumType": "ADLER32",
                      "Checksum": "aaabbbccc",
                      "Size": 10,
                      "SourceSE": "CERN-DST",
                      "TargetSE": "RAL-DST" }

  def tearDown( self ):
    """ test tear down """
    del self.fromDict

  def test01Ctor( self ):
    """ test ctor and (de-)serialization """

    ftsFile = FTSFile( self.fromDict )
    self.assertEqual( isinstance( ftsFile, FTSFile ), True )
    for k, v in self.fromDict.items():
      self.assertEqual( getattr( ftsFile, k ), v )

    json = ftsFile.toJSON()
    ftsFileJSON = FTSFile( json["Value"] )
    self.assertEqual( isinstance( ftsFileJSON, FTSFile ), True )
    for k, v in self.fromDict.items():
      self.assertEqual( getattr( ftsFileJSON, k ), v )


# # test execution
if __name__ == "__main__":

  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSFileTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )





