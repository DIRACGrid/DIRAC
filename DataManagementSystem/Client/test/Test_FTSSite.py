########################################################################
# File: FTSSiteTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/16 08:52:36
########################################################################

""" :mod: FTSSiteTests
    ==================

    .. module: FTSSiteTests
    :synopsis: unittest for FTSSite class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for FTSSite class
"""

# #
# @file FTSSiteTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/16 08:52:44
# @brief Definition of FTSSiteTests class.

# # imports
import unittest
# # SUT
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite

########################################################################
class FTSSiteTests( unittest.TestCase ):
  """
  .. class:: FTSSiteTests

  """

  def setUp( self ):
    """ test set up """
    self.fromDict = { "FTSServer": "https://something.somewhere.org/FTSService",
                      "Name": "something.somewhere.org",
                      "MaxActiveJobs" : 100 }

  def tearDown( self ):
    """ test tear down """
    del self.fromDict

  def test( self ):
    """ test case """
    ftsSite = FTSSite( name = "something.somewhere.org", ftsServer = "https://something.somewhere.org/FTSService",
                       maxActiveJobs = 100 )

    self.assertEqual( type( ftsSite ), FTSSite, "wrong type" )
    for k, v in self.fromDict.items():
      self.assertEqual( hasattr( ftsSite, k ), True, "%s attr is missing" % k )
      self.assertEqual( getattr( ftsSite, k ), v, "wrong value for attr %s" % k )

    # # serialization
    # FS: actually these methods aren't present
#
#    # # to JSON
#    toJSON = ftsSite.toJSON()
#    self.assertEqual( toJSON["OK"], True, "toJSON failed" )
#    toJSON = toJSON["Value"]
#
#    # # to SQL
#    toSQL = ftsSite.toSQL()
#    self.assertEqual( toSQL["OK"], True, "toSQL failed" )
#    self.assertEqual( toSQL["Value"].startswith( "INSERT" ), True, "toSQL should start with INSERT" )
#
#    # # FTSSiteID set
#    ftsSite.FTSSiteID = 10
#    self.assertEqual( ftsSite.FTSSiteID, 10, "wrong value for FTSSite" )
#
#    # # to SQL again
#    toSQL = ftsSite.toSQL()
#    self.assertEqual( toSQL["OK"], True, "toSQL failed" )
#    self.assertEqual( toSQL["Value"].startswith( "UPDATE" ), True, "toSQL should start with UPDATE" )

# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSSiteTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
