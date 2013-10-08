########################################################################
# File: FTSGraphTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/08 09:05:23
########################################################################
""" :mod: FTSGraphTests
    =======================

    .. module: FTSGraphTests
    :synopsis: test cases for FTSGraph
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for FTSGraph
"""
# #
# @file FTSGraphTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/08 09:05:33
# @brief Definition of FTSGraphTests class.

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

# # imports
import unittest
# # SUT
from DIRAC.DataManagementSystem.private.FTSGraph import FTSGraph
# # from DIRAC
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView


########################################################################
class FTSGraphTests( unittest.TestCase ):
  """
  .. class:: FTSGraphTests

  """

  def setUp( self ):
    """ test set up """
    self.ftsHistoryViews = [
      FTSHistoryView( { "TargetSE": "RAL-USER",
                        "SourceSE": "CERN-USER",
                        "FTSJobs": 10,
                        "FTSServer":
                        "https://fts22-t0-export.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer",
                        "Status": "Active",
                        "Files" : 1000,
                        "Size": 10000000 } ) ]

  def tearDown( self ):
    """ test case tear down """
    del self.ftsHistoryViews

  def test( self ):
    """ test case """
    graph = FTSGraph( "ftsGraph", self.ftsHistoryViews )

    self.assertEqual( type( graph ), FTSGraph, "c'tor failed" )

    site = graph.findSiteForSE( "CERN-FOO" )
    self.assertEqual( site["OK"], False, "findSiteForSE call failed for unknown SE" )

    sourceSite = graph.findSiteForSE( "CERN-USER" )
    self.assertEqual( sourceSite["OK"], True, "findSiteForSE call failed for target SE" )

    targetSite = graph.findSiteForSE( "RAL-USER" )
    self.assertEqual( targetSite["OK"], True, "findSiteForSE call failed for source SE" )

    route = graph.findRoute( "RAL-USER", "CERN-USER" )
    self.assertEqual( route["OK"], True, "findRoute failed for known source and target SEs" )

    route = graph.findRoute( "RAL-FOO", "CERN-BAR" )
    self.assertEqual( route["OK"], False, "findRoute failed for unknown source and target SEs" )


# # test execution
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( FTSGraphTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( gSuite )
