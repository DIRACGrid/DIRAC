########################################################################
# $HeadURL $
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
__RCSID__ = "$Id: $"
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
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite


########################################################################
class FTSGraphTests( unittest.TestCase ):
  """
  .. class:: FTSGraphTests

  """

  def setUp( self ):
    """ test set up """
    self.ftsSites = [ FTSSite( { "FTSServer": "https://fts22-t0-export.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer",
                                "Name": "CERN.ch",
                                "FTSSiteID": 1 } ),
                      FTSSite( { "FTSServer": "https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer",
                                "Name": "PIC.es",
                                "FTSSiteID": 2 } ),
                      FTSSite( { "FTSServer": "https://lcgfts.gridpp.rl.ac.uk:8443/glite-data-transfer-fts/services/FileTransfer",
                                "Name": "RAL.uk",
                                "FTSSiteID": 3 } ),
                      FTSSite( { "FTSServer": "https://fts.grid.sara.nl:8443/glite-data-transfer-fts/services/FileTransfer",
                                "Name": "SARA.nl",
                                "FTSSiteID": 4 } ),
                      FTSSite( { "FTSServer": "https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer",
                                "Name": "CNAF.it",
                                "FTSSiteID": 5 } ),
                      FTSSite( { "FTSServer": "https://fts.grid.sara.nl:8443/glite-data-transfer-fts/services/FileTransfer",
                                "Name": "NIKHEF.nl",
                                "FTSSiteID": 6 } ),
                      FTSSite( { "FTSServer": "https://fts-fzk.gridka.de:8443/glite-data-transfer-fts/services/FileTransfer",
                                "Name": "GRIDKA.de",
                                "FTSSiteID": 7 } ),
                      FTSSite( { "FTSServer": "https://cclcgftsprod.in2p3.fr:8443/glite-data-transfer-fts/services/FileTransfer",
                                "Name": "IN2P3.fr",
                                "FTSSiteID": 8 } ) ]
    self.ftsHistoryViews = [ FTSHistoryView( { "TargetSE": "RAL-USER",
                                               "SourceSE": "CERN-USER",
                                               "FTSJobs": 10,
                                               "FTSServer": "https://fts22-t0-export.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer",
                                               "Status": "Active",
                                               "Files" : 1000,
                                               "Size": 10000000 } ) ]

  def tearDown( self ):
    """ test case tear down """
    del self.ftsSites
    del self.ftsHistoryViews

  def test( self ):
    """ test case """
    graph = FTSGraph( "ftsGraph", self.ftsSites, self.ftsHistoryViews )

    self.assertEqual( type( graph ), FTSGraph, "c'tor failed" )

    for node in graph.nodes():
      print node

    for route in graph.edges():
      print route

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
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSGraphTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
