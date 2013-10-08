# TODO: to be removed

########################################################################
# $HeadURL $
# File: StrategyHandlerTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/10/01 11:48:31
########################################################################

""" :mod: StrategyHandlerTests 
    =======================
 
    .. module: StrategyHandlerTests
    :synopsis: unittest for StrategyHandler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for StrategyHandler


    OBSOLETE
    K.C.
"""

__RCSID__ = "$Id $"

##
# @file StrategyHandlerTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/10/01 11:48:42
# @brief Definition of StrategyHandlerTests class.

## imports 
import unittest
## 
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
## from DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
## SUT
from DIRAC.DataManagementSystem.private.StrategyHandler import *

########################################################################
class StrategyHandlerTests(unittest.TestCase):
  """
  .. class:: StrategyHandlerTests
  
  """

  def setUp( self ):
    """ test setup """
    self.configPath = PathFinder.getAgentSection( "DataManagement/TransferAgent" )
    self.channels = { 1L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'CERN', 'ChannelName': 'CERN-CERN', 'Size': 0},
                      2L: {'Status': 'Active', 'Files': 0, 'Destination': 'CNAF', 'Source': 'CERN', 'ChannelName': 'CERN-CNAF', 'Size': 0},
                      3L: {'Status': 'Active', 'Files': 0, 'Destination': 'GRIDKA', 'Source': 'CERN', 'ChannelName': 'CERN-GRIDKA', 'Size': 0},
                      4L: {'Status': 'Active', 'Files': 0, 'Destination': 'IN2P3', 'Source': 'CERN', 'ChannelName': 'CERN-IN2P3', 'Size': 0},
                      5L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'CERN', 'ChannelName': 'CERN-NIKHEF', 'Size': 0},
                      6L: {'Status': 'Active', 'Files': 2, 'Destination': 'PIC', 'Source': 'CERN', 'ChannelName': 'CERN-PIC', 'Size': 0},
                      7L: {'Status': 'Active', 'Files': 0, 'Destination': 'RAL', 'Source': 'CERN', 'ChannelName': 'CERN-RAL', 'Size': 0},
                      8L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'CNAF', 'ChannelName': 'CNAF-CERN', 'Size': 0},
                      9L: {'Status': 'Active', 'Files': 0, 'Destination': 'CNAF', 'Source': 'CNAF', 'ChannelName': 'CNAF-CNAF', 'Size': 0},
                      10L: {'Status': 'Active', 'Files': 0, 'Destination': 'GRIDKA', 'Source': 'CNAF', 'ChannelName': 'CNAF-GRIDKA', 'Size': 0},
                      11L: {'Status': 'Active', 'Files': 0, 'Destination': 'IN2P3', 'Source': 'CNAF', 'ChannelName': 'CNAF-IN2P3', 'Size': 0},
                      12L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'CNAF', 'ChannelName': 'CNAF-NIKHEF', 'Size': 0},
                      13L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'CNAF', 'ChannelName': 'CNAF-PIC', 'Size': 0},
                      14L: {'Status': 'Active', 'Files': 0, 'Destination': 'RAL', 'Source': 'CNAF', 'ChannelName': 'CNAF-RAL', 'Size': 0},
                      15L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-CERN', 'Size': 0},
                      16L: {'Status': 'Active', 'Files': 0, 'Destination': 'CNAF', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-CNAF', 'Size': 0},
                      17L: {'Status': 'Active', 'Files': 0, 'Destination': 'GRIDKA', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-GRIDKA', 'Size': 0},
                      18L: {'Status': 'Active', 'Files': 0, 'Destination': 'IN2P3', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-IN2P3', 'Size': 0},
                      19L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-NIKHEF', 'Size': 0},
                      20L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-PIC', 'Size': 0},
                      21L: {'Status': 'Active', 'Files': 0, 'Destination': 'RAL', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-RAL', 'Size': 0},
                      22L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-CERN', 'Size': 0},
                      23L: {'Status': 'Active', 'Files': 0, 'Destination': 'CNAF', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-CNAF', 'Size': 0},
                      24L: {'Status': 'Active', 'Files': 0, 'Destination': 'GRIDKA', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-GRIDKA', 'Size': 0},
                      25L: {'Status': 'Active', 'Files': 0, 'Destination': 'IN2P3', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-IN2P3', 'Size': 0},
                      26L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-NIKHEF', 'Size': 0},
                      27L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-PIC', 'Size': 0},
                      28L: {'Status': 'Active', 'Files': 0, 'Destination': 'RAL', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-RAL', 'Size': 0},
                      29L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-CERN', 'Size': 0},
                      30L: {'Status': 'Active', 'Files': 0, 'Destination': 'CNAF', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-CNAF', 'Size': 0},
                      31L: {'Status': 'Active', 'Files': 0, 'Destination': 'GRIDKA', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-GRIDKA', 'Size': 0},
                      32L: {'Status': 'Active', 'Files': 0, 'Destination': 'IN2P3', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-IN2P3', 'Size': 0},
                      33L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-NIKHEF', 'Size': 0},
                      34L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-PIC', 'Size': 0},
                      35L: {'Status': 'Active', 'Files': 0, 'Destination': 'RAL', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-RAL', 'Size': 0},
                      36L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'PIC', 'ChannelName': 'PIC-CERN', 'Size': 0},
                      37L: {'Status': 'Active', 'Files': 0, 'Destination': 'CNAF', 'Source': 'PIC', 'ChannelName': 'PIC-CNAF', 'Size': 0},
                      38L: {'Status': 'Active', 'Files': 0, 'Destination': 'GRIDKA', 'Source': 'PIC', 'ChannelName': 'PIC-GRIDKA', 'Size': 0},
                      39L: {'Status': 'Active', 'Files': 0, 'Destination': 'IN2P3', 'Source': 'PIC', 'ChannelName': 'PIC-IN2P3', 'Size': 0},
                      40L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'PIC', 'ChannelName': 'PIC-NIKHEF', 'Size': 0},
                      41L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'PIC', 'ChannelName': 'PIC-PIC', 'Size': 0},
                      42L: {'Status': 'Active', 'Files': 0, 'Destination': 'RAL', 'Source': 'PIC', 'ChannelName': 'PIC-RAL', 'Size': 0},
                      43L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'RAL', 'ChannelName': 'RAL-CERN', 'Size': 0},
                      44L: {'Status': 'Active', 'Files': 0, 'Destination': 'CNAF', 'Source': 'RAL', 'ChannelName': 'RAL-CNAF', 'Size': 0},
                      45L: {'Status': 'Active', 'Files': 0, 'Destination': 'GRIDKA', 'Source': 'RAL', 'ChannelName': 'RAL-GRIDKA', 'Size': 0},
                      46L: {'Status': 'Active', 'Files': 0, 'Destination': 'IN2P3', 'Source': 'RAL', 'ChannelName': 'RAL-IN2P3', 'Size': 0},
                      47L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'RAL', 'ChannelName': 'RAL-NIKHEF', 'Size': 0},
                      48L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'RAL', 'ChannelName': 'RAL-PIC', 'Size': 0},
                      49L: {'Status': 'Active', 'Files': 0, 'Destination': 'RAL', 'Source': 'RAL', 'ChannelName': 'RAL-RAL', 'Size': 0},
                      50L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'CERN', 'ChannelName': 'CERN-SARA', 'Size': 0},
                      51L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'SARA', 'ChannelName': 'SARA-CERN', 'Size': 0},
                      52L: {'Status': 'Active', 'Files': 0, 'Destination': 'CNAF', 'Source': 'SARA', 'ChannelName': 'SARA-CNAF', 'Size': 0},
                      53L: {'Status': 'Active', 'Files': 0, 'Destination': 'GRIDKA', 'Source': 'SARA', 'ChannelName': 'SARA-GRIDKA', 'Size': 0},
                      54L: {'Status': 'Active', 'Files': 0, 'Destination': 'IN2P3', 'Source': 'SARA', 'ChannelName': 'SARA-IN2P3', 'Size': 0},
                      55L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'SARA', 'ChannelName': 'SARA-NIKHEF', 'Size': 0},
                      56L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'SARA', 'ChannelName': 'SARA-PIC', 'Size': 0},
                      57L: {'Status': 'Active', 'Files': 0, 'Destination': 'RAL', 'Source': 'SARA', 'ChannelName': 'SARA-RAL', 'Size': 0},
                      58L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'SARA', 'ChannelName': 'SARA-SARA', 'Size': 0},
                      59L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'CNAF', 'ChannelName': 'CNAF-SARA', 'Size': 0},
                      60L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-SARA', 'Size': 0},
                      61L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-SARA', 'Size': 0},
                      62L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-SARA', 'Size': 0},
                      63L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'PIC', 'ChannelName': 'PIC-SARA', 'Size': 0},
                      64L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'RAL', 'ChannelName': 'RAL-SARA', 'Size': 0},
                      65L: {'Status': 'Active', 'Files': 0, 'Destination': 'CERN', 'Source': 'RAL-HEP', 'ChannelName': 'CERN-RAL-HEP', 'Size':0 } }

    self.bands = dict.fromkeys( self.channels.keys() )
    for k in self.bands:
      self.bands[k] = { 'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0 }
    #self.bands[1]["Fileput"] = 10.0
    self.bands[14]["Fileput"] = 10.0
    self.failedFiles = dict.fromkeys( self.channels.keys(), 0 )

  def tearDown( self ):
    """ claen up """
    del self.channels
    del self.bands
    del self.failedFiles
    
  def test_01_ctor( self ):
    """ constructor + update test """
    sHandler = StrategyHandler( self.configPath, self.bands, self.channels, self.failedFiles )
    self.assertEqual( isinstance( sHandler, StrategyHandler), True )
    
    gr = sHandler.setup( self.channels, self.bands, self.failedFiles )
    self.assertEqual( gr["OK"], True )
    self.assertEqual( isinstance( sHandler.ftsGraph, Graph ), True  )

    ## change one channel
    self.channels[1]["Size"] = 100000000L
    gr = sHandler.setup( self.channels, self.bands, self.failedFiles )
    self.assertEqual( gr["OK"], True )
    self.assertEqual( isinstance( sHandler.ftsGraph, Graph ), True  )

    ## get channel
    channel = sHandler.ftsGraph.findChannel( "CERN-USER", "CERN-USER" )
    self.assertEqual( channel["OK"], True )
    self.assertEqual( channel["Value"].channelName, "CERN-CERN" )
    self.assertEqual( channel["Value"].size,  self.channels[1]["Size"] )
  
  def test_02_Strategies( self ):
    """ test strategies """
    sHandler = StrategyHandler( self.configPath, self.bands, self.channels, self.failedFiles ) 

    tree = sHandler.minimiseTotalWait( [ 'CERN-DST' ], ['RAL-HEP-DST'] )
    print tree

    ## simple - wrong args
    tree = sHandler.simple( ["CERN-USER", "PIC-USER"], ["CNAF-USER"] )
    self.assertEqual( tree["OK"], False )
    self.assertEqual( tree["Message"], "simple: wrong argument supplied for sourceSEs, only one sourceSE allowed" )

    ## simple - no channel defined 
    tree = sHandler.simple( [ "CERN-USER" ], ["FOO-USER"] )
    self.assertEqual( tree["OK"], False )
    self.assertEqual( tree["Message"], "FTSGraph: unable to find FTS channel between 'CERN-USER' and 'FOO-USER'" ) 

    ## simple -  channel used twice
    tree = sHandler.simple( ["CERN-USER"], [ "CNAF-DST", "CNAF-USER" ] )
    self.assertEqual( tree["OK"], False )
    self.assertEqual( tree["Message"], "simple: unable to create replication tree, channel 'CERN-CNAF' can't be used twice")

    ## simple - OK
    tree = sHandler.simple( ["CERN-USER"], ["PIC-USER" ] )
    self.assertEqual( tree["OK"], True )
    self.assertEqual( tree["Value"], {6L: {'Ancestor': False, 'Strategy': 'Simple', 'DestSE': 'PIC-USER', 'SourceSE': 'CERN-USER'}} )

    ## swarm - wrong args
    tree = sHandler.swarm( [ "CERN-USER" ], [ "CNAF-USER", "PIC-USER" ] )
    self.assertEqual( tree["OK"], False )
    self.assertEqual( tree["Message"], "swarm: wrong argument supplied for targetSEs, only one targetSE allowed" )

    ## swarm - channel not defined
    tree = sHandler.swarm( ["CERN-USER"], ["FOO-USER"] )
    self.assertEqual( tree["OK"], False )
    self.assertEqual( tree["Message"], "swarm: unable to find FTS channels between 'CERN-USER' and 'FOO-USER'" )

    ## swarm OK
    tree = sHandler.swarm( [ "CERN-USER" ], [ "CNAF-USER"] )
    self.assertEqual( tree["OK"], True )
    self.assertEqual( tree["Value"], {2L: {'Ancestor': False, 'Strategy': 'Swarm', 'DestSE': 'CNAF-USER', 'SourceSE': 'CERN-USER'}} )

    ## minimiseTotalWait - no channel defined
    tree = sHandler.minimiseTotalWait( ["CERN-USER"], ["FOO-USER"] )
    self.assertEqual( tree["OK"], False )
    self.assertEqual( tree["Message"], "minimiseTotalWait: FTS channels between ['CERN-USER'] and ['FOO-USER'] not defined or already used")

    ## minimiseTotalWait - OK
    tree = sHandler.minimiseTotalWait( ["CERN-USER"], ["CNAF-USER"] )
    self.assertEqual( tree["OK"], True )
    self.assertEqual( tree["Value"], {2L: {'Ancestor': False, 'Strategy': 'MinimiseTotalWait', 'DestSE': 'CNAF-USER', 'SourceSE': 'CERN-USER'}})

    ## dynamic throughput - OK
    tree = sHandler.dynamicThroughput( ["CERN-USER"], ["CNAF-USER"] )
    self.assertEqual( tree["OK"], True )
    self.assertEqual( tree["Value"], {2L: {'Ancestor': False, 'Strategy': 'DynamicThroughput', 'DestSE': 'CNAF-USER', 'SourceSE': 'CERN-USER'}})

  def test_03_replicationTree( self ):
    """ test replication tree """
    sHandler = StrategyHandler( self.configPath, self.bands, self.channels, self.failedFiles )

    tree = sHandler.replicationTree( ["CERN-USER", "CNAF-USER"], ["PIC-USER", "RAL-USER"], 1000L, "MinimiseTotalWait" )
    self.assertEqual( tree["OK"], True )
    tree = tree["Value"]
    self.assertEqual( tree, { 48L: {'Ancestor': 14L, 'Strategy': 'MinimiseTotalWait', 'DestSE': 'PIC-USER', 'SourceSE': 'RAL-USER'},
                              14L: {'Ancestor': False, 'Strategy': 'MinimiseTotalWait', 'DestSE': 'RAL-USER', 'SourceSE': 'CNAF-USER'}})
    for channelID, repDict in tree.items():
      sourceSE = repDict["SourceSE"]
      targetSE = repDict["DestSE"]
      ftsChannel = sHandler.ftsGraph.findChannel( sourceSE, targetSE )
      self.assertEqual( ftsChannel["OK"], True  )
      ftsChannel = ftsChannel["Value"]
      self.assertEqual( ftsChannel.size, 1000L )
      self.assertEqual( ftsChannel.files, 1 )
      if ftsChannel.channelName == "CNAF-RAL":
        self.assertEqual( ftsChannel.timeToStart, 0.1 )
      if ftsChannel.channelName == "RAL-PIC":
        self.assertEqual( ftsChannel.timeToStart, 0.0 )
        
    ## the same, should use CERN-USER
    tree = sHandler.replicationTree( ["CERN-USER", "CNAF-USER"], ["PIC-USER", "RAL-USER"], 1000L, "MinimiseTotalWait" )
    self.assertEqual( tree["OK"], True )
    tree = tree["Value"]
    self.assertEqual( tree, { 48L: {'Ancestor': 7L, 'Strategy': 'MinimiseTotalWait', 'DestSE': 'PIC-USER', 'SourceSE': 'RAL-USER'},
                              7L:  {'Ancestor': False, 'Strategy': 'MinimiseTotalWait', 'DestSE': 'RAL-USER', 'SourceSE': 'CERN-USER'}} )

    for channelID, repDict in tree.items():
      sourceSE = repDict["SourceSE"]
      targetSE = repDict["DestSE"]
      ftsChannel = sHandler.ftsGraph.findChannel( sourceSE, targetSE )
      self.assertEqual( ftsChannel["OK"], True  )
      ftsChannel = ftsChannel["Value"]
      if ftsChannel.channelName == "CERN-RAL":
        self.assertEqual( ftsChannel.size, 1000L )
        self.assertEqual( ftsChannel.files, 1 )        
        self.assertEqual( ftsChannel.timeToStart, 0.0 )
      if ftsChannel.channelName == "RAL-PIC":
        self.assertEqual( ftsChannel.size, 2000L )
        self.assertEqual( ftsChannel.files, 2 ) 
        self.assertEqual( ftsChannel.timeToStart, 0.0 )

## test execution
if __name__ == "__main__":

  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( StrategyHandlerTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

