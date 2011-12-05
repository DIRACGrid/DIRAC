########################################################################
# $HeadURL $
# File: TransferAgentTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/11/28 10:10:13
########################################################################

""" :mod: TransferAgentTests 
    =======================
 
    .. module: TransferAgentTests
    :synopsis: unitest for TransferAgent and TransferTask
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for TransferAgent and TransferTask
"""

__RCSID__ = "$Id $"

##
# @file TransferAgentTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/11/28 10:10:26
# @brief Definition of TransferAgentTests class.

## imports 
import unittest
from mock import *

from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.DataManagementSystem.Agent.TransferAgent import TransferAgent
from DIRAC.DataManagementSystem.Agent.TransferTask import TransferTask
  
AGENT_NAME = "DataManagement/TransferAgent"

class TransferTaskTests( unittest.TestCase ):
  """ test case for TransferTask 

  """
  def setUp( self ):
    pass


class TransferAgentTests( unittest.TestCase ):
  """ test case for TransferAgent

  """

  def setUp( self ):
    pass

  def test__01_ctor( self ):
    agent = None
    try:
      agent = TransferAgent( AGENT_NAME )
    except:
      pass
    self.assertEqual( isinstance( agent, TransferAgent ), True )

    
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suiteTA = testLoader.loadTestsFromTestCase( TransferAgentTests )     
  suiteTT = testLoader.loadTestsFromTestCase( TransferTaskTests )
  suite = unittest.TestSuite( [ suiteTA ] )
  unittest.TextTestRunner(verbosity=3).run(suite)
