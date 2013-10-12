########################################################################
# File: FTSStrategyTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/06 16:50:30
########################################################################
""" :mod: FTSStrategyTests
    ======================

    .. module: FTSStrategyTests
    :synopsis: unittests for FTSStrategy
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittests for FTSStrategy
"""
# #
# @file FTSStrategyTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/06 16:50:40
# @brief Definition of FTSStrategyTests class.

# # imports
import unittest
# # SUT
from DIRAC.DataManagementSystem.private.FTSStrategy import FTSStrategy
# # helper classes
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView


########################################################################
class FTSStrategyTests( unittest.TestCase ):
  """
  .. class:: FTSStrategyTests

  """

  def setUp( self ):
    """ test case setup """
    pass

  def tearDown( self ):
    """ test case tear down """
    pass


# # test execution
if __name__ == "__main__":
  pass
