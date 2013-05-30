########################################################################
# $HeadURL $
# File: FTSCleaningAgentTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/08/16 13:41:17
########################################################################

""" :mod: FTSCleaningAgentTests 
    =======================
 
    .. module: FTSCleaningAgentTests
    :synopsis: unit tests for FTSCleaningAgent
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unit tests for FTSCleaningAgent

    TODO: needs some smart content


    OBSOLETE
    K.C.
"""

__RCSID__ = "$Id $"

##
# @file FTSCleaningAgentTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/08/16 13:41:41
# @brief Definition of FTSCleaningAgentTests class.

## imports 
import mock
import unitest
## from DIRAC
from DIRAC.DataManagementSystem.Agent.FTSCleaningAgent import FTSCleaningAgent

########################################################################
class FTSCleaningAgentTests(unittest.TestCase):
  """
  .. class:: FTSCleaningAgentTests
  unit test for FTSCleaningAgent
  """

  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    pass


  def tearDown( self ):
    pass


if __name__ == "__main__":
  pass

