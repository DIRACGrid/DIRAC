########################################################################
# $HeadURL $
# File: RequestAgentBaseTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/12/15 12:47:50
########################################################################

""" :mod: RequestAgentBaseTests 
    =======================
 
    .. module: RequestAgentBaseTests
    :synopsis: test suite for RequestAgentBase class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test suite for RequestAgentBase class
"""

__RCSID__ = "$Id $"

##
# @file RequestAgentBaseTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/12/15 12:48:03
# @brief Definition of RequestAgentBaseTests class.

## imports 
import unittest
from mock import *

## from DIRAC
from DIRAC,DataManagementSystem.private.RequestAgentBase import RequestAgentBase 

########################################################################
class RequestAgentBaseTests(unittest.TestCase):
  """
  .. class:: RequestAgentBaseTests
  
  """

  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    pass


  def test_01_ctor( self ):
    pass




if __name__ == "__main__":
  pass

