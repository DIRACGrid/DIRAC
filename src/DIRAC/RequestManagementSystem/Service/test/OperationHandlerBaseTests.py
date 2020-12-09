########################################################################
# $HeadURL $
# File: OperationHandlerBaseTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 08:09:08
########################################################################

""" :mod: OperationHandlerBaseTests
    ===============================

    .. module: OperationHandlerBaseTests
    :synopsis: unittests for OperationHandlerBase
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittests for OperationHandlerBase
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id $"

# #
# @file OperationHandlerBaseTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 08:09:21
# @brief Definition of OperationHandlerBaseTests class.

# # imports
import unittest
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

########################################################################
class OperationHandlerBaseTests( unittest.TestCase ):
  """
  .. class:: OperationHandlerBaseTests

  """

  def setUp( self ):
    """ test set up """
    self.req = Request()
    self.req.RequestName = "testRequest"
    self.op = Operation( {"Type" : "ForwardDISET", "Arguments" : "foobar" } )
    self.req.addOperation( self.op )
    self.baseOp = OperationHandlerBase()

  def tearDown( self ):
    """ test tear down """
    del self.baseOp
    del self.op
    del self.req

  def testOperationHandlerBase( self ):
    """ base op test """
    self.baseOp.setOperation( self.op )

    # # log is there
    self.assertEqual( "log" in dir( self.baseOp ), True, "log missing" )
    # # operation is there
    self.assertEqual( "operation" in dir( self.baseOp ), True, "operation is missing" )
    # # request is there
    self.assertEqual( "request" in dir( self.baseOp ), True, "request is missing" )
    # # __call__ not implemented
    self.assertRaises( NotImplementedError, self.baseOp )
    # # replica manager
    self.assertEqual( isinstance( self.baseOp.dm, DataManager ), True, "DataManager is missing" )

# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  OperationHandlerBaseTests = testLoader.loadTestsFromTestCase( OperationHandlerBaseTests )
  suite = unittest.TestSuite( [ OperationHandlerBaseTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

