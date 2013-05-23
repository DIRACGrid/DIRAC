########################################################################
# $HeadURL $
# File: BaseOperationTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 08:09:08
########################################################################

""" :mod: BaseOperationTests
    ========================

    .. module: BaseOperationTests
    :synopsis: unittests for BaseOperation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittests for BaseOperation
"""

__RCSID__ = "$Id $"

# #
# @file BaseOperationTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 08:09:21
# @brief Definition of BaseOperationTests class.

# # imports
import unittest
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

########################################################################
class BaseOperationTests( unittest.TestCase ):
  """
  .. class:: BaseOperationTests

  """

  def setUp( self ):
    """ test set up """
    self.req = Request()
    self.req.RequestName = "testRequest"
    self.op = Operation( {"Type" : "ForwardDISET", "Arguments" : "foobar" } )
    self.req.addOperation( self.op )
    self.baseOp = BaseOperation()

  def tearDown( self ):
    """ test tear down """
    del self.baseOp
    del self.op
    del self.req

  def testBaseOperation( self ):
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
    self.assertEqual( isinstance( self.baseOp.replicaManager(), ReplicaManager ), True, "ReplicaManger is missing" )
    # # DataLoggingClient
    self.assertEqual( isinstance( self.baseOp.dataLoggingClient(), DataLoggingClient ), True, "DataLoggingClient is missing" )

# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  baseOperationTests = testLoader.loadTestsFromTestCase( BaseOperationTests )
  suite = unittest.TestSuite( [ baseOperationTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

