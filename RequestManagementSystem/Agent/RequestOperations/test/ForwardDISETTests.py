########################################################################
# $HeadURL $
# File: ForwardDISETTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/18 09:23:05
########################################################################

""" :mod: ForwardDISETTests
    =======================

    .. module: ForwardDISETTests
    :synopsis: unittest for ForwardDISET handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for ForwardDISET handler
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id $"

# #
# @file ForwardDISETTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/18 09:23:18
# @brief Definition of ForwardDISETTests class.

# # imports
import unittest
# # from DIRAC
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
# # SUT
from DIRAC.RequestManagementSystem.Agent.RequestOperations.ForwardDISET import ForwardDISET

########################################################################
class ForwardDISETTests( unittest.TestCase ):
  """
  .. class:: ForwardDISETTests

  """
  def setUp( self ):
    """ test set up """

    self.hiArgs = ( ( "RequestManagement/RequestManager",
                      { "keepAliveLapse": 10, "timeout": 5 } ),
                      "foo",
                      ( 12345, { "Hi": "There!" } ) )
    self.req = Request( { "RequestName": "testRequest" } )
    self.op = Operation( { "Type": "ForwardDISET",
                           "Arguments": DEncode.encode( self.hiArgs ) } )
    self.req += self.op

  def tearDown( self ):
    """ tear down """
    del self.hiArgs
    del self.op
    del self.req

  def testCase( self ):
    """ ctor and functionality """
    forwardDISET = None
    try:
      forwardDISET = ForwardDISET()
    except:
      pass
    self.assertEqual( isinstance( forwardDISET, ForwardDISET ), True, "construction error" )

    forwardDISET.setOperation( self.op )
    self.assertEqual( isinstance( forwardDISET.operation, Operation ), True, "setOperation error" )

    call = forwardDISET()
    # # should be failing right now
    self.assertEqual( call["OK"], False, "call failed" )

if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  forwardDISETTests = testLoader.loadTestsFromTestCase( ForwardDISETTests )
  suite = unittest.TestSuite( [ forwardDISETTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )


