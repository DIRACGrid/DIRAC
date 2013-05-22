########################################################################
# $HeadURL $
# File: RequestTaskTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/27 15:59:24
########################################################################
""" :mod: RequestTaskTests
    =======================

    .. module: RequestTaskTests
    :synopsis: test cases for RequestTask class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for RequestTask class
"""
__RCSID__ = "$Id $"
# #
# @file RequestTaskTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/27 15:59:40
# @brief Definition of RequestTaskTests class.
# # imports
import unittest
from mock import *
# # SUT
from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask

## requect client 
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
RequestClient = Mock(spec=RequestClient)
# # from DIRAC
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation

########################################################################
class RequestTaskTests( unittest.TestCase ):
  """
  .. class:: RequestTaskTests

  """

  def setUp( self ):
    """ test case set up """
    self.handlersDict = { "ForwardDISET" : "DIRAC/RequestManagementSystem/private/ForwardDISET" }
    self.req = Request()
    self.req.RequestName = "foobarbaz"
    self.req.OwnerGroup = "dirac_user"
    self.req.OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba"
    self.op = Operation( { "Type": "ForwardDISET", "Arguments" : "tts10:helloWorldee" } )
    self.req.addOperation( self.op )
    self.task = None  

  def tearDown( self ):
    """ test case tear down """
    del self.req
    del self.op
    del self.task

  def testAPI( self ):
    """ test API """
    self.task = RequestTask( self.req.toXML()["Value"], self.handlersDict )
    self.task.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    self.task.requestClient().updateRequest = Mock()
    self.task.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }    
    ret = self.task()
    self.assertEqual( ret["OK"], True , "call failed")


# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  requestTaskTests = testLoader.loadTestsFromTestCase( RequestTaskTests )
  suite = unittest.TestSuite( [ requestTaskTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
