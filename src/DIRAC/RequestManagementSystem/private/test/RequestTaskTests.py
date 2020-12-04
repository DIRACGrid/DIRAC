
""" :mod: RequestTaskTests
    =======================

    .. module: RequestTaskTests
    :synopsis: test cases for RequestTask class

    test cases for RequestTask class
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id $"
# #
# @file RequestTaskTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/27 15:59:40
# @brief Definition of RequestTaskTests class.
# # imports
import unittest
import importlib
from mock import Mock, MagicMock
# # SUT
from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask

# # request client
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
ReqClient = Mock( spec = ReqClient )
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
    self.req.OwnerGroup = "lhcb_user"
    self.req.OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=chaen/CN=705305/CN=Christophe Haen"
    self.op = Operation( { "Type": "ForwardDISET", "Arguments" : "tts10:helloWorldee" } )
    self.req.addOperation( self.op )
    self.task = None
    self.mockRC = MagicMock()

    self.mockObjectOps = MagicMock()
    self.mockObjectOps.getSections.return_value = {'OK': True,
                                                   'Value': ['DataProcessing',
                                                             'DataManager']}
    self.mockObjectOps.getOptionsDict.return_value = {'OK': True,
                                                      'Value': {'Group': 'lhcb_user', 'User': 'fstagni'}}
    self.mockOps = MagicMock()
    self.mockOps.return_value = self.mockObjectOps


  def tearDown( self ):
    """ test case tear down """
    del self.req
    del self.op
    del self.task

  def testAPI( self ):
    """ test API
    """
    rt = importlib.import_module( 'DIRAC.RequestManagementSystem.private.RequestTask' )
    rt.gMonitor = MagicMock()
    rt.Operations = self.mockOps
    rt.CS = MagicMock()

    self.task = RequestTask( self.req.toJSON()["Value"], self.handlersDict, 'csPath', 'RequestManagement/RequestExecutingAgent',
                             requestClient = self.mockRC )
    self.task.requestClient = Mock( return_value = Mock( spec = ReqClient ) )
    self.task.requestClient().updateRequest = Mock()
    self.task.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }
    ret = self.task()
    self.assertEqual( ret["OK"], True , "call failed" )

    ret = self.task.setupProxy()
    print(ret)


# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  requestTaskTests = testLoader.loadTestsFromTestCase( RequestTaskTests )
  suite = unittest.TestSuite( [ requestTaskTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
