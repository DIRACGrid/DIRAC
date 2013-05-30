########################################################################
# $HeadURL $
# File: RequestManagerHandlerTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/19 10:18:23
########################################################################

""" :mod: RequestManagerHandlerTests
    =======================

    .. module: RequestManagerHandlerTests
    :synopsis: unittest for RequestManagerHandler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for RequestManagerHandler
"""

__RCSID__ = "$Id $"

# #
# @file RequestManagerHandlerTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/19 10:18:34
# @brief Definition of RequestManagerHandlerTests class.

# # imports
import unittest


# # from DIRAC
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient

########################################################################
class RequestManagerHandlerTests( unittest.TestCase ):
  """
  .. class:: RequestManagerHandlerTests

  """

  def setUp( self ):
    """ test setup

    :param self: self reference
    """
    self.request = Request()
    self.request.RequestName = "RequestManagerHandlerTests"
    self.request.OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba"
    self.request.OwnerGroup = "dirac_user"
    self.operation = Operation()
    self.operation.Type = "ReplicateAndRegister"
    self.operation.TargetSE = "CERN-USER"
    self.file = File()
    self.file.LFN = "/lhcb/user/c/cibak/testFile"
    self.file.Checksum = "123456"
    self.file.ChecksumType = "ADLER32"
    self.request.addOperation( self.operation )
    self.operation.addFile( self.file )
    # # xml representation of a whole request
    self.xmlStr = self.request.toXML( True )["Value"]
    # # request client
    self.requestClient = RequestClient()


  def tearDown( self ):
    """ test case tear down """
    del self.request
    del self.operation
    del self.file
    del self.xmlStr

  def test01PutRequest( self ):
    """ test set request """
    put = self.requestClient.putRequest( self.request )
    self.assertEqual( put["OK"], True, "put failed" )

  def test02GetRequest( self ):
    """ test get request """
    get = self.requestClient.getRequest( self.request.RequestName )
    self.assertEqual( get["OK"], True, "get failed" )

  def test03DeleteRequest( self ):
    """ test delete request """
    delete = self.requestClient.deleteRequest( "test" )
    self.assertEqual( delete["OK"], True, "delete failed" )

# # test execution
if __name__ == "__main__":
  gLoader = unittest.TestLoader()
  gSuite = gLoader.loadTestsFromTestCase( RequestManagerHandlerTests )
  unittest.TextTestRunner( verbosity = 3 ).run( gSuite )

