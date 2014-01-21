# TODO: to be removed

########################################################################
# $HeadURL $
# File: RegisterTaskTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/10/21 14:41:19
########################################################################

""" :mod: RegisterTaskTests 
    =======================
 
    .. module: RegisterTaskTests
    :synopsis: unit tests for RegisterTask
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unit tests for RegisterTask

  OBSOLETE
  K.C.
"""

__RCSID__ = "$Id $"

##
# @file RegisterTaskTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/10/21 14:41:33
# @brief Definition of RegisterTaskTests class.

## imports 
import unittest
from mock import *
global MySQL, DB, RequestDBMySQL
from DIRAC.Core.Utilities.MySQL import MySQL
MySQL = Mock(spec=MySQL)
from DIRAC.Core.Base.DB import DB
DB = Mock(spec=DB)
from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
RequestDBMySQL = Mock(spec=RequestDBMySQL)
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
RequestClient = Mock(spec=RequestClient)
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
DataLoggingClient = Mock(spec=DataLoggingClient)
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
DataManager = Mock( spec = DataManager )
from DIRAC.DataManagementSystem.private.RegistrationTask import RegistrationTask

from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager 
gProxyManager = Mock( spec=gProxyManager.__class__ )

def getRegisterRequest( ):
  """ helper fcn to build request
  """
  requestContainer = RequestContainer( init = False )
  requestContainer.setJobID( 11889410 )
  #requestContainer.setOwnerDN( "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba" )
  requestContainer.setOwnerGroup( "lhcb_user" )
  requestContainer.setDIRACSetup( "LHCb-Production" )
  requestContainer.setSourceComponent( None )
  requestContainer.setCreationTime( "0000-00-00 00:00:00" )
  requestContainer.setLastUpdate( "2011-02-19 04:57:02" )
  requestContainer.setStatus( "Waiting" )
  

  requestContainer.initiateSubRequest( "register" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2259916, 
                     "Operation" : "registerFile", 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "RAL-USER",
                     "Catalogue" : "LcgFileCatalogCombined", 
                     "CreationTime" : "2011-02-19 04:57:02", 
                     "SubmissionTime" : "2011-02-19 04:57:02",
                     "LastUpdate" : "2011-08-18 20:14:22" }
  requestContainer.setSubRequestAttributes( 0, "register", subRequestDict )
  
  files =  [ { "FileID" : 1610538, 
               "LFN" : "/lhcb/user/c/cblanks/11889/11889410/LDSB.rsQrRL", 
               "Size" : 153961749, 
               "PFN" : "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cblanks/11889/11889410/LDSB.rsQrRL", 
               "GUID" : "5911A19C-7CDF-7F2A-36ED-089CD410F98A", 
               "Md5" : None, 
               "Addler" : "92b85e26", 
               "Attempt" : 1, 
               "Status" : "Waiting" } ]    

  requestContainer.setSubRequestFiles( 0, "register", files )

  return { "requestName" : "11889410.xml",
           "requestString" : requestContainer.toXML()["Value"],
           "jobID" : 11889410,
           "executionOrder" : 0,
           "sourceServer" : "foobarserver",
           "configPath" : "/Systems/DataManagement/Development/Agents/RegistrationAgent" }

########################################################################
class RegisterTaskTests(unittest.TestCase):
  """
  .. class:: RegisterTaskTests
  
  """

  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    kwargs = getRegisterRequest()

    self.registerTask = RegistrationTask( **kwargs )
   
    self.registerTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    self.registerTask.dataLoggingClient().addFileRecord = Mock()
    self.registerTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    self.registerTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 

    self.registerTask.requestClient().updateRequest = Mock()
    self.registerTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    self.registerTask.requestClient().getRequestStatus = Mock()
    self.registerTask.requestClient().getRequestStatus.return_value = { "OK" : True, 
                                                                        "Value" : { "RequestStatus" : "Done", 
                                                                                    "SubRequestStatus" : "Done" }}


    self.registerTask.requestClient().finalizeRequest = Mock()
    self.registerTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    self.registerTask.dataManager = Mock( return_value = Mock( spec = DataManager ) )
    self.registerTask.dm.registerFile = Mock()
    self.registerTask.dm.registerFile.return_value = { "OK" : True,
                                                                    "Value" : 
                                                                    { "Failed" : {},
                                                                      "Succesfull" : { "/lhcb/user/c/cblanks/11889/11889410/LDSB.rsQrRL" : True } } } 

  def test_01_call( self ):
    """ call test """
    self.assertEqual( self.registerTask.__call__(), { 'OK': True, 
                                                      'Value': { 'monitor': { 'Execute': 1, 'Done': 1} } } )


## suite execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase(RegisterTaskTests)     
  unittest.TextTestRunner(verbosity=3).run(suite)
