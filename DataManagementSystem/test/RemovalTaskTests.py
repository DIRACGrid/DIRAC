# TODO: to be removed

########################################################################
# $HeadURL $
# File: RemovalTaskTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/12/01 15:31:29
########################################################################

""" :mod: RemovalTaskTests 
    =======================
 
    .. module: RemovalTaskTests
    :synopsis: test case for RemovalTask
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test case for RemovalTask

    OBSOLETE
    K.C.
"""

__RCSID__ = "$Id $"

##
# @file RemovalTaskTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/12/01 15:31:39
# @brief Definition of RemovalTaskTests class.

## imports 
import unittest
from mock import *

global MySQL, DB, RequestDBMySQL

from DIRAC.Core.Base.Script import parseCommandLine
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
from DIRAC.DataManagementSystem.private.RemovalTask import RemovalTask

from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager 
gProxyManager = Mock( spec=gProxyManager.__class__ )

def getKwargsReTransfer():
  """ create reTransfer operation in RequestContainer """
  requestContainer = RequestContainer( init = False )
  requestContainer.setJobID( 1 )
  #requestContainer.setOwnerDN( "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba" )
  requestContainer.setOwnerGroup( "lhcb_user" )
  requestContainer.setDIRACSetup( "LHCb-Production" )
  requestContainer.setSourceComponent( None )
  requestContainer.setCreationTime( "0000-00-00 00:00:00" )
  requestContainer.setLastUpdate( "2011-12-01 04:57:02" )
  requestContainer.setStatus( "Waiting" )
  requestContainer.initiateSubRequest( "removal" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2222222, 
                     "Operation" : "reTransfer", 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "RAL-USER",
                     "Catalogue" : "LcgFileCatalogCombined", 
                     "CreationTime" : "2011-12-01 04:57:02", 
                     "SubmissionTime" : "2011-12-01 04:57:02",
                     "LastUpdate" : "2011-12-01 20:14:22" }
  requestContainer.setSubRequestAttributes( 0, "removal", subRequestDict )  
  files =  [ { "FileID" : 3333333, 
               "LFN" : "/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "Size" : 44444444, 
               "PFN" : "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "GUID" : "5P13RD4L-4J5L-3D21-U5P1-3RD4L4J5P13R", 
               "Md5" : None, 
               "Addler" : "92b85e26", 
               "Attempt" : 1, 
               "Status" : "Waiting" } ]    
  requestContainer.setSubRequestFiles( 0, "removal", files )
  return { "requestName" : "0000003.xml",
           "requestString" : requestContainer.toXML()["Value"],
           "jobID" : 1,
           "executionOrder" : 0,
           "sourceServer" : "foobarserver",
           "configPath" : "/Systems/DataManagement/Development/Agents/RemovalAgent" }


def getKwargsPhysicalRemoval():
  """ create 'physicalRemoval' operation RequestContainer """
  requestContainer = RequestContainer( init = False )
  requestContainer.setJobID( 1 )
  #requestContainer.setOwnerDN( "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba" )
  requestContainer.setOwnerGroup( "lhcb_user" )
  requestContainer.setDIRACSetup( "LHCb-Production" )
  requestContainer.setSourceComponent( None )
  requestContainer.setCreationTime( "0000-00-00 00:00:00" )
  requestContainer.setLastUpdate( "2011-12-01 04:57:02" )
  requestContainer.setStatus( "Waiting" )
  requestContainer.initiateSubRequest( "removal" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2222222, 
                     "Operation" : "physicalRemoval", 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "RAL-USER",
                     "Catalogue" : "LcgFileCatalogCombined", 
                     "CreationTime" : "2011-12-01 04:57:02", 
                     "SubmissionTime" : "2011-12-01 04:57:02",
                     "LastUpdate" : "2011-12-01 20:14:22" }
  requestContainer.setSubRequestAttributes( 0, "removal", subRequestDict )  
  files =  [ { "FileID" : 3333333, 
               "LFN" : "/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "Size" : 44444444, 
               "PFN" : "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "GUID" : "5P13RD4L-4J5L-3D21-U5P1-3RD4L4J5P13R", 
               "Md5" : None, 
               "Addler" : "92b85e26", 
               "Attempt" : 1, 
               "Status" : "Waiting" } ]    
  requestContainer.setSubRequestFiles( 0, "removal", files )
  return { "requestName" : "0000002.xml",
           "requestString" : requestContainer.toXML()["Value"],
           "jobID" : 1,
           "executionOrder" : 0,
           "sourceServer" : "foobarserver",
           "configPath" : "/Systems/DataManagement/Development/Agents/RemovalAgent" }

def getKwargsRemoveReplica():
  """ create 'replicaRemoval' operation RequestContainer """

  requestContainer = RequestContainer( init = False )
  requestContainer.setJobID( 11111111 )
  #requestContainer.setOwnerDN( "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba" )
  requestContainer.setOwnerGroup( "lhcb_user" )
  requestContainer.setDIRACSetup( "LHCb-Production" )
  requestContainer.setSourceComponent( None )
  requestContainer.setCreationTime( "0000-00-00 00:00:00" )
  requestContainer.setLastUpdate( "2011-12-01 04:57:02" )
  requestContainer.setStatus( "Waiting" )
  requestContainer.initiateSubRequest( "removal" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2222222, 
                     "Operation" : "replicaRemoval", 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "RAL-USER",
                     "Catalogue" : "LcgFileCatalogCombined", 
                     "CreationTime" : "2011-12-01 04:57:02", 
                     "SubmissionTime" : "2011-12-01 04:57:02",
                     "LastUpdate" : "2011-12-01 20:14:22" }
  requestContainer.setSubRequestAttributes( 0, "removal", subRequestDict )  
  files =  [ { "FileID" : 3333333, 
               "LFN" : "/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "Size" : 44444444, 
               "PFN" : "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "GUID" : "5P13RD4L-4J5L-3D21-U5P1-3RD4L4J5P13R", 
               "Md5" : None, 
               "Addler" : "92b85e26", 
               "Attempt" : 1, 
               "Status" : "Waiting" } ]    
  requestContainer.setSubRequestFiles( 0, "removal", files )
  return { "requestName" : "0000000.xml",
           "requestString" : requestContainer.toXML()["Value"],
           "jobID" : 1,
           "executionOrder" : 0,
           "sourceServer" : "foobarserver",
           "configPath" : "/Systems/DataManagement/Development/Agents/RemovalAgent" }

def getKwargsRemoveFile():
  """ helper fcn to build request """
  requestContainer = RequestContainer( init = False )
  requestContainer.setJobID( 11111111 )
  #requestContainer.setOwnerDN( "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba" )
  requestContainer.setOwnerGroup( "lhcb_user" )
  requestContainer.setDIRACSetup( "LHCb-Production" )
  requestContainer.setSourceComponent( None )
  requestContainer.setCreationTime( "0000-00-00 00:00:00" )
  requestContainer.setLastUpdate( "2011-12-01 04:57:02" )
  requestContainer.setStatus( "Waiting" )
  
  requestContainer.initiateSubRequest( "removal" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2222222, 
                     "Operation" : "removeFile", 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "RAL-USER",
                     "Catalogue" : "LcgFileCatalogCombined", 
                     "CreationTime" : "2011-12-01 04:57:02", 
                     "SubmissionTime" : "2011-12-01 04:57:02",
                     "LastUpdate" : "2011-12-01 20:14:22" }
  requestContainer.setSubRequestAttributes( 0, "removal", subRequestDict )  
  files =  [ { "FileID" : 3333333, 
               "LFN" : "/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "Size" : 44444444, 
               "PFN" : "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "GUID" : "5P13RD4L-4J5L-3D21-U5P1-3RD4L4J5P13R", 
               "Md5" : None, 
               "Addler" : "92b85e26", 
               "Attempt" : 1, 
               "Status" : "Waiting" } ]    
  requestContainer.setSubRequestFiles( 0, "removal", files )

  return { "requestName" : "00000001.xml",
           "requestString" : requestContainer.toXML()["Value"],
           "jobID" : 1,
           "executionOrder" : 0,
           "sourceServer" : "foobarserver",
           "configPath" : "/Systems/DataManagement/Development/Agents/RemovalAgent" }

########################################################################
class RemovalTaskTests( unittest.TestCase ):
  """
  .. class:: RemovalTask_RemoveReplicaTests

  """
  
  def test_01_removeReplica( self ):
    """ removeReplica operation

    """
    kwargs = getKwargsRemoveReplica()
    removalTask = RemovalTask( **kwargs )
    removalTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    removalTask.dataLoggingClient().addFileRecord = Mock()
    removalTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    removalTask.getProxyForLFN = Mock( return_value = { "OK" : True, "Value" : None} )
    

    removalTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 

    removalTask.requestClient().updateRequest = Mock()
    removalTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    removalTask.requestClient().getRequestStatus = Mock()
    removalTask.requestClient().getRequestStatus.return_value = { "OK" : True, 
                                                                  "Value" : { "RequestStatus" : "Done", 
                                                                              "SubRequestStatus" : "Done" }}
    removalTask.requestClient().finalizeRequest = Mock()
    removalTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    removalTask.dataManager = Mock( return_value = Mock( spec = DataManager ) )
    removalTask.dm.removeReplica = Mock()
    removalTask.dm.removeReplica.return_value = { "OK" : True,
                                                                 "Value" : { "Failed" : {},
                                                                   "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : True } } }
    
    self.assertEqual( removalTask.__call__(),
                      {'OK': True, 'Value': {'monitor': {'Done': 1, 'Execute': 1, 'ReplicaRemovalAtt': 1, 'ReplicaRemovalFail': 0, 'ReplicaRemovalDone': 1}}} )
   
    del removalTask

  def test_02_removeFile( self ):
    """ removeFile operation """

    kwargs = getKwargsRemoveFile()
    removalTask = RemovalTask( **kwargs )
    removalTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    removalTask.dataLoggingClient().addFileRecord = Mock()
    removalTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    removalTask.getProxyForLFN = Mock( return_value = { "OK" : True, "Value" : None} )
    removalTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 

    removalTask.requestClient().updateRequest = Mock()
    removalTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    removalTask.requestClient().getRequestStatus = Mock()
    removalTask.requestClient().getRequestStatus.return_value = { "OK" : True, 
                                                                  "Value" : { "RequestStatus" : "Done", 
                                                                              "SubRequestStatus" : "Done" }}

    removalTask.requestClient().finalizeRequest = Mock()
    removalTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    removalTask.dataManager = Mock( return_value = Mock( spec = DataManager ) )
    removalTask.dm.removeFile = Mock( return_value = { "OK" : True,
                                                                       "Value" : 
                                                                       { "Failed" : {},
                                                                         "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : True } } } )
    self.assertEqual( removalTask.__call__(), 
                      {'OK': True, 
                       'Value': { 'monitor': 
                                  { 'RemoveFileFail': 0, 
                                    'Execute': 1, 
                                    'Done': 1, 
                                    'RemoveFileDone': 1, 
                                    'RemoveFileAtt': 1}}} )
    

    del removalTask


  def test_03_physicalRemoval( self ):
    kwargs = getKwargsPhysicalRemoval()
    removalTask = RemovalTask( **kwargs )
    removalTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    removalTask.dataLoggingClient().addFileRecord = Mock()
    removalTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    removalTask.getProxyForLFN = Mock( return_value = { "OK" : True, "Value" : None} )

    removalTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 

    removalTask.requestClient().updateRequest = Mock()
    removalTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    removalTask.requestClient().getRequestStatus = Mock()
    removalTask.requestClient().getRequestStatus.return_value = { "OK" : True, 
                                                                  "Value" : { "RequestStatus" : "Done", 
                                                                              "SubRequestStatus" : "Done" }}
    removalTask.requestClient().finalizeRequest = Mock()
    removalTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    removalTask.dataManager = Mock( return_value = Mock( spec = DataManager ) )
    removalTask.dm.removeStorageFile = Mock()
    removalTask.dm.removeStorageFile.return_value = { "OK" : True,
                                                                    "Value" : 
                                                                    { "Failed" : {},
                                                                      "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : True } } }
    self.assertEqual( removalTask.__call__(),
                      {'OK': True, 'Value': {'monitor': {'Done': 1, 'Execute': 1, 'PhysicalRemovalAtt': 1, 'PhysicalRemovalDone': 1}}} )
    del removalTask


  def test_04_reTransfer(self):
    kwargs = getKwargsReTransfer()
    removalTask = RemovalTask( **kwargs )
    removalTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    removalTask.dataLoggingClient().addFileRecord = Mock()
    removalTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    removalTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 

    removalTask.requestClient().updateRequest = Mock()
    removalTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    removalTask.requestClient().getRequestStatus = Mock()
    removalTask.requestClient().getRequestStatus.return_value = { "OK" : True, 
                                                                  "Value" : { "RequestStatus" : "Done", 
                                                                              "SubRequestStatus" : "Done" }}
    removalTask.requestClient().finalizeRequest = Mock()
    removalTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    removalTask.dataManager = Mock( return_value = Mock( spec = DataManager ) )
    removalTask.dm.onlineRetransfer = Mock()
    removalTask.dm.onlineRetransfer.return_value = { "OK" : True,
                                                                    "Value" : 
                                                                    { "Failed" : {},
                                                                      "Successful" : { "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz" : True } } }
    self.assertEqual( removalTask.__call__(),
                      {'OK': True, 'Value': {'monitor': {'Execute': 1, 'Done': 1}}} )
    del removalTask

## suite execution
if __name__ == "__main__":
  #parseCommandLine()
  testLoader = unittest.TestLoader()
  suiteRemovalTask = testLoader.loadTestsFromTestCase( RemovalTaskTests )
  suite = unittest.TestSuite( [ suiteRemovalTask ] )
  unittest.TextTestRunner(verbosity=3).run(suite)
