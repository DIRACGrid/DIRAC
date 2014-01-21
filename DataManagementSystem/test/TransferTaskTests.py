# TODO: to be removed

########################################################################
# $HeadURL $
# File: TransferTaskTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/12/09 08:08:13
########################################################################

""" :mod: TransferTaskTests 
    =======================
 
    .. module: TransferTaskTests
    :synopsis: test case for TransferTask
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test case for TransferTask

    OBSOLETE
    K.C.
"""

__RCSID__ = "$Id $"

##
# @file TransferTaskTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/12/09 08:08:44
# @brief Definition of TransferTaskTests class.

## imports 
import unittest
from mock import *
import random, os, sys, datetime
## from DIRAC
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine() 
from DIRAC import S_OK, S_ERROR, gConfig, gLogger 
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
## tested code
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.DataManagementSystem.private.TransferTask import TransferTask

#####################################################################################
## mock littel helpers

## dummy S_OK for mocking
SOK = { "OK" : True, "Value" : None }
def getRequest( operation ):
  """ fake requestDict 

  :param str operation: sub-request operation attribute 
  """ 
  requestContainer = RequestContainer( init = False )
  requestContainer.setJobID( 1 )
  #requestContainer.setOwnerDN( "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba" )
  requestContainer.setOwnerGroup( "lhcb_user" )
  requestContainer.setDIRACSetup( "LHCb-Production" )
  requestContainer.setSourceComponent( None )
  requestContainer.setCreationTime( "0000-00-00 00:00:00" )
  requestContainer.setLastUpdate( "2011-12-01 04:57:02" )
  requestContainer.setStatus( "Waiting" )
  requestContainer.setAttribute( "RequestID", 123456789  )
  requestContainer.initiateSubRequest( "transfer" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2222222, 
                     "Operation" : operation, 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "CERN-USER",
                     "Catalogue" : "LcgFileCatalogCombined", 
                     "CreationTime" : "2011-12-01 04:57:02", 
                     "SubmissionTime" : "2011-12-01 04:57:02",
                     "LastUpdate" : "2011-12-01 20:14:22" }
  requestContainer.setSubRequestAttributes( 0, "transfer", subRequestDict )  
  files =  [ { "FileID" : 3333333, 
               "LFN" : "/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "Size" : 44444444, 
               "PFN" : "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "GUID" : "5P13RD4L-4J5L-3D21-U5P1-3RD4L4J5P13R", 
               "Md5" : None, 
               "Addler" : "92b85e26", 
               "Attempt" : 1, 
               "Status" : "Waiting" } ]    
  requestContainer.setSubRequestFiles( 0, "transfer", files )
  return { "OK" : True, "Value" : { "requestName" : "%s.xml" % operation,
                                    "requestString" : requestContainer.toXML_new()["Value"],
                                    "requestObj" : requestContainer,
                                    "jobID" : 1,
                                    "executionOrder" : 0,
                                    "sourceServer" : "foobarserver" } }
def getKwargs( operation ):
  """ fake kwargs for request task
  
  :param str operation: sub-request operation attribute
  """
  reqDict = getRequest( operation )["Value"]
  del reqDict["requestObj"]
  reqDict["configPath"] = "/Systems/DataManagement/Development/Agents/TransferAgent"
  return reqDict

class TransferTaskTests( unittest.TestCase ):
  """ 
  .. class:: TransferTaskTests

  test case for TransferTask """
             
  def test_02_putAndRegister( self ):
    """ 'putAndRegister' operation """
    kwargs = getKwargs( "putAndRegister" )
    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = SOK
    tTask.requestClient = Mock( return_value = Mock( spec=RequestClient ) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = SOK

    tTask.requestClient().getRequestStatus = Mock()
    tTask.requestClient().getRequestStatus.return_value = { "OK" : True, 
                                                            "Value" : { "RequestStatus" : "Done", 
                                                                        "SubRequestStatus" : "Done" }}
    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = SOK


    tTask.dataManager = Mock( return_value = Mock( spec = DataManager ) )
    tTask.dm.put = Mock()
    tTask.dm.putAndRegister.return_value = { "OK": True,
                                                            "Value": { "Failed": {}, 
                                                                       "Successful": { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : 
                                                                                       { "put": 1, "register": 1 } } } }
    self.assertEqual( tTask.__call__(),
                      { 'OK': True, 
                        'Value': { 'monitor': 
                                   { 'File registration successful': 1, 
                                     'Execute': 1, 
                                     'Done': 1, 
                                     'Put successful': 1, 
                                     'Put and register': 1}}} )
    del tTask

  def test_05_replicateAndRegister( self ):
    """ 'replicateAndRegister' operation """
    kwargs = getKwargs( "replicateAndRegister" )
    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = SOK
    tTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = SOK
    tTask.requestClient().getRequestStatus = Mock()
    tTask.requestClient().getRequestStatus.return_value = { "OK" : True, 
                                                            "Value" : { "RequestStatus" : "Done", 
                                                                        "SubRequestStatus" : "Done" }}
    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = SOK
    tTask.dataManager = Mock( return_value = Mock( spec = DataManager ) )
    tTask.dm.replicateAndRegister = Mock()
    tTask.dm.replicateAndRegister.return_value = { "OK": True,
                                                                 "Value": { 
        "Failed": {}, 
        "Successful": { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : { "replicate": 1, "register": 1 } } } }

    self.assertEqual( tTask(),
                      { 'OK': True, 
                        'Value': { 'monitor': 
                                   { 'Replica registration successful': 1, 
                                     'Execute': 1, 
                                     'Done': 1, 
                                     'Replication successful': 1, 
                                     'Replicate and register': 1}}} )
                      
    del tTask


### test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suiteTT = testLoader.loadTestsFromTestCase( TransferTaskTests )
  suite = unittest.TestSuite( [ suiteTT ] )
  unittest.TextTestRunner(verbosity=3).run(suite)
