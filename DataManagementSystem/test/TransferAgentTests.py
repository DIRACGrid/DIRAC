########################################################################
# $HeadURL $
# File: TransferAgentTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/11/28 10:10:13
########################################################################

""" :mod: TransferAgentTests 
    =======================
 
    .. module: TransferAgentTests
    :synopsis: unitest for TransferAgent and TransferTask
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for TransferAgent and TransferTask
"""

__RCSID__ = "$Id $"

##
# @file TransferAgentTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/11/28 10:10:26
# @brief Definition of TransferAgentTests class.

## imports 
import unittest
from mock import *

## DIRAC generic tools
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
## tested code
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.DataManagementSystem.Agent.TransferAgent import TransferAgent
from DIRAC.DataManagementSystem.Agent.TransferTask import TransferTask

## agent name
AGENT_NAME = "DataManagement/TransferAgent"

def getKwargs( operation ):
  """ fake kwargs for task
  
  :param str operation: sub-request operation name 
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
  requestContainer.initiateSubRequest( "transfer" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2222222, 
                     "Operation" : operation, 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "RAL-USER",
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
  return { "requestName" : "%s.xml" % operation,
           "requestString" : requestContainer.toXML()["Value"],
           "jobID" : 1,
           "executionOrder" : 0,
           "sourceServer" : "foobarserver",
           "configPath" : "/Systems/DataManagement/Development/Agents/TransferAgent" }


class TransferTaskTests( unittest.TestCase ):
  """ test case for TransferTask """

  def test_01_put( self ):
    """ 'put' operation """
    kwargs = getKwargs( "put" )
    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    tTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    tTask.replicaManager = Mock( return_value = Mock( spec=ReplicaManager) )
    tTask.replicaManager().put = Mock()
    tTask.replicaManager().put.return_value =  { "OK" : True,
                                                 "Value" : 
                                                 { "Failed" : {},
                                                   "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : True } } }
    
    self.assertEqual( tTask.__call__(),
                      {'OK': True, 'Value': {'monitor': {'Put': 1, 'Execute': 1, 'Done': 1, 'Put successful': 1}}} )
    del tTask
             
  def test_02_putAndRegister( self ):
    """ 'putAndRegister' operation """
    kwargs = getKwargs( "putAndRegister" )
    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    tTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    tTask.replicaManager = Mock( return_value = Mock( spec=ReplicaManager) )
    tTask.replicaManager().put = Mock()
    tTask.replicaManager().putAndRegister.return_value =  { "OK" : True,
                                                            "Value" : 
                                                            { "Failed" : {},
                                                              "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : { "put" : 1,
                                                                                                                                "register" : 1 } } } }
    

    self.assertEqual( tTask.__call__(),
                      {'OK' : True, 
                       'Value' : { 'monitor' : 
                                   { 'File registration successful': 1, 
                                     'Execute': 1, 
                                     'Done': 1, 
                                     'Put successful': 1, 
                                     'Put and register': 1 } } } )

    del tTask

  def test_03_putAndRegisterAndRemove( self ):
    """ 'putAndRegisterAndRemove' operation """
    kwargs = getKwargs( "putAndRegisterAndRemove" )

    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    tTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    tTask.replicaManager = Mock( return_value = Mock( spec=ReplicaManager) )
    tTask.replicaManager().put = Mock()
    tTask.replicaManager().putAndRegister.return_value =  { "OK" : True,
                                                            "Value" : 
                                                            { "Failed" : {},
                                                              "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : { "put" : 1,
                                                                                                                                "register" : 1 } } } }


    self.assertEqual( tTask.__call__(),
                      {'OK': True, 'Value': {'monitor': {'File registration successful': 1, 'Execute': 1, 'Done': 1, 'Put successful': 1, 'Put and register': 1}}} )
    del tTask


  def test_04_replicate( self ):
    """ 'replicate' operation """
    kwargs = getKwargs( "replicate" )

    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    tTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    tTask.replicaManager = Mock( return_value = Mock( spec=ReplicaManager) )
    tTask.replicaManager().replicate = Mock()
    tTask.replicaManager().replicate.return_value =  { "OK" : True,
                                                            "Value" : 
                                                            { "Failed" : {},
                                                              "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : True } } }
    self.assertEqual( tTask(),
                      {'OK': True, 'Value': {'monitor': {'Done': 1, 'Execute': 1, 'Replicate': 1, 'Replication successful': 1}}} )
    
    del tTask

  def test_05_replicateAndRegister( self ):
    """ 'replicateAndRegister' operation """
    kwargs = getKwargs( "replicateAndRegister" )

    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    tTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    tTask.replicaManager = Mock( return_value = Mock( spec=ReplicaManager) )
    tTask.replicaManager().replicateAndRegister = Mock()
    tTask.replicaManager().replicateAndRegister.return_value =  { "OK" : True,
                                                                  "Value" : 
                                                                  { "Failed" : {},
                                                                    "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : {
            "replicate" : 1,
            "register" : 1
            } } } }

    
    self.assertEqual( tTask(),
                      {'OK': True, 'Value': {'monitor': {'Replica registration successful': 1, 'Execute': 1, 'Done': 1, 'Replication successful': 1, 'Replicate and register': 1}}} )

    
    del tTask

  def test_06_replicateAndRegisterAndRemove( self ):
    """ 'replicateAndRegisterAndRemove' operation """
    kwargs = getKwargs( "replicateAndRegisterAndRemove" )

    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    tTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    tTask.replicaManager = Mock( return_value = Mock( spec=ReplicaManager) )
    tTask.replicaManager().replicateAndRegister = Mock()
    tTask.replicaManager().replicateAndRegister.return_value =  { "OK" : True,
                                                                  "Value" : 
                                                                  { "Failed" : {},
                                                                    "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : {
            "replicate" : 1,
            "register" : 1
            } } } }

    
    
    self.assertEqual( tTask(),
                      {'OK': True, 'Value': {'monitor': {'Replica registration successful': 1, 'Execute': 1, 'Done': 1, 'Replication successful': 1, 'Replicate and register': 1}}} )
    
    del tTask


  def test_07_get( self ):
    """ 'get' operation """
    kwargs = getKwargs( "get" )

    tTask = TransferTask( **kwargs )
    tTask.dataLoggingClient = Mock( return_value = Mock(spec = DataLoggingClient ) )
    tTask.dataLoggingClient().addFileRecord = Mock()
    tTask.dataLoggingClient().addFileRecord.return_value = { "OK" : True, "Value" : "" }

    tTask.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    tTask.requestClient().updateRequest = Mock()
    tTask.requestClient().updateRequest.return_value = { "OK" : True, "Value" : None }

    tTask.requestClient().finalizeRequest = Mock()
    tTask.requestClient().finalizeRequest.return_value = { "OK" : True, "Value" : None }

    tTask.replicaManager = Mock( return_value = Mock( spec=ReplicaManager) )

    tTask.replicaManager().getStorageFile = Mock()
    tTask.replicaManager().getStorageFile.return_value =  { "OK" : True,
                                                            "Value" : 
                                                            { "Failed" : {},
                                                              "Successful" : {  "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz" : True } } }
    
    tTask.replicaManager().getFile = Mock()
    tTask.replicaManager().getFile.return_value =  { "OK" : True,
                                                     "Value" : 
                                                     { "Failed" : {},
                                                       "Successful" : { "/lhcb/user/c/cibak/11889/11889410/test.zzz" : True }}}

    
    self.assertEqual( tTask(),
                      {'OK': True, 'Value': {'monitor': {'Execute': 1, 'Done': 1}}} )
    
    del tTask

    
class TransferAgentTests( unittest.TestCase ):
  """ test case for TransferAgent

  """

  def setUp( self ):
    pass

  def test__01_ctor( self ):
    """ test c'tor """
    agent = None
    try:
      agent = TransferAgent( AGENT_NAME )
    except:
      pass
    self.assertEqual( isinstance( agent, TransferAgent ), True )

    
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suiteTA = testLoader.loadTestsFromTestCase( TransferAgentTests )     
  suiteTT = testLoader.loadTestsFromTestCase( TransferTaskTests )
  suite = unittest.TestSuite( [ suiteTA, suiteTT ] )
  unittest.TextTestRunner(verbosity=3).run(suite)
