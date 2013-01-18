########################################################################
# $HeadURL $
# File: FixSURLEqTURLPluginTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/08/17 07:49:56
########################################################################

""" :mod: FixSURLEqTURLPluginTests 
    =======================
 
    .. module: FixSURLEqTURLPluginTests
    :synopsis: unit tests from FixSURLEqTURLPlugin
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unit tests from FixSURLEqTURLPlugin
"""

__RCSID__ = "$Id $"

##
# @file FixSURLEqTURLPluginTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/08/17 07:50:11
# @brief Definition of FixSURLEqTURLPluginTests class.

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
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

from DIRAC.DataManagementSystem.private.FTSCurePlugin import FTSCurePlugin, injectFunction
from DIRAC.DataManagementSystem.private.FixSURLEqTURLPlugin import FixSURLEqTURLPlugin
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase

def getRequest():
  """
  helper fcn to build requestContainer
  """

  requestContainer = RequestContainer( init = False )

  ## get request
  requestContainer.setRequestName( "00009423_00000118" )
  requestContainer.setJobID( 0 )
  requestContainer.setOwnerDN( "" )
  requestContainer.setOwnerGroup( "" )
  requestContainer.setDIRACSetup( "" )
  requestContainer.setSourceComponent( None )
  requestContainer.setCreationTime( "0000-00-00 00:00:00" )
  requestContainer.setLastUpdate( "2011-02-19 04:57:02" )
  requestContainer.setStatus( "Waiting" )
  
  ## get subrequest
  requestContainer.initiateSubRequest( "transfer" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2259916, 
                     "Operation" : "replicateAndRegister", 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "GRIDKA_MC-DST,GRIDKA_MC_M-DST",
                     "Catalogue" : None, 
                     "CreationTime" : "2011-02-19 04:57:02", 
                     "SubmissionTime" : "2011-02-19 04:57:02",
                     "LastUpdate" : "2011-08-18 20:14:22" }
  requestContainer.setSubRequestAttributes( 0, "transfer", subRequestDict )

  ## get subrequest files
  files =  [ { "FileID" : 1610538, 
               "LFN" : "/lhcb/MC/MC10/ALLSTREAMS.DST/00009422/0000/00009422_00000171_1.allstreams.dst", 
               "Size" : None, 
               "PFN" : None, 
               "GUID" : None, 
               "Md5" : None, 
               "Addler" : None, 
               "Attempt" : 1, 
               "Status" : "Scheduled" } ]    

  requestContainer.setSubRequestFiles( 0, "transfer", files )
  return { "OK" : True,
           "Value" : { "RequestName" : "00009423_00000118",
                       "RequestString" : requestContainer.toXML()["Value"],
                       "JobID" : 0,
                       "RequestContainer" : requestContainer } }

########################################################################
class FixSURLEqTURLPluginTests(unittest.TestCase):
  """
  .. class:: FixSURLEqTURLPluginTests
  
  """

  def setUp( self ):
    """c'tor
    :param self: self reference
    """

    self.fixSURLEqTURLPlugin = FixSURLEqTURLPlugin()
    self.fixSURLEqTURLPlugin.__requestDBMySQL = Mock(spec=RequestDBMySQL)

    
  def test_NoRequestNorSubRequest( self ):
    """
    SubRequestID = 0, no SubRequest nor Request in RequestDB
    """
    ## mocking 
    self.fixSURLEqTURLPlugin.requestDBMySQL().selectFileSourceSURLEqTargetSURL = Mock()
    self.fixSURLEqTURLPlugin.requestDBMySQL().selectFileSourceSURLEqTargetSURL.return_value = { 
      "OK" : True, 
      "Value" : ( 1558187, 
                  "/lhcb/MC/MC10/ALLSTREAMS.DST/00009359/0000/00009359_00000005_1.allstreams.dst",
                  17, 
                  "GRIDKA_MC_M-DST",
                  "GRIDKA_MC-DST",
                  0 ) }
    self.fixSURLEqTURLPlugin.requestDBMySQL().countLFNInFiles = Mock()
    self.fixSURLEqTURLPlugin.requestDBMySQL().countLFNInFiles.return_value = {
      "OK" : True,
      "Value" : ( 1, )
      }
    self.fixSURLEqTURLPlugin.requestDBMySQL().deleteFileAndChannel = Mock()
    self.fixSURLEqTURLPlugin.requestDBMySQL().deleteFileAndChannel.return_value = {
      "OK" : True,
      "Value" : tuple()
      }

    ## excute plugin
    ret = self.fixSURLEqTURLPlugin.execute()
    self.assertEqual( ret, {'OK': True, 'Value': ()} )

  def test_RequestPresent( self ):
    """
    Request is there
    """

    ## mocking
    self.fixSURLEqTURLPlugin.requestDBMySQL().selectFileSourceSURLEqTargetSURL = Mock()
    self.fixSURLEqTURLPlugin.requestDBMySQL().selectFileSourceSURLEqTargetSURL.return_value = { 
      "OK" : True, 
      "Value" : ( 1610538,
                  "/lhcb/MC/MC10/ALLSTREAMS.DST/00009422/0000/00009422_00000171_1.allstreams.dst",
                  17, 
                  "GRIDKA_MC_M-DST",
                  "GRIDKA_MC-DST", 
                  2259916 ) }


    self.fixSURLEqTURLPlugin.requestDBMySQL().getRequestForSubRequest = Mock()
    self.fixSURLEqTURLPlugin.requestDBMySQL().getRequestForSubRequest.return_value = getRequest()
    self.fixSURLEqTURLPlugin.replicaManager().getReplicas = Mock()
    self.fixSURLEqTURLPlugin.replicaManager().getReplicas.return_value = { 
      "OK" : True,
      "Value" : { 'Failed': {},
                  'Successful': {'/lhcb/MC/MC10/ALLSTREAMS.DST/00009422/0000/00009422_00000171_1.allstreams.dst': {'CERN_MC_M-DST': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/MC/MC10/ALLSTREAMS.DST/00009422/0000/00009422_00000171_1.allstreams.dst',
                                                                                                  'GRIDKA_MC_M-DST': 'srm://gridka-dCache.fzk.de/pnfs/gridka.de/lhcb/MC/MC10/ALLSTREAMS.DST/00009422/0000/00009422_00000171_1.allstreams.dst'}}}
      }
    self.fixSURLEqTURLPlugin.requestDBMySQL().selectChannelIDSourceSURLTargetSURL = Mock()
    self.fixSURLEqTURLPlugin.requestDBMySQL().selectChannelIDSourceSURLTargetSURL.return_value = { 
      "OK" : True,
      "Value" : {
        3 : { "SourceSE" : "CERN_MC_M-DST",
              "TargetSE" : "GRIDKA_MC_M-DST", 
              "Status" : "Done", 
              "SourceSURLEqTargetSURL" : False },
        17 : { "SourceSE" : "GRIDKA_MC_M-DST",
               "TargetSE" : "GRIDKA_MC-DST", 
               "Status" : "Waiting", 
               "SourceSURLEqTargetSURL" : True }
          }
        } 

    self.fixSURLEqTURLPlugin.requestClient().updateRequest = Mock()
    self.fixSURLEqTURLPlugin.requestClient().updateRequest.return_value = { "OK" : True, 
                                                                            "Value" : None }

    ## execute plugin
    ret = self.fixSURLEqTURLPlugin.execute()
    self.assertEqual( ret, { "OK": True, "Value" : "" })
    


  
## test suites executions
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase(FixSURLEqTURLPluginTests)     
  unittest.TextTestRunner(verbosity=3).run(suite)
