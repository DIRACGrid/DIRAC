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

import random
import datetime
import sys, os

## DIRAC generic tools
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine() 
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
 
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.DB.RequestDBMySQL  import RequestDBMySQL
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
## tested code
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.DataManagementSystem.Agent.TransferAgent import TransferAgent
from DIRAC.DataManagementSystem.Agent.TransferTask import TransferTask

## agent name
AGENT_NAME = "DataManagement/TransferAgent"

###############################################
# mock little helpers

## dummy SOK without value set
SOK = { "OK" : True, "Value" : None }
## get request
def getRequest( operation ):
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
                     "TargetSE" : "CERN-USER,PIC-USER",
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
                                    "requestString" : requestContainer.toXML()["Value"],
                                    "requestObj" : requestContainer,
                                    "jobID" : 1,
                                    "executionOrder" : 0,
                                    "sourceServer" : "foobarserver" } }

getChannelObservedThroughput = {'OK': True, 'Value': {1L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 2L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 3L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 4L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 5L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 6L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 7L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 8L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 9L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 10L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 11L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 12L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 13L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 14L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 15L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 16L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 17L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 18L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 19L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 20L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 21L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 22L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 23L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 24L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 25L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 26L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 27L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 28L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 29L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 30L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 31L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 32L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 33L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 34L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 35L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 36L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 37L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 38L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 39L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 40L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 41L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 42L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 43L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 44L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 45L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 46L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 47L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 48L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 49L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 50L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 51L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 52L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 53L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 54L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 55L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 56L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 57L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 58L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 59L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 60L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 61L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 62L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 63L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}, 64L: {'SuccessfulFiles': 0, 'FailedFiles': 0, 'Throughput': 0, 'Fileput': 0}}}

getChannelQueues = {'OK': True, 'Value': {1L: {'Status': 'Active', 'Files': 158, 'Destination': 'CERN', 'Source': 'CERN', 'ChannelName': 'CERN-CERN', 'Size': 497060115700}, 2L: {'Status': 'Active', 'Files': 13, 'Destination': 'CNAF', 'Source': 'CERN', 'ChannelName': 'CERN-CNAF', 'Size': 43040791282}, 3L: {'Status': 'Active', 'Files': 13, 'Destination': 'GRIDKA', 'Source': 'CERN', 'ChannelName': 'CERN-GRIDKA', 'Size': 40822516279}, 4L: {'Status': 'Active', 'Files': 15, 'Destination': 'IN2P3', 'Source': 'CERN', 'ChannelName': 'CERN-IN2P3', 'Size': 55339050482}, 5L: {'Status': 'Active', 'Files': 3, 'Destination': 'NIKHEF', 'Source': 'CERN', 'ChannelName': 'CERN-NIKHEF', 'Size': 12443946392}, 6L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'CERN', 'ChannelName': 'CERN-PIC', 'Size': 0}, 7L: {'Status': 'Active', 'Files': 14, 'Destination': 'RAL', 'Source': 'CERN', 'ChannelName': 'CERN-RAL', 'Size': 53777907265}, 8L: {'Status': 'Active', 'Files': 31, 'Destination': 'CERN', 'Source': 'CNAF', 'ChannelName': 'CNAF-CERN', 'Size': 70181418168}, 9L: {'Status': 'Active', 'Files': 11, 'Destination': 'CNAF', 'Source': 'CNAF', 'ChannelName': 'CNAF-CNAF', 'Size': 39864975697}, 10L: {'Status': 'Active', 'Files': 9, 'Destination': 'GRIDKA', 'Source': 'CNAF', 'ChannelName': 'CNAF-GRIDKA', 'Size': 14137847219}, 11L: {'Status': 'Active', 'Files': 1, 'Destination': 'IN2P3', 'Source': 'CNAF', 'ChannelName': 'CNAF-IN2P3', 'Size': 3721057460}, 12L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'CNAF', 'ChannelName': 'CNAF-NIKHEF', 'Size': 0}, 13L: {'Status': 'Active', 'Files': 2, 'Destination': 'PIC', 'Source': 'CNAF', 'ChannelName': 'CNAF-PIC', 'Size': 9905837267}, 14L: {'Status': 'Active', 'Files': 3, 'Destination': 'RAL', 'Source': 'CNAF', 'ChannelName': 'CNAF-RAL', 'Size': 14383112902}, 15L: {'Status': 'Active', 'Files': 19, 'Destination': 'CERN', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-CERN', 'Size': 50045383225}, 16L: {'Status': 'Active', 'Files': 3, 'Destination': 'CNAF', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-CNAF', 'Size': 8827259343}, 17L: {'Status': 'Active', 'Files': 11, 'Destination': 'GRIDKA', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-GRIDKA', 'Size': 46622696400}, 18L: {'Status': 'Active', 'Files': 9, 'Destination': 'IN2P3', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-IN2P3', 'Size': 44340821396}, 19L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-NIKHEF', 'Size': 0}, 20L: {'Status': 'Active', 'Files': 2, 'Destination': 'PIC', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-PIC', 'Size': 4889831360}, 21L: {'Status': 'Active', 'Files': 8, 'Destination': 'RAL', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-RAL', 'Size': 35523993123}, 22L: {'Status': 'Active', 'Files': 7, 'Destination': 'CERN', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-CERN', 'Size': 5129491612}, 23L: {'Status': 'Active', 'Files': 10, 'Destination': 'CNAF', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-CNAF', 'Size': 5597505586}, 24L: {'Status': 'Active', 'Files': 13, 'Destination': 'GRIDKA', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-GRIDKA', 'Size': 20221027044}, 25L: {'Status': 'Active', 'Files': 10, 'Destination': 'IN2P3', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-IN2P3', 'Size': 15618767652}, 26L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-NIKHEF', 'Size': 0}, 27L: {'Status': 'Active', 'Files': 4, 'Destination': 'PIC', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-PIC', 'Size': 14683443411}, 28L: {'Status': 'Active', 'Files': 7, 'Destination': 'RAL', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-RAL', 'Size': 14243663100}, 29L: {'Status': 'Active', 'Files': 16, 'Destination': 'CERN', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-CERN', 'Size': 46552378554}, 30L: {'Status': 'Active', 'Files': 6, 'Destination': 'CNAF', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-CNAF', 'Size': 16090030361}, 31L: {'Status': 'Active', 'Files': 3, 'Destination': 'GRIDKA', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-GRIDKA', 'Size': 13208436190}, 32L: {'Status': 'Active', 'Files': 3, 'Destination': 'IN2P3', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-IN2P3', 'Size': 7033684541}, 33L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-NIKHEF', 'Size': 0}, 34L: {'Status': 'Active', 'Files': 0, 'Destination': 'PIC', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-PIC', 'Size': 0}, 35L: {'Status': 'Active', 'Files': 3, 'Destination': 'RAL', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-RAL', 'Size': 8851673515}, 36L: {'Status': 'Active', 'Files': 144, 'Destination': 'CERN', 'Source': 'PIC', 'ChannelName': 'PIC-CERN', 'Size': 515941426986}, 37L: {'Status': 'Active', 'Files': 38, 'Destination': 'CNAF', 'Source': 'PIC', 'ChannelName': 'PIC-CNAF', 'Size': 118480456416}, 38L: {'Status': 'Active', 'Files': 3, 'Destination': 'GRIDKA', 'Source': 'PIC', 'ChannelName': 'PIC-GRIDKA', 'Size': 9211228103}, 39L: {'Status': 'Active', 'Files': 9, 'Destination': 'IN2P3', 'Source': 'PIC', 'ChannelName': 'PIC-IN2P3', 'Size': 41163886580}, 40L: {'Status': 'Active', 'Files': 34, 'Destination': 'NIKHEF', 'Source': 'PIC', 'ChannelName': 'PIC-NIKHEF', 'Size': 90808612748}, 41L: {'Status': 'Active', 'Files': 2, 'Destination': 'PIC', 'Source': 'PIC', 'ChannelName': 'PIC-PIC', 'Size': 4603910204}, 42L: {'Status': 'Active', 'Files': 53, 'Destination': 'RAL', 'Source': 'PIC', 'ChannelName': 'PIC-RAL', 'Size': 169288902125}, 43L: {'Status': 'Active', 'Files': 43, 'Destination': 'CERN', 'Source': 'RAL', 'ChannelName': 'RAL-CERN', 'Size': 156653336747}, 44L: {'Status': 'Active', 'Files': 8, 'Destination': 'CNAF', 'Source': 'RAL', 'ChannelName': 'RAL-CNAF', 'Size': 13487974151}, 45L: {'Status': 'Active', 'Files': 3, 'Destination': 'GRIDKA', 'Source': 'RAL', 'ChannelName': 'RAL-GRIDKA', 'Size': 8932788358}, 46L: {'Status': 'Active', 'Files': 7, 'Destination': 'IN2P3', 'Source': 'RAL', 'ChannelName': 'RAL-IN2P3', 'Size': 15113148584}, 47L: {'Status': 'Active', 'Files': 1, 'Destination': 'NIKHEF', 'Source': 'RAL', 'ChannelName': 'RAL-NIKHEF', 'Size': 186426735}, 48L: {'Status': 'Active', 'Files': 1, 'Destination': 'PIC', 'Source': 'RAL', 'ChannelName': 'RAL-PIC', 'Size': 3917796023}, 49L: {'Status': 'Active', 'Files': 20, 'Destination': 'RAL', 'Source': 'RAL', 'ChannelName': 'RAL-RAL', 'Size': 93170693673}, 50L: {'Status': 'Active', 'Files': 1, 'Destination': 'SARA', 'Source': 'CERN', 'ChannelName': 'CERN-SARA', 'Size': 3742350704}, 51L: {'Status': 'Active', 'Files': 114, 'Destination': 'CERN', 'Source': 'SARA', 'ChannelName': 'SARA-CERN', 'Size': 406787517049}, 52L: {'Status': 'Active', 'Files': 40, 'Destination': 'CNAF', 'Source': 'SARA', 'ChannelName': 'SARA-CNAF', 'Size': 146191280765}, 53L: {'Status': 'Active', 'Files': 24, 'Destination': 'GRIDKA', 'Source': 'SARA', 'ChannelName': 'SARA-GRIDKA', 'Size': 105280200446}, 54L: {'Status': 'Active', 'Files': 51, 'Destination': 'IN2P3', 'Source': 'SARA', 'ChannelName': 'SARA-IN2P3', 'Size': 148432861031}, 55L: {'Status': 'Active', 'Files': 0, 'Destination': 'NIKHEF', 'Source': 'SARA', 'ChannelName': 'SARA-NIKHEF', 'Size': 0}, 56L: {'Status': 'Active', 'Files': 36, 'Destination': 'PIC', 'Source': 'SARA', 'ChannelName': 'SARA-PIC', 'Size': 153661657265}, 57L: {'Status': 'Active', 'Files': 20, 'Destination': 'RAL', 'Source': 'SARA', 'ChannelName': 'SARA-RAL', 'Size': 78247765719}, 58L: {'Status': 'Active', 'Files': 16, 'Destination': 'SARA', 'Source': 'SARA', 'ChannelName': 'SARA-SARA', 'Size': 43217857393}, 59L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'CNAF', 'ChannelName': 'CNAF-SARA', 'Size': 0}, 60L: {'Status': 'Active', 'Files': 2, 'Destination': 'SARA', 'Source': 'GRIDKA', 'ChannelName': 'GRIDKA-SARA', 'Size': 9203092031}, 61L: {'Status': 'Active', 'Files': 3, 'Destination': 'SARA', 'Source': 'IN2P3', 'ChannelName': 'IN2P3-SARA', 'Size': 288443230}, 62L: {'Status': 'Active', 'Files': 0, 'Destination': 'SARA', 'Source': 'NIKHEF', 'ChannelName': 'NIKHEF-SARA', 'Size': 0}, 63L: {'Status': 'Active', 'Files': 2, 'Destination': 'SARA', 'Source': 'PIC', 'ChannelName': 'PIC-SARA', 'Size': 10030235449}, 64L: {'Status': 'Active', 'Files': 10, 'Destination': 'SARA', 'Source': 'RAL', 'ChannelName': 'RAL-SARA', 'Size': 37825291471}}}


getCatalogReplicas = {'OK': True, 'Value': {'Successful': {'/lhcb/user/c/cibak/11889/11889410/test.zzz': {'RAL-USER': 'srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz'}}, 'Failed': {}}}

getCatalogFileMetadata = {'OK': True, 'Value': {'Successful': {'/lhcb/user/c/cibak/11889/11889410/test.zzz': {'Status': '-', 'CreationDate': datetime.datetime(2010, 6, 29, 16, 57, 33), 'CheckSumType': 'AD', 'Checksum': 'bdf27d7a', 'NumberOfLinks': 1, 'Mode': 436, 'ModificationDate': datetime.datetime(2010, 6, 29, 16, 57, 33), 'GUID': '18AA42EB-8C83-DF11-BE35-0017A477102C', 'Size': 1453164512L}}, 'Failed': {}}}

getPfnForProtocol = {'OK': True, 'Value': {'Successful': {'srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz': 'srm://srm-lhcb.gridpp.rl.ac.uk:8443/srm/managerv2?SFN=/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz'}, 'Failed': {}}}

class TransferAgentTests( unittest.TestCase ):
  """ test case for TransferAgent """

  def setUp( self ):
    self.tAgent = TransferAgent( AGENT_NAME )
    self.tAgent.transferDB = Mock( return_value = Mock( spec = TransferDB ) )

  def tearDown( self ):
    del self.tAgent

  def test_01_ctor( self ):
    self.assertEqual( isinstance( self.tAgent, TransferAgent ), True )

  def test_02_schedule( self ):
    ## sanitize getRequest
    self.tAgent.getRequest = Mock( return_value = getRequest( "replicateAndRegister") )
    ## sanitise TransferDB
    self.tAgent.transferDB = Mock( return_value = Mock( spec = TransferDB ) )
    self.tAgent.transferDB().getChannelQueues = Mock()
    self.tAgent.transferDB().getChannelQueues.return_value = getChannelQueues
    self.tAgent.transferDB().getChannelObservedThroughput = Mock()
    self.tAgent.transferDB().getChannelObservedThroughput.return_value = getChannelObservedThroughput
    self.tAgent.transferDB().addFileToChannel = Mock( return_value = SOK )
    self.tAgent.transferDB().addFileRegistration = Mock( return_value = SOK )
    self.tAgent.transferDB().addReplicationTree = Mock( return_value = SOK )
    ## sanitize RequestDBMySQL
    self.tAgent.requestDBMySQL = Mock( return_value = Mock(spec = RequestDBMySQL ) )
    self.tAgent.requestDBMySQL().updateRequest = Mock( return_value = SOK )
    ## sanitise ReplicaManager
    self.tAgent.replicaManager = Mock( return_value = Mock( spec = ReplicaManager ) )
    self.tAgent.replicaManager().getCatalogReplicas = Mock()
    self.tAgent.replicaManager().getCatalogReplicas.return_value = getCatalogReplicas   
    self.tAgent.replicaManager().getCatalogFileMetadata = Mock()
    self.tAgent.replicaManager().getCatalogFileMetadata.return_value = getCatalogFileMetadata
    self.tAgent.replicaManager().getPfnForProtocol = Mock()
    self.tAgent.replicaManager().getPfnForProtocol.return_value = getPfnForProtocol

    ## sanitise RequestClient
    self.tAgent.requestClient = Mock( return_value = Mock(spec=RequestClient) ) 
    self.tAgent.requestClient().updateRequest = Mock()
    self.tAgent.requestClient().updateRequest.return_value = SOK
    self.tAgent.requestClient().finalizeRequest = Mock()
    self.tAgent.requestClient().finalizeRequest.return_value = SOK
    ## sanitize ProcessPool
    self.tAgent.processPool = Mock( return_value = Mock( spec = ProcessPool) )
    self.tAgent.processPool().createAndQueueTask = Mock( return_value = SOK )
    self.tAgent.processPool().processResults = Mock( return_value = SOK )

    ret = self.tAgent.setupStrategyHandler()
    print ret

    ret = self.tAgent.schedule( getRequest("replicateAndRegister")["Value"] )
    print ret


    

    
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suiteTA = testLoader.loadTestsFromTestCase( TransferAgentTests )     
  suite = unittest.TestSuite( [ suiteTA ] )
  unittest.TextTestRunner(verbosity=3).run(suite)
