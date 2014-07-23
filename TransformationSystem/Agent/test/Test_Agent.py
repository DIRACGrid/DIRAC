""" Test class for agents
"""

# imports
import unittest, importlib, datetime
from mock import MagicMock

from DIRAC import gLogger

#sut
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase import TaskManagerAgentBase
from DIRAC.TransformationSystem.Agent.TransformationAgent import TransformationAgent


class AgentsTestCase( unittest.TestCase ):
  """ Base class for the Agents test cases
  """
  def setUp( self ):
    self.mockAM = MagicMock()
    self.tmab_m = importlib.import_module( 'DIRAC.TransformationSystem.Agent.TaskManagerAgentBase' )
    self.tmab_m.AgentModule = self.mockAM
    self.tmab_m.FileReport = MagicMock()
    self.tmab = TaskManagerAgentBase()
    self.tmab.log = gLogger
    self.tmab.am_getOption = self.mockAM
    self.tmab.log.setLevel( 'DEBUG' )
    
    self.ta_m = importlib.import_module( 'DIRAC.TransformationSystem.Agent.TransformationAgent' )
    self.ta_m.AgentModule = self.mockAM
    self.ta = TransformationAgent()
    self.ta.log = gLogger
    self.ta.am_getOption = self.mockAM
    self.tmab.log.setLevel( 'DEBUG' )

    self.tc_mock = MagicMock()
    self.tm_mock = MagicMock()

  def tearDown( self ):
#     sys.modules.pop( 'DIRAC.Core.Base.AgentModule' )
#     sys.modules.pop( 'DIRAC.TransformationSystem.Agent.TransformationAgent' )
    pass

class TaskManagerAgentBaseSuccess(AgentsTestCase):

  def test__fillTheQueue( self ):
    operationsOnTransformationsDict = {1:{'Operations':['op1', 'op2'], 'Body':'veryBigBody'}}
    self.tmab._fillTheQueue( operationsOnTransformationsDict )
    self.assert_( self.tmab.transInQueue == [1] )
    self.assert_( self.tmab.transQueue.qsize() == 1 )

    operationsOnTransformationsDict = {2:{'Operations':['op3', 'op2'], 'Body':'veryveryBigBody'}}
    self.tmab._fillTheQueue( operationsOnTransformationsDict )
    self.assert_( self.tmab.transInQueue == [1, 2] )
    self.assert_( self.tmab.transQueue.qsize() == 2 )

    operationsOnTransformationsDict = {2:{'Operations':['op3', 'op2'], 'Body':'veryveryBigBody'}}
    self.tmab._fillTheQueue( operationsOnTransformationsDict )
    self.assert_( self.tmab.transInQueue == [1, 2] )
    self.assert_( self.tmab.transQueue.qsize() == 2 )
    
  def test_updateTaskStatusSuccess( self ):
    clients = {'TransformationClient':self.tc_mock, 'TaskManager':self.tm_mock}

    transIDOPBody = {1:{'Operations':['op1', 'op2'], 'Body':'veryBigBody'}}

    # errors getting
    self.tc_mock.getTransformationTasks.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.updateTaskStatus( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # no tasks
    self.tc_mock.getTransformationTasks.return_value = {'OK': True, 'Value': []}
    res = self.tmab.updateTaskStatus( transIDOPBody, clients )
    self.assert_( res['OK'] )

    # tasks, fail in update
    self.tc_mock.getTransformationTasks.return_value = {'OK': True,
                                                        'Value': [{'CreationTime': None,
                                                                   'ExternalID': '1',
                                                                   'ExternalStatus': 'Reserved',
                                                                   'LastUpdateTime': None,
                                                                   'RunNumber': 0L,
                                                                   'TargetSE': 'Unknown',
                                                                   'TaskID': 1L,
                                                                   'TransformationID': 101L},
                                                                  {'CreationTime': datetime.datetime( 2014, 7, 21, 14, 19, 3 ),
                                                                   'ExternalID': '0',
                                                                   'ExternalStatus': 'Reserved',
                                                                   'LastUpdateTime': datetime.datetime( 2014, 7, 21, 14, 19, 3 ),
                                                                   'RunNumber': 0L,
                                                                   'TargetSE': 'Unknown',
                                                                   'TaskID': 2L,
                                                                   'TransformationID': 101L}]}

    self.tm_mock.getSubmittedTaskStatus.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.updateTaskStatus( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # tasks, nothing to update
    self.tc_mock.getTransformationTasks.return_value = {'OK': True,
                                                        'Value': [{'CreationTime': None,
                                                                   'ExternalID': '1',
                                                                   'ExternalStatus': 'Reserved',
                                                                   'LastUpdateTime': None,
                                                                   'RunNumber': 0L,
                                                                   'TargetSE': 'Unknown',
                                                                   'TaskID': 1L,
                                                                   'TransformationID': 101L},
                                                                  {'CreationTime': datetime.datetime( 2014, 7, 21, 14, 19, 3 ),
                                                                   'ExternalID': '0',
                                                                   'ExternalStatus': 'Reserved',
                                                                   'LastUpdateTime': datetime.datetime( 2014, 7, 21, 14, 19, 3 ),
                                                                   'RunNumber': 0L,
                                                                   'TargetSE': 'Unknown',
                                                                   'TaskID': 2L,
                                                                   'TransformationID': 101L}]}

    self.tm_mock.getSubmittedTaskStatus.return_value = {'OK': True, 'Value': {}}
    res = self.tmab.updateTaskStatus( transIDOPBody, clients )
    self.assert_( res['OK'] )

    # tasks, to update, no errors
    self.tc_mock.getTransformationTasks.return_value = {'OK': True,
                                                        'Value': [{'CreationTime': None,
                                                                   'ExternalID': '1',
                                                                   'ExternalStatus': 'Reserved',
                                                                   'LastUpdateTime': None,
                                                                   'RunNumber': 0L,
                                                                   'TargetSE': 'Unknown',
                                                                   'TaskID': 1L,
                                                                   'TransformationID': 101L},
                                                                  {'CreationTime': datetime.datetime( 2014, 7, 21, 14, 19, 3 ),
                                                                   'ExternalID': '0',
                                                                   'ExternalStatus': 'Reserved',
                                                                   'LastUpdateTime': datetime.datetime( 2014, 7, 21, 14, 19, 3 ),
                                                                   'RunNumber': 0L,
                                                                   'TargetSE': 'Unknown',
                                                                   'TaskID': 2L,
                                                                   'TransformationID': 101L}]}

    self.tm_mock.getSubmittedTaskStatus.return_value = {'OK': True, 'Value': {'Running': [1, 2], 'Done': [3]}}
    res = self.tmab.updateTaskStatus( transIDOPBody, clients )
    self.assert_( res['OK'] )


  def test_updateFileStatusSuccess( self ):
    clients = {'TransformationClient':self.tc_mock, 'TaskManager':self.tm_mock}

    transIDOPBody = {1:{'Operations':['op1', 'op2'], 'Body':'veryBigBody'}}

    # errors getting
    self.tc_mock.getTransformationFiles.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.updateFileStatus( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # no files
    self.tc_mock.getTransformationFiles.return_value = {'OK': True, 'Value': []}
    res = self.tmab.updateFileStatus( transIDOPBody, clients )
    self.assert_( res['OK'] )

    # files, failing to update
    self.tc_mock.getTransformationFiles.return_value = {'OK': True, 'Value': [{'file1': 'boh'}]}
    self.tm_mock.getSubmittedFileStatus.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.updateFileStatus( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # files, nothing to update
    self.tc_mock.getTransformationFiles.return_value = {'OK': True, 'Value': [{'file1': 'boh'}]}
    self.tm_mock.getSubmittedFileStatus.return_value = {'OK': True, 'Value': []}
    res = self.tmab.updateFileStatus( transIDOPBody, clients )
    self.assert_( res['OK'] )

    # files, something to update
    self.tc_mock.getTransformationFiles.return_value = {'OK': True, 'Value': [{'file1': 'boh'}]}
    self.tm_mock.getSubmittedFileStatus.return_value = {'OK': True, 'Value': {'file1': 'OK', 'file2': 'NOK'}}
    res = self.tmab.updateFileStatus( transIDOPBody, clients )
    self.assert_( res['OK'] )

  def test_checkReservedTasks( self ):
    clients = {'TransformationClient':self.tc_mock, 'TaskManager':self.tm_mock}

    transIDOPBody = {1:{'Operations':['op1', 'op2'], 'Body':'veryBigBody'}}

    # errors getting
    self.tc_mock.getTransformationTasks.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.checkReservedTasks( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # no tasks
    self.tc_mock.getTransformationTasks.return_value = {'OK': True, 'Value': []}
    res = self.tmab.checkReservedTasks( transIDOPBody, clients )
    self.assert_( res['OK'] )

    # tasks, failing to update
    self.tc_mock.getTransformationTasks.return_value = {'OK': True,
                                                        'Value': [{'CreationTime': None,
                                                                   'ExternalID': '1',
                                                                   'ExternalStatus': 'Reserved',
                                                                   'LastUpdateTime': None,
                                                                   'RunNumber': 0L,
                                                                   'TargetSE': 'Unknown',
                                                                   'TaskID': 1L,
                                                                   'TransformationID': 101L},
                                                                  {'CreationTime': datetime.datetime( 2014, 7, 21, 14, 19, 3 ),
                                                                   'ExternalID': '0',
                                                                   'ExternalStatus': 'Reserved',
                                                                   'LastUpdateTime': datetime.datetime( 2014, 7, 21, 14, 19, 3 ),
                                                                   'RunNumber': 0L,
                                                                   'TargetSE': 'Unknown',
                                                                   'TaskID': 2L,
                                                                   'TransformationID': 101L}]}
    self.tm_mock.updateTransformationReservedTasks.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.checkReservedTasks( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # tasks, something to update, fail
    self.tc_mock.setTaskStatusAndWmsID.return_value = {'OK': False, 'Message': 'a mess'}
    self.tm_mock.updateTransformationReservedTasks.return_value = {'OK': True, 'Value': {'NoTasks': [],
                                                                                         'TaskNameIDs': {'1_1':123, '2_1':456}}}
    res = self.tmab.checkReservedTasks( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # tasks, something to update, no fail
    self.tc_mock.setTaskStatusAndWmsID.return_value = {'OK': True}
    self.tm_mock.updateTransformationReservedTasks.return_value = {'OK': True, 'Value': {'NoTasks': ['3_4', '5_6'],
                                                                                         'TaskNameIDs': {'1_1':123, '2_1':456}}}
    res = self.tmab.checkReservedTasks( transIDOPBody, clients )
    self.assert_( res['OK'] )



  def test_submitTasks( self ):
    clients = {'TransformationClient':self.tc_mock, 'TaskManager':self.tm_mock}

    transIDOPBody = {1:{'Operations':['op1', 'op2'], 'Body':'veryBigBody'}}

    # errors getting
    self.tc_mock.getTasksToSubmit.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.submitTasks( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # no tasks
    self.tc_mock.getTasksToSubmit.return_value = {'OK': True, 'Value': {'JobDictionary': {}}}
    res = self.tmab.submitTasks( transIDOPBody, clients )
    self.assert_( res['OK'] )

    # tasks, errors
    self.tc_mock.getTasksToSubmit.return_value = {'OK': True, 'Value': {'JobDictionary': {123: 'foo', 456: 'bar'}}}
    self.tm_mock.prepareTransformationTasks.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.submitTasks( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # tasks, still errors
    self.tc_mock.getTasksToSubmit.return_value = {'OK': True, 'Value': {'JobDictionary': {123: 'foo', 456: 'bar'}}}
    self.tm_mock.prepareTransformationTasks.return_value = {'OK': True, 'Value': {123: 'foo', 456: 'bar'}}
    self.tm_mock.submitTransformationTasks.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.submitTasks( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # tasks, still errors
    self.tc_mock.getTasksToSubmit.return_value = {'OK': True, 'Value': {'JobDictionary': {123: 'foo', 456: 'bar'}}}
    self.tm_mock.prepareTransformationTasks.return_value = {'OK': True, 'Value': {123: 'foo', 456: 'bar'}}
    self.tm_mock.submitTransformationTasks.return_value = {'OK': True, 'Value': [] }
    self.tm_mock.updateDBAfterTaskSubmission.return_value = {'OK': False, 'Message': 'a mess'}
    res = self.tmab.submitTasks( transIDOPBody, clients )
    self.assertFalse( res['OK'] )

    # tasks, no errors
    self.tc_mock.getTasksToSubmit.return_value = {'OK': True, 'Value': {'JobDictionary': {123: 'foo', 456: 'bar'}}}
    self.tm_mock.prepareTransformationTasks.return_value = {'OK': True, 'Value': {123: 'foo', 456: 'bar'}}
    self.tm_mock.submitTransformationTasks.return_value = {'OK': True, 'Value': [] }
    self.tm_mock.updateDBAfterTaskSubmission.return_value = {'OK': True, 'Value': [] }
    res = self.tmab.submitTasks( transIDOPBody, clients )
    self.assert_( res['OK'] )



class TransformationAgentSuccess( AgentsTestCase ):

  def test__getTransformationFiles( self ):
    goodFiles = {'OK':True,
                 'Value':[{'ErrorCount': 1L,
                           'FileID': 17990660L,
                           'InsertedTime': datetime.datetime( 2012, 3, 15, 17, 5, 50 ),
                           'LFN': '/00012574_00000239_1.charmcompleteevent.dst',
                           'LastUpdate': datetime.datetime( 2012, 3, 16, 23, 43, 26 ),
                           'RunNumber': 90269L,
                           'Status': 'Unused',
                           'TargetSE': 'Unknown',
                           'TaskID': '222',
                           'TransformationID': 17042L,
                           'UsedSE': 'CERN-DST,IN2P3_M-DST,PIC-DST,RAL-DST'},
                          {'ErrorCount': 1L,
                           'FileID': 17022945L,
                           'InsertedTime': datetime.datetime( 2012, 3, 15, 17, 5, 50 ),
                           'LFN': '/00012574_00000119_1.charmcompleteevent.dst',
                           'LastUpdate': datetime.datetime( 2012, 3, 16, 23, 54, 59 ),
                           'RunNumber': 90322L,
                           'Status': 'Unused',
                           'TargetSE': 'Unknown',
                           'TaskID': '82',
                           'TransformationID': 17042L,
                           'UsedSE': 'CERN-DST,CNAF-DST,RAL_M-DST,SARA-DST'}]
                  }
    noFiles = {'OK':True, 'Value':[]}

    for getTFiles in [goodFiles, noFiles]:
      self.tc_mock.getTransformationFiles.return_value = getTFiles

      transDict = {'TransformationID': 123, 'Status': 'Stopped', 'Type': 'Replication' }
      res = self.ta._getTransformationFiles( transDict, {'TransformationClient': self.tc_mock} )
      self.assertTrue( res['OK'] )

      transDict = {'TransformationID': 123, 'Status': 'Stopped', 'Type': 'Removal' }
      res = self.ta._getTransformationFiles( transDict, {'TransformationClient': self.tc_mock} )
      self.assertTrue( res['OK'] )






#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AgentsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskManagerAgentBaseSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationAgentSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#











