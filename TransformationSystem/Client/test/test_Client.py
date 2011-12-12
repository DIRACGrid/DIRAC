import unittest

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from mock import Mock
from DIRAC.TransformationSystem.Client.TaskManager import TaskBase, WorkflowTasks, RequestTasks

#############################################################################

class ClientsTestCase( unittest.TestCase ):
  """ Base class for the clients test cases
  """
  def setUp( self ):

    self.mockTransClient = Mock()
    self.mockTransClient.setTaskStatusAndWmsID.return_value = {'OK':True}

    self.WMSClientMock = Mock()
    self.jobMonitoringClient = Mock()
    self.mockRequestClient = Mock()

    self.jobMock = Mock()

    self.taskBase = TaskBase( transClient = self.mockTransClient )
    self.wfTasks = WorkflowTasks( transClient = self.mockTransClient,
                                  submissionClient = self.WMSClientMock,
                                  jobMonitoringClient = self.jobMonitoringClient,
                                  outputDataModule = "mock" )
    self.requestTasks = RequestTasks( transClient = self.mockTransClient,
                                      requestClient = self.mockRequestClient
                                      )

#############################################################################

class TaskBaseSuccess( ClientsTestCase ):

  def test_updateDBAfterTaskSubmission( self ):
    res = self.taskBase.updateDBAfterTaskSubmission( {} )
    self.assertEqual( res['OK'], True )

#############################################################################

class WorkflowTasksSuccess( ClientsTestCase ):

  def test_prepareTranformationTasks( self ):
    taskDict = {1:{'TransformationID':1, 'a1':'aa1', 'b1':'bb1', 'Site':'MySite'},
                2:{'TransformationID':1, 'a2':'aa2', 'b2':'bb2', 'InputData':['a1', 'a2']},
                3:{'TransformationID':2, 'a3':'aa3', 'b3':'bb3'},
                }
    res = self.wfTasks.prepareTransformationTasks( Mock(), taskDict, 'test_user', 'test_group', self.jobMock )

    print res

#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskBaseSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( WorkflowTasksSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
