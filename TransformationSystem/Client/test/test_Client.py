import unittest

from mock import Mock
from DIRAC.TransformationSystem.Client.TaskManager            import TaskBase, WorkflowTasks, RequestTasks
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

def getSitesForSE( ses ):
  if ses == 'pippo':
    return {'OK':True, 'Value':['Site2', 'Site3']}
  else:
    return {'OK':True, 'Value':['Site3']}

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
    self.jobMock2 = Mock()
    mockWF = Mock()
    mockPar = Mock()
    mockWF.findParameter.return_value = mockPar
    mockPar.getValue.return_value = 'MySite'

    self.jobMock2.workflow = mockWF
    self.jobMock2.setDestination.return_value = {'OK':True}
    self.jobMock.workflow.return_value = ''
    self.jobMock.return_value = self.jobMock2

    self.taskBase = TaskBase( transClient = self.mockTransClient )
    self.wfTasks = WorkflowTasks( transClient = self.mockTransClient,
                                  submissionClient = self.WMSClientMock,
                                  jobMonitoringClient = self.jobMonitoringClient,
                                  outputDataModule = "mock",
                                  jobClass = self.jobMock )
    self.requestTasks = RequestTasks( transClient = self.mockTransClient,
                                      requestClient = self.mockRequestClient
                                      )

    self.tc = TransformationClient()

    self.maxDiff = None


  def tearDown( self ):
    pass

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

    res = self.wfTasks.prepareTransformationTasks( '', taskDict, 'test_user', 'test_group' )

    self.assertEqual( res, {'OK': True,
                           'Value': {1: {'a1': 'aa1', 'TaskObject': '', 'TransformationID': 1,
                                          'b1': 'bb1', 'Site': 'MySite'},
                                     2: {'TaskObject': '', 'a2': 'aa2', 'TransformationID': 1,
                                         'InputData': ['a1', 'a2'], 'b2': 'bb2', 'Site': 'MySite'},
                                     3: {'TaskObject': '', 'a3': 'aa3', 'TransformationID': 2,
                                         'b3': 'bb3', 'Site': 'MySite'}
                                     }
                            }
                    )

  def test__handleDestination( self ):
    res = self.wfTasks._handleDestination( {'Site':'', 'TargetSE':''} )
    self.assertEqual( res, ['ANY'] )
    res = self.wfTasks._handleDestination( {'Site':'ANY', 'TargetSE':''} )
    self.assertEqual( res, ['ANY'] )
    res = self.wfTasks._handleDestination( {'TargetSE':'Unknown'} )
    self.assertEqual( res, ['ANY'] )
    res = self.wfTasks._handleDestination( {'Site':'Site1;Site2', 'TargetSE':''} )
    self.assertEqual( res, ['Site1', 'Site2'] )
    res = self.wfTasks._handleDestination( {'Site':'Site1;Site2', 'TargetSE':'pippo'}, getSitesForSE )
    self.assertEqual( res, ['Site2'] )
    res = self.wfTasks._handleDestination( {'Site':'Site1;Site2', 'TargetSE':'pippo, pluto'}, getSitesForSE )
    self.assertEqual( res, ['Site2'] )
    res = self.wfTasks._handleDestination( {'Site':'Site1;Site2;Site3', 'TargetSE':'pippo, pluto'}, getSitesForSE )
    self.assertEqual( res, ['Site2', 'Site3'] )
    res = self.wfTasks._handleDestination( {'Site':'Site2', 'TargetSE':'pippo, pluto'}, getSitesForSE )
    self.assertEqual( res, ['Site2'] )
    res = self.wfTasks._handleDestination( {'Site':'ANY', 'TargetSE':'pippo, pluto'}, getSitesForSE )
    self.assertEqual( res, ['Site2', 'Site3'] )
    res = self.wfTasks._handleDestination( {'Site':'Site1', 'TargetSE':'pluto'}, getSitesForSE )
    self.assertEqual( res, [] )

#############################################################################

class TransformationClientSuccess( ClientsTestCase ):

  def test__applyProductionFilesStateMachine( self ):
    tsFiles = {}
    dictOfNewLFNsStatus = {}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {}
    dictOfNewLFNsStatus = {'foo':['status', 2L, 1234]}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'Removed'} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'status'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'status'} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar':['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'status'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'status'} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar': ['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A', 'bar':'B'} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A', 'bar':'Removed'} )

    tsFiles = {'foo': ['Assigned', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A', 'bar':'Removed'} )

    tsFiles = {'foo':['Assigned', 2L, 1234], 'bar':['Assigned', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'Processed', 'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['MaxReset', 12L, 1234], 'bar':['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'MaxReset', 'bar':'Processed'} )

    tsFiles = {'foo':['MaxReset', 12L, 1234], 'bar':['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Unused', 'bar':'Unused'} )

    tsFiles = {'foo':['Assigned', 20L, 1234], 'bar':['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'MaxReset', 'bar':'Processed'} )

    tsFiles = {'foo':['Assigned', 20L, 1234], 'bar':['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Unused', 'bar':'Unused'} )

#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskBaseSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( WorkflowTasksSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationClientSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
