""" test
"""

import unittest
import types
import importlib
import itertools

from mock import MagicMock
from DIRAC import S_OK, gLogger

from DIRAC.RequestManagementSystem.Client.Request             import Request
from DIRAC.TransformationSystem.Client.TaskManager            import TaskBase, WorkflowTasks, RequestTasks
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.TransformationSystem.Client.Transformation         import Transformation
from DIRAC.TransformationSystem.Client.TaskManagerPlugin      import TaskManagerPlugin
from DIRAC.TransformationSystem.Client.Utilities              import PluginUtilities, getFileGroups

# Fake classes
class opsHelperFakeUser( object ):
  def getValue( self, foo = '', bar = '' ):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    return ['PAK', 'Ferrara', 'Bologna', 'Paris']
  def getOptionsDict( self, foo = '' ):
    return {'OK': True, 'Value': {'Paris': 'IN2P3'}}

class opsHelperFakeUser2( object ):
  def getValue( self, foo = '', bar = '' ):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return []
    return ['PAK', 'Ferrara', 'Bologna', 'Paris', 'CERN', 'IN2P3']
  def getOptionsDict( self, foo = '' ):
    return {'OK': True, 'Value': {'Paris': 'IN2P3', 'CERN': 'CERN', 'IN2P3':'IN2P3'}}

class opsHelperFakeDataReco( object ):
  def getValue( self, foo = '', bar = '' ):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    return ['PAK', 'Ferrara', 'CERN', 'IN2P3']
  def getOptionsDict(self, foo = ''):
    return {'OK': True, 'Value': {'Ferrara': 'CERN', 'IN2P3': 'IN2P3, CERN'}}

class opsHelperFakeMerge( object ):
  def getValue( self, foo = '', bar = '' ):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    return ['ALL']
  def getOptionsDict( self, foo = '' ):
    return {'OK': False, 'Message': 'JobTypeMapping/MCSimulation/Allow in Operations does not exist'}

class opsHelperFakeMerge2( object ):
  def getValue( self, foo = '', bar = '' ):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return []
    return ['ALL']
  def getOptionsDict( self, foo = '' ):
    return {'OK': True, 'Value': {'CERN': 'CERN', 'IN2P3': 'IN2P3'}}

class opsHelperFakeMerge3( object ):
  def getValue( self, foo = '', bar = '' ):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    return ['ALL']
  def getOptionsDict( self, foo = '' ):
    return {'OK': True, 'Value': {'Paris': 'CERN'}}

class opsHelperFakeMC( object ):
  def getValue( self, foo = '', bar = '' ):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    return []
  def getOptionsDict( self, foo = '' ):
    return {'OK': False, 'Message': 'JobTypeMapping/MCSimulation/Allow in Operations does not exist'}


def getSitesFake():
  return S_OK( ['Ferrara', 'Bologna', 'Paris', 'CSCS', 'PAK', 'CERN', 'IN2P3'] )

def getSitesForSE( ses ):
  if ses == ['CERN-DST'] or ses == 'CERN-DST':
    return S_OK( ['CERN'] )
  elif ses == ['IN2P3-DST'] or ses == 'IN2P3-DST':
    return S_OK( ['IN2P3'] )
  elif ses == ['CSCS-DST'] or ses == 'CSCS-DST':
    return S_OK( ['CSCS'] )
  elif ses == ['CERN-DST', 'CSCS-DST'] or ses == 'CERN-DST,CSCS-DST':
    return S_OK( ['CERN', 'CSCS'] )



# Test data for plugins
data = {'/this/is/at_1':['SE1'],
        '/this/is/at_2':['SE2'],
        '/this/is/at_12':['SE1', 'SE2'],
        '/this/is/also/at_12':['SE1', 'SE2'],
        '/this/is/at_123':['SE1', 'SE2', 'SE3'],
        '/this/is/at_23':['SE2', 'SE3'],
        '/this/is/at_4':['SE4']}

cachedLFNSize = {'/this/is/at_1':1,
                 '/this/is/at_2':2,
                 '/this/is/at_12':12,
                 '/this/is/also/at_12':12,
                 '/this/is/at_123':123,
                 '/this/is/at_23':23,
                 '/this/is/at_4':4}

#############################################################################

class ClientsTestCase( unittest.TestCase ):
  """ Base class for the clients test cases
  """
  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )

    self.mockTransClient = MagicMock()
    self.mockTransClient.setTaskStatusAndWmsID.return_value = {'OK':True}

    self.WMSClientMock = MagicMock()
    self.jobMonitoringClient = MagicMock()
    self.mockReqClient = MagicMock()

    self.jobMock = MagicMock()
    self.jobMock2 = MagicMock()
    mockWF = MagicMock()
    mockPar = MagicMock()
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
                                      requestClient = self.mockReqClient,
                                      requestValidator = MagicMock() )
    self.tc = TransformationClient()
    self.transformation = Transformation()

    self.fcMock = MagicMock()
    self.fcMock.getFileSize.return_value = S_OK( {'Failed':[], 'Successful': cachedLFNSize} )

    gLogger.setLevel( 'DEBUG' )
    
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
                3:{'TransformationID':2, 'a3':'aa3', 'b3':'bb3'}}

    res = self.wfTasks.prepareTransformationTasks( '', taskDict, 'test_user', 'test_group', 'test_DN' )

    self.assertEqual( res, {'OK': True,
                           'Value': {1: {'a1': 'aa1', 'TaskObject': '', 'TransformationID': 1,
                                          'b1': 'bb1', 'Site': 'MySite', 'JobType': 'MySite'},
                                     2: {'TaskObject': '', 'a2': 'aa2', 'TransformationID': 1,
                                         'InputData': ['a1', 'a2'], 'b2': 'bb2', 'Site': 'MySite', 'JobType': 'MySite'},
                                     3: {'TaskObject': '', 'a3': 'aa3', 'TransformationID': 2,
                                         'b3': 'bb3', 'Site': 'MySite', 'JobType': 'MySite'}}} )

  def test__handleDestination(self):
    destPluginMock = MagicMock()

    # nothing out of the plugin
    destPluginMock.run.return_value = set( [] )
    self.wfTasks.destinationPlugin_o = destPluginMock

    paramsDict = {'Site':'', 'TargetSE':''}
    res = self.wfTasks._handleDestination( paramsDict )
    self.assertEqual( res, ['ANY'] )

    paramsDict = {'Site':'ANY', 'TargetSE':''}
    res = self.wfTasks._handleDestination( paramsDict )
    self.assertEqual( res, ['ANY'] )

    paramsDict = {'Site':'PIPPO', 'TargetSE':''}
    res = self.wfTasks._handleDestination( paramsDict )
    self.assertEqual( res, ['PIPPO'] )

    paramsDict = {'Site':'PIPPO;PLUTO', 'TargetSE':''}
    res = self.wfTasks._handleDestination( paramsDict )
    self.assertEqual( res, ['PIPPO', 'PLUTO'] )

    # something out of the plugin
    destPluginMock.run.return_value = set( ['Site1', 'PIPPO'] )
    self.wfTasks.destinationPlugin_o = destPluginMock

    paramsDict = {'Site':'', 'TargetSE':''}
    res = self.wfTasks._handleDestination( paramsDict )
    self.assertEqual( sorted( res ), sorted( ['Site1', 'PIPPO'] ) )

    paramsDict = {'Site':'ANY', 'TargetSE':''}
    res = self.wfTasks._handleDestination( paramsDict )
    self.assertEqual( sorted( res ), sorted( ['Site1', 'PIPPO'] ) )

    paramsDict = {'Site':'PIPPO', 'TargetSE':''}
    res = self.wfTasks._handleDestination( paramsDict )
    self.assertEqual( res, ['PIPPO'] )

    paramsDict = {'Site':'PIPPO;PLUTO', 'TargetSE':''}
    res = self.wfTasks._handleDestination( paramsDict )
    self.assertEqual( res, ['PIPPO'] )

  def test_submitTransformationTasks( self ):
    taskDict = {}
    res = self.wfTasks.submitTransformationTasks( taskDict )
    self.assertEqual( res['OK'], True, res['Message'] if 'Message' in res else 'OK' )


#############################################################################

class TaskManagerPluginSuccess(ClientsTestCase):

  def test__BySE( self ):

    ourPG = importlib.import_module( 'DIRAC.TransformationSystem.Client.TaskManagerPlugin' )
    ourPG.getSitesForSE = getSitesForSE
    p_o = TaskManagerPlugin( 'BySE', operationsHelper = MagicMock() )

    p_o.params = {'Site':'', 'TargetSE':''}
    res = p_o.run()
    self.assertEqual( res, set( [] ) )

    p_o.params = {'Site':'ANY', 'TargetSE':''}
    res = p_o.run()
    self.assertEqual( res, set( [] ) )

    p_o.params = {'TargetSE':'Unknown'}
    res = p_o.run()
    self.assertEqual( res, set( [] ) )

    p_o.params = {'Site':'Site1;Site2', 'TargetSE':''}
    res = p_o.run()
    self.assertEqual( res, set( [] ) )

    p_o.params = {'TargetSE':'CERN-DST'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN'] ) )

    p_o.params = {'TargetSE':'IN2P3-DST'}
    res = p_o.run()
    self.assertEqual( res, set( ['IN2P3'] ) )

    p_o.params = {'TargetSE':'CERN-DST,CSCS-DST'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN', 'CSCS'] ) )

    p_o.params = {'TargetSE':['CERN-DST', 'CSCS-DST']}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN', 'CSCS'] ) )


  def test__ByJobType( self ):

    ourPG = importlib.import_module( 'DIRAC.TransformationSystem.Client.TaskManagerPlugin' )
    ourPG.getSites = getSitesFake
    ourPG.getSitesForSE = getSitesForSE

    # "User" case - 1
    p_o = TaskManagerPlugin( 'ByJobType', operationsHelper = opsHelperFakeUser() )

    p_o.params = {'Site':'', 'TargetSE':'CERN-DST', 'JobType':'User'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN', 'CSCS'] ) )

    p_o.params = {'Site':'', 'TargetSE':'IN2P3-DST', 'JobType':'User'}
    res = p_o.run()
    self.assertEqual( res, set( ['IN2P3', 'Paris', 'CSCS'] ) )

    p_o.params = {'Site':'', 'TargetSE':['CERN-DST', 'CSCS-DST'], 'JobType':'User'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN', 'CSCS'] ) )

    # "User" case - 2
    p_o = TaskManagerPlugin( 'ByJobType', operationsHelper = opsHelperFakeUser2() )

    p_o.params = {'Site':'', 'TargetSE':'CERN-DST', 'JobType':'User'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN', 'CSCS'] ) )

    p_o.params = {'Site':'', 'TargetSE':'IN2P3-DST', 'JobType':'User'}
    res = p_o.run()
    self.assertEqual( res, set( ['IN2P3', 'Paris', 'CSCS'] ) )

    p_o.params = {'Site':'', 'TargetSE':['CERN-DST', 'CSCS-DST'], 'JobType':'User'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN', 'CSCS'] ) )

    # "DataReconstruction" case
    p_o = TaskManagerPlugin( 'ByJobType', operationsHelper = opsHelperFakeDataReco() )

    p_o.params = {'Site':'', 'TargetSE':'CERN-DST', 'JobType':'DataReconstruction'}
    res = p_o.run()
    self.assertEqual( res, set( ['Bologna', 'Ferrara', 'Paris', 'CSCS', 'CERN', 'IN2P3'] ) )

    p_o.params = {'Site':'', 'TargetSE':'IN2P3-DST', 'JobType':'DataReconstruction'}
    res = p_o.run()
    self.assertEqual( res, set( ['Bologna', 'Paris', 'CSCS', 'IN2P3'] ) )


    # "Merge" case - 1
    p_o = TaskManagerPlugin( 'ByJobType', operationsHelper = opsHelperFakeMerge() )

    p_o.params = {'Site':'', 'TargetSE':'CERN-DST', 'JobType':'Merge'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN'] ) )

    p_o.params = {'Site':'', 'TargetSE':'IN2P3-DST', 'JobType':'Merge'}
    res = p_o.run()
    self.assertEqual( res, set( ['IN2P3'] ) )

    # "Merge" case - 2
    p_o = TaskManagerPlugin( 'ByJobType', operationsHelper = opsHelperFakeMerge2() )

    p_o.params = {'Site':'', 'TargetSE':'CERN-DST', 'JobType':'Merge'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN'] ) )

    p_o.params = {'Site':'', 'TargetSE':'IN2P3-DST', 'JobType':'Merge'}
    res = p_o.run()
    self.assertEqual( res, set( ['IN2P3'] ) )

    # "Merge" case - 3
    p_o = TaskManagerPlugin( 'ByJobType', operationsHelper = opsHelperFakeMerge3() )

    p_o.params = {'Site':'', 'TargetSE':'CERN-DST', 'JobType':'Merge'}
    res = p_o.run()
    self.assertEqual( res, set( ['Paris', 'CERN'] ) )

    p_o.params = {'Site':'', 'TargetSE':'IN2P3-DST', 'JobType':'Merge'}
    res = p_o.run()
    self.assertEqual( res, set( ['IN2P3'] ) )

    # "MC" case
    p_o = TaskManagerPlugin( 'ByJobType', operationsHelper = opsHelperFakeMC() )

    p_o.params = {'Site':'', 'TargetSE':'', 'JobType':'MC'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN', 'IN2P3', 'Bologna', 'Ferrara', 'Paris', 'CSCS', 'PAK'] ) )

    p_o.params = {'Site':'', 'TargetSE':'', 'JobType':'MC'}
    res = p_o.run()
    self.assertEqual( res, set( ['CERN', 'IN2P3', 'Bologna', 'Ferrara', 'Paris', 'CSCS', 'PAK'] ) )


#############################################################################

class RequestTasksSuccess( ClientsTestCase ):

  def test_prepareTranformationTasks( self ):
    taskDict = {1:{'TransformationID':1, 'TargetSE':'SE1', 'b1':'bb1', 'Site':'MySite',
                   'InputData':['/this/is/a1.lfn', '/this/is/a2.lfn']},
                2:{'TransformationID':1, 'TargetSE':'SE2', 'b2':'bb2', 'InputData':"/this/is/a1.lfn;/this/is/a2.lfn"},
                3:{'TransformationID':2, 'TargetSE':'SE3', 'b3':'bb3', 'InputData':''}
                }

    res = self.requestTasks.prepareTransformationTasks( '', taskDict, 'owner', 'ownerGroup', '/bih/boh/DN' )

    self.assert_( res['OK'] )
    for task in res['Value'].values():
      self.assert_( isinstance( task['TaskObject'], Request ) )
      self.assertEqual( task['TaskObject'][0].Type, 'ReplicateAndRegister' )
      self.assertEqual( task['TaskObject'][0][0].LFN, '/this/is/a1.lfn' )
      self.assertEqual( task['TaskObject'][0][1].LFN, '/this/is/a2.lfn' )

#############################################################################


class TransformationClientSuccess( ClientsTestCase ):

  def test__applyTransformationFilesStateMachine( self ):
    tsFiles = {}
    dictOfNewLFNsStatus = {}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {}
    dictOfNewLFNsStatus = {'foo':['status', 2L, 1234]}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'status'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'statusA'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'statusA'} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar':['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'status'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar':['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'statusA'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'statusA'} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar': ['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A', 'bar':'B'} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A'} )

    tsFiles = {'foo': ['Assigned', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A'} )

    tsFiles = {'foo':['Assigned', 2L, 1234], 'bar':['Assigned', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'Processed', 'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['MaxReset', 12L, 1234], 'bar':['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'MaxReset', 'bar':'Processed'} )

    tsFiles = {'foo':['MaxReset', 12L, 1234], 'bar':['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Unused', 'bar':'Unused'} )

    tsFiles = {'foo':['Assigned', 20L, 1234], 'bar':['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'MaxReset', 'bar':'Processed'} )

    tsFiles = {'foo':['Assigned', 20L, 1234], 'bar':['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Unused', 'bar':'Unused'} )

#############################################################################


class TransformationSuccess( ClientsTestCase ):

  def test_setGet( self ):

    res = self.transformation.setTransformationName( 'TestTName' )
    self.assert_( res['OK'] )
    description = 'Test transformation description'
    res = self.transformation.setDescription( description )
    longDescription = 'Test transformation long description'
    res = self.transformation.setLongDescription( longDescription )
    self.assert_( res['OK'] )
    res = self.transformation.setType( 'MCSimulation' )
    self.assert_( res['OK'] )
    res = self.transformation.setPlugin( 'aPlugin' )
    self.assertTrue( res['OK'] )

  def test_SetGetReset( self ):
    """ Testing of the set, get and reset methods.

          set*()
          get*()
          setTargetSE()
          setSourceSE()
          getTargetSE()
          getSourceSE()
          reset()
        Ensures that after a reset all parameters are returned to their defaults
    """

    res = self.transformation.getParameters()
    self.assert_( res['OK'] )
    defaultParams = res['Value'].copy()
    for parameterName, defaultValue in res['Value'].items():
      if type( defaultValue ) in types.StringTypes:
        testValue = 'TestValue'
      else:
        testValue = 99999
      # # set*

      setterName = 'set%s' % parameterName
      self.assert_( hasattr( self.transformation, setterName ) )
      setter = getattr( self.transformation, setterName )
      self.assert_( callable( setter ) )
      res = setter( testValue )
      self.assert_( res['OK'] )
      # # get*
      getterName = "get%s" % parameterName
      self.assert_( hasattr( self.transformation, getterName ) )
      getter = getattr( self.transformation, getterName )
      self.assert_( callable( getter ) )
      res = getter()
      self.assert_( res['OK'] )
      self.assert_( res['Value'], testValue )

    res = self.transformation.reset()
    self.assert_( res['OK'] )
    res = self.transformation.getParameters()
    self.assert_( res['OK'] )
    for parameterName, resetValue in res['Value'].items():
      self.assertEqual( resetValue, defaultParams[parameterName] )
    self.assertRaises( AttributeError, self.transformation.getTargetSE )
    self.assertRaises( AttributeError, self.transformation.getSourceSE )

class PluginsUtilitiesSuccess( ClientsTestCase ):

  def test_getFileGroups( self ):

    res = getFileGroups( data )
    resExpected = {'SE1':['/this/is/at_1'],
                   'SE2':['/this/is/at_2'],
                   'SE1,SE2':sorted( ['/this/is/at_12', '/this/is/also/at_12'] ),
                   'SE1,SE2,SE3':['/this/is/at_123'],
                   'SE2,SE3':['/this/is/at_23'],
                   'SE4':['/this/is/at_4']}
    for t, tExp in itertools.izip( res.items(), resExpected.items() ):
      self.assertEqual( t[0], tExp[0] )
      self.assertEqual( sorted( t[1] ), tExp[1] )

    res = getFileGroups( data, False )
    resExpected = {'SE1': sorted( ['/this/is/at_1', '/this/is/at_123', '/this/is/at_12', '/this/is/also/at_12'] ),
                   'SE2': sorted( ['/this/is/at_23', 'this/is/at_2', '/this/is/at_123', '/this/is/at_12', '/this/is/also/at_12'] ),
                   'SE3': sorted( ['/this/is/at_23', '/this/is/at_123'] ),
                   'SE4': sorted( ['/this/is/at_4'] )}

    self.assertItemsEqual( res, resExpected )
    
  def test_groupByReplicas( self ):
    
    pu = PluginUtilities()
    res = pu.groupByReplicas( data, 'Active' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    pu = PluginUtilities()
    pu.params['GroupSize'] = 2
    res = pu.groupByReplicas( data, 'Active' )
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 3 )
    for t in res['Value']:
      self.assert_( len( t[1] ) <= 2 )

    pu = PluginUtilities()
    pu.params['GroupSize'] = 2
    res = pu.groupByReplicas( data, 'Flush' )
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 4 )

    pu = PluginUtilities()
    res = pu.groupByReplicas( data, 'Flush' )
    self.assert_( res['OK'] )
    resExpected = [( 'SE1', sorted( ['/this/is/also/at_12', '/this/is/at_1', '/this/is/at_123', '/this/is/at_12'] ) ),
                   ( 'SE2', sorted( ['/this/is/at_23', '/this/is/at_2'] ) ),
                   ( 'SE4', sorted( ['/this/is/at_4'] ) )]
    for t, tExp in itertools.izip( res['Value'], resExpected ):
      self.assertEqual( t[0], tExp[0] )
      self.assertEqual( sorted( t[1] ), tExp[1] )

  def test_groupBySize( self ):

    # no files, nothing happens
    pu = PluginUtilities( fc = self.fcMock )
    res = pu.groupBySize( {}, 'Active' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # files, cached, nothing happens as too small
    pu = PluginUtilities( fc = self.fcMock )
    pu.cachedLFNSize = cachedLFNSize
    res = pu.groupBySize( data, 'Active' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # files, cached, low GroupSize imposed
    pu = PluginUtilities( fc = self.fcMock )
    pu.cachedLFNSize = cachedLFNSize
    pu.groupSize = 10
    res = pu.groupBySize( data, 'Active' )
    self.assert_( res['OK'] )
    resExpected = [( 'SE1,SE2', ['/this/is/at_12'] ),
                   ( 'SE2,SE3', ['/this/is/at_23'] ),
                   ( 'SE1,SE2,SE3', ['/this/is/at_123'] ),
                   ( 'SE1,SE2', ['/this/is/also/at_12'] )]
    for tExp in resExpected:
      self.assert_( tExp in res['Value'] )

    # files, cached, flushed
    pu = PluginUtilities( fc = self.fcMock )
    pu.cachedLFNSize = cachedLFNSize
    res = pu.groupBySize( data, 'Flush' )
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 6 )

    # files, not cached, nothing happens as too small
    pu = PluginUtilities( fc = self.fcMock )
    res = pu.groupBySize( data, 'Active' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # files, not cached, flushed
    pu = PluginUtilities( fc = self.fcMock )

    res = pu.groupBySize( data, 'Flush' )
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 6 )

    # files, not cached, low GroupSize imposed
    pu = PluginUtilities( fc = self.fcMock )
    pu.groupSize = 10
    res = pu.groupBySize( data, 'Active' )
    self.assert_( res['OK'] )
    self.assert_( res['OK'] )
    resExpected = [( 'SE1,SE2', ['/this/is/at_12'] ),
                   ( 'SE2,SE3', ['/this/is/at_23'] ),
                   ( 'SE1,SE2,SE3', ['/this/is/at_123'] ),
                   ( 'SE1,SE2', ['/this/is/also/at_12'] )]
    for tExp in resExpected:
      self.assert_( tExp in res['Value'] )

    # files, not cached, low GroupSize imposed, Flushed
    pu = PluginUtilities( fc = self.fcMock )
    pu.groupSize = 10
    res = pu.groupBySize( data, 'Flush' )
    self.assert_( res['OK'] )
    self.assert_( res['OK'] )
    resExpected = [( 'SE1,SE2', ['/this/is/at_12'] ),
                   ( 'SE2,SE3', ['/this/is/at_23'] ),
                   ( 'SE1,SE2,SE3', ['/this/is/at_123'] ),
                   ( 'SE1,SE2', ['/this/is/also/at_12'] )]
    for tExp in resExpected:
      self.assert_( tExp in res['Value'] )



#############################################################################
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskBaseSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( WorkflowTasksSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RequestTasksSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationClientSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( PluginsUtilitiesSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
