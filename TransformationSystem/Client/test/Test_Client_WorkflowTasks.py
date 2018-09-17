""" pytest for WorkflowTasks
"""

# pylint: disable=protected-access,missing-docstring,invalid-name

from mock import MagicMock
import pytest


from DIRAC.Interfaces.API.Job import Job

# sut
from DIRAC.TransformationSystem.Client.TaskManager import WorkflowTasks


mockTransClient = MagicMock()
mockTransClient.setTaskStatusAndWmsID.return_value = {'OK': True}

WMSClientMock = MagicMock()
jobMonitoringClient = MagicMock()

wfTasks = WorkflowTasks(transClient=mockTransClient,
                        submissionClient=WMSClientMock,
                        jobMonitoringClient=jobMonitoringClient,
                        outputDataModule="mock")
odm_o = MagicMock()
odm_o.execute.return_value = {'OK': True, 'Value': {}}
wfTasks.outputDataModule_o = odm_o

taskDict = {1: {'TransformationID': 1, 'a1': 'aa1', 'b1': 'bb1', 'Site': 'MySite'},
            2: {'TransformationID': 1, 'a2': 'aa2', 'b2': 'bb2', 'InputData': ['a1', 'a2']},
            3: {'TransformationID': 2, 'a3': 'aa3', 'b3': 'bb3'}, }

taskDictNoInputs = {1: {'TransformationID': 1, 'a1': 'aa1', 'b1': 'bb1', 'Site': 'MySite'},
                    2: {'TransformationID': 1, 'a2': 'aa2', 'b2': 'bb2'},
                    3: {'TransformationID': 2, 'a3': 'aa3', 'b3': 'bb3'}, }

expected = {'OK': True,
            'Value': {1: {'a1': 'aa1', 'TaskObject': '', 'TransformationID': 1,
                          'b1': 'bb1', 'Site': 'ANY', 'JobType': 'User'},
                      2: {'TaskObject': '', 'a2': 'aa2', 'TransformationID': 1,
                          'InputData': ['a1', 'a2'], 'b2': 'bb2', 'Site': 'ANY', 'JobType': 'User'},
                      3: {'TaskObject': '', 'a3': 'aa3', 'TransformationID': 2,
                          'b3': 'bb3', 'Site': 'ANY', 'JobType': 'User'}
                      }
            }
expectedBulk = {'OK': True,
                'Value': {'BulkJobObject': '',
                          1: {'a1': 'aa1', 'TransformationID': 1, 'b1': 'bb1',
                              'Site': 'MySite', 'JobType': 'User'},
                          2: {'a2': 'aa2', 'TransformationID': 1, 'b2': 'bb2',
                              'InputData': ['a1', 'a2'], 'JobType': 'User'},
                          3: {'TransformationID': 2, 'a3': 'aa3', 'b3': 'bb3',
                              'JobType': 'User'}}}


@pytest.mark.parametrize("taskDictionary, bulkSubmissionFlag, result, expectedRes", [
    (taskDict, False, True, expected),
    (taskDict, True, False, expectedBulk),
    (taskDictNoInputs, True, True, expectedBulk),
])
def test_prepareTranformationTasks(taskDictionary, bulkSubmissionFlag, result, expectedRes):
  res = wfTasks.prepareTransformationTasks('', taskDictionary, 'test_user', 'test_group', 'test_DN',
                                           bulkSubmissionFlag=bulkSubmissionFlag)
  assert res['OK'] == result
  if res['OK']:
    for key, value in res['Value'].iteritems():
      if key != 'BulkJobObject':
        assert key in expectedRes['Value']
        for tKey, tValue in value.iteritems():
          assert tKey in expectedRes['Value'][key]
          if tKey == 'TaskObject':
            assert isinstance(tValue, Job)
          else:
            assert tValue == expectedRes['Value'][key][tKey]


def ourgetSitesForSE(ses):
  if ses == ['pippo'] or ses == 'pippo':
    return {'OK': True, 'Value': ['Site1']}
  elif ses == ['pluto'] or ses == 'pluto':
    return {'OK': True, 'Value': ['Site2']}
  elif ses == ['pippo', 'pluto'] or ses == 'pippo,pluto':
    return {'OK': True, 'Value': ['Site1', 'Site2']}


@pytest.mark.parametrize("paramsDict, expected", [
    ({'Site': '', 'TargetSE': ''}, ['ANY']),
    ({'Site': 'ANY', 'TargetSE': ''}, ['ANY']),
    ({'TargetSE': 'Unknown'}, ['ANY']),
    ({'Site': 'Site2', 'TargetSE': ''}, ['Site2']),
    ({'Site': 'Site1;Site2', 'TargetSE': 'pippo'}, ['Site1']),
    ({'Site': 'Site1;Site2', 'TargetSE': 'pippo,pluto'}, ['Site1', 'Site2']),
    ({'Site': 'Site1;Site2;Site3', 'TargetSE': 'pippo,pluto'}, ['Site1', 'Site2']),
    ({'Site': 'Site2', 'TargetSE': 'pippo,pluto'}, ['Site2']),
    ({'Site': 'ANY', 'TargetSE': 'pippo,pluto'}, ['Site1', 'Site2']),
    ({'Site': 'Site1', 'TargetSE': 'pluto'}, [])
])
def test__handleDestination(mocker, paramsDict, expected):
  mocker.patch('DIRAC.TransformationSystem.Client.TaskManagerPlugin.getSitesForSE', side_effect=ourgetSitesForSE)
  res = wfTasks._handleDestination(paramsDict)
  assert sorted(res) == sorted(expected)
