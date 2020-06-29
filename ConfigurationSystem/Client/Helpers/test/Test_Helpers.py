from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import pytest
from mock import MagicMock

from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform, getCompatiblePlatforms


mockGCReply = MagicMock()


@pytest.mark.parametrize("mockGCReplyInput, requested, expectedRes, expectedValue", [
    ({'OK': False, 'Message': 'error'}, 'plat', False, None),
    ({'OK': True, 'Value': ''}, 'plat', False, None),
    ({'OK': True, 'Value': {'plat1': 'OS1, OS2,  OS3',
                            'plat2': 'OS4, OS5',
                            'plat3': 'OS1, OS4'}}, 'plat', False, None),
    ({'OK': True, 'Value': {'plat1': 'OS1, OS2,  OS3',
                            'plat2': 'OS4, OS5',
                            'plat3': 'OS1, OS4'}}, 'OS1', True, ['plat1', 'plat3']),
    ({'OK': True, 'Value': {'plat1': 'OS1, OS2,  OS3',
                            'plat2': 'OS4, OS5',
                            'plat3': 'OS1, OS4'}}, 'OS2', True, ['plat1']),
    ({'OK': True, 'Value': {'plat1': 'OS1, OS2,  OS3',
                            'plat2': 'OS4, OS5',
                            'plat3': 'OS1, OS4'}}, 'OS3', True, ['plat1']),
    ({'OK': True, 'Value': {'plat1': 'OS1, OS2,  OS3',
                            'plat2': 'OS4, OS5',
                            'plat3': 'OS1, OS4'}}, 'OS4', True, ['plat2', 'plat3']),
    ({'OK': True, 'Value': {'plat1': 'OS1, OS2,  OS3',
                            'plat2': 'OS4, OS5',
                            'plat3': 'OS1, OS4'}}, 'OS5', True, ['plat2']),
    ({'OK': True, 'Value': {'plat1': 'OS1, OS2,  OS3',
                            'plat2': 'OS4, OS5',
                            'plat3': 'OS1, OS4'}}, 'plat1', True, ['plat1']),
])
def test_getDIRACPlatform(mocker, mockGCReplyInput, requested, expectedRes, expectedValue):

  mockGCReply.return_value = mockGCReplyInput

  mocker.patch('DIRAC.Interfaces.API.Dirac.gConfig.getOptionsDict', side_effect=mockGCReply)

  res = getDIRACPlatform(requested)
  assert res['OK'] is expectedRes, res
  if expectedRes:
    assert set(res['Value']) == set(expectedValue), res['Value']


@pytest.mark.parametrize("mockGCReplyInput, requested, expectedRes, expectedValue", [
    ({'OK': False, 'Message': 'error'}, 'plat', False, None),
    ({'OK': True, 'Value': ''}, 'plat', False, None),
    ({'OK': True, 'Value': {'plat1': 'xOS1, xOS2,  xOS3',
                            'plat2': 'sys2, xOS4, xOS5',
                            'plat3': 'sys1, xOS1, xOS4'}}, 'plat', True, ['plat']),
    ({'OK': True, 'Value': {'plat1': 'xOS1, xOS2,  xOS3',
                            'plat2': 'sys2, xOS4, xOS5',
                            'plat3': 'sys1, xOS1, xOS4'}}, 'plat1', True, ['plat1', 'xOS1', 'xOS2', 'xOS3'])
])
def test_getCompatiblePlatforms(mocker, mockGCReplyInput, requested, expectedRes, expectedValue):
  mockGCReply.return_value = mockGCReplyInput

  mocker.patch('DIRAC.Interfaces.API.Dirac.gConfig.getOptionsDict', side_effect=mockGCReply)

  res = getCompatiblePlatforms(requested)
  assert res['OK'] is expectedRes, res
  if expectedRes:
    assert set(res['Value']) == set(expectedValue), res['Value']
