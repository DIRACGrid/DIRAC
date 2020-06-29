"""Tests for the CheckMigration Operation"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools

import pytest
from mock import MagicMock

from DIRAC import S_OK
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request

from DIRAC.DataManagementSystem.Agent.RequestOperations import CheckMigration

MODULE = 'DIRAC.DataManagementSystem.Agent.RequestOperations.CheckMigration'
FILE_NAME = 'fileName'
N_FILES = 3


@pytest.fixture
def listOfLFNs():
  lfns = []
  for index, name in enumerate([FILE_NAME] * N_FILES):
    lfns.append('/vo/%s_%d' % (name, index))
  return lfns


@pytest.fixture
def seMock(mocker):
  """Mock call to StorageElement."""
  seModMock = mocker.MagicMock(name='StorageElementModule')
  seClassMock = mocker.MagicMock(name='StorageElementClass')
  seClassMock.getFileMetadata = mocker.MagicMock(return_value=S_OK({'Migrated': 0}))
  seModMock.return_value = seClassMock
  mocker.patch(MODULE + '.StorageElement', new=seModMock)
  return seModMock, seClassMock


@pytest.fixture
def checkRequestAndOp(listOfLFNs):
  req = Request()
  req.RequestName = 'MyRequest'
  op = Operation()
  op.Type = 'CheckMigration'
  for index, lfn in enumerate(listOfLFNs):
    oFile = File()
    oFile.LFN = lfn
    oFile.Size = index
    oFile.Checksum = '01130a%0d' % index
    oFile.ChecksumType = 'adler32'
    op.addFile(oFile)
  req.addOperation(op)
  return req, op


@pytest.fixture
def multiRetVal(listOfLFNs):
  """Return a return structure for multiple values"""
  def retFunc(*args, **kwargs):
    retVal = {'OK': True, 'Value':
              {'Failed': {},
               'Successful': {},
               }}
    for lfn in listOfLFNs:
      if kwargs.get('OK', not kwargs.get('Error', False)):
        retVal['Value']['Successful'][lfn] = {'Migrated': kwargs.get('Migrated', 0)}
      else:
        retVal['Value']['Failed'][lfn] = kwargs.get('Error', 'Failed to do X')
    return retVal
  return retFunc


@pytest.fixture
def checkMigration(mocker, checkRequestAndOp):
  cm = CheckMigration.CheckMigration(checkRequestAndOp[1])
  return cm


def test_constructor(checkMigration):
  assert checkMigration.waitingFiles == []


def test_run_NotMigrated(checkMigration, seMock, multiRetVal):
  seModMock, seClassMock = seMock
  seClassMock.getFileMetadata = MagicMock(side_effect=functools.partial(multiRetVal, Migrated=0))
  checkMigration._run()
  assert len(checkMigration.waitingFiles) == N_FILES
  seModMock.assert_called_with('')
  for opFile in checkMigration.operation:
    assert opFile.Status == 'Waiting'


def test_run_Migrated(checkMigration, seMock, multiRetVal):
  seModMock, seClassMock = seMock
  seClassMock.getFileMetadata = MagicMock(side_effect=functools.partial(multiRetVal, Migrated=1))
  checkMigration._run()
  assert len(checkMigration.waitingFiles) == N_FILES
  for opFile in checkMigration.operation:
    assert opFile.Status == 'Done'


def test_run_Failed(checkMigration, seMock, multiRetVal):
  seModMock, seClassMock = seMock
  seClassMock.getFileMetadata = MagicMock(side_effect=functools.partial(multiRetVal, Error='Fail Fail Fail'))
  checkMigration._run()
  assert len(checkMigration.waitingFiles) == N_FILES
  for opFile in checkMigration.operation:
    assert opFile.Status == 'Waiting'


def test_call_Migrated(checkMigration, seMock, multiRetVal):
  seModMock, seClassMock = seMock
  seClassMock.getFileMetadata = MagicMock(side_effect=functools.partial(multiRetVal, Migrated=1))
  assert checkMigration()['OK']
  assert len(checkMigration.waitingFiles) == N_FILES
  for opFile in checkMigration.operation:
    assert opFile.Status == 'Done'


def test_call_Exception(checkMigration, seMock, multiRetVal):
  seModMock, seClassMock = seMock
  seClassMock.getFileMetadata = MagicMock(side_effect=RuntimeError('Throw Down'))
  ret = checkMigration()
  assert not ret['OK']
  assert ret['Message'] == 'Throw Down'
  assert len(checkMigration.waitingFiles) == N_FILES
  for opFile in checkMigration.operation:
    assert opFile.Status == 'Waiting'
