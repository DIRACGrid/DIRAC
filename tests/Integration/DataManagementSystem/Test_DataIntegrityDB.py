""" This is a test of the IntegrityDB

    It supposes that the DB is present.

    This is pytest!
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

from DIRAC import gLogger

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.DataManagementSystem.DB.DataIntegrityDB import DataIntegrityDB


def test_DataIntegrityDB():
  """ Some test cases
  """

  diDB = DataIntegrityDB()

  source = 'Test'
  prognosis = 'TestError'
  prodID = 1234
  lfn = '/Test/%08d/File1' % prodID
  fileMetadata1 = {lfn: {'Prognosis': prognosis, 'PFN': 'File1', 'SE': 'Test-SE'}}
  fileOut1 = {'FileID': 1, 'LFN': lfn, 'PFN': 'File1', 'Prognosis': prognosis,
              'GUID': None, 'SE': 'Test-SE', 'Size': None}
  newStatus = 'Solved'
  newPrognosis = 'AnotherError'

  result = diDB.insertProblematic(source, fileMetadata1)
  assert result['OK']
  assert result['Value'] == {'Successful': {lfn: True}, 'Failed': {}}

  result = diDB.insertProblematic(source, fileMetadata1)
  assert result['OK']
  assert result['Value'] == {'Successful': {lfn: 'Already exists'}, 'Failed': {}}

  result = diDB.getProblematicsSummary()
  assert result['OK']
  assert result['Value'] == {'TestError': {'New': 1}}

  result = diDB.getDistinctPrognosis()
  assert result['OK']
  assert result['Value'] == ['TestError']

  result = diDB.getProblematic()
  assert result['OK']
  assert result['Value'] == fileOut1

  result = diDB.incrementProblematicRetry(result['Value']['FileID'])
  assert result['OK']
  assert result['Value'] == 1

  result = diDB.getProblematic()
  assert result['OK']
  assert result['Value'] == fileOut1

  result = diDB.getPrognosisProblematics(prognosis)
  assert result['OK']
  assert result['Value'] == [fileOut1]

  result = diDB.getTransformationProblematics(prodID)
  assert result['OK']
  assert result['Value'][lfn] == 1

  result = diDB.setProblematicStatus(1, newStatus)
  assert result['OK']
  assert result['Value'] == 1

  result = diDB.changeProblematicPrognosis(1, newPrognosis)
  assert result['OK']
  assert result['Value'] == 1

  result = diDB.getPrognosisProblematics(prognosis)
  assert result['OK']
  assert result['Value'] == []

  result = diDB.removeProblematic(1)
  assert result['OK']
  assert result['Value'] == 1

  result = diDB.getProblematicsSummary()
  assert result['OK']
  assert result['Value'] == {}

  gLogger.info('\n OK\n')
