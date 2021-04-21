""" This tests only need the PilotAgentsDB, and connects directly to it

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_PilotAgentsDB.py
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=wrong-import-position

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from mock import patch
from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PivotedPilotSummaryTable

gLogger.setLevel('DEBUG')

paDB = PilotAgentsDB()


def preparePilots(stateCount, testSite, testCE, testGroup):
  """
  Set up a bunch of pilots in different states.

  :param list stateCount:
  :param str testSite: Site name
  :param str testCE: CE name
  :param str testGroup: group name
  :return list pilot reference list:
  """
  pilotRef = []
  nPilots = sum(stateCount)

  for i in range(nPilots):
    pilotRef.append('pilotRef_' + str(i))

  res = paDB.addPilotTQReference(pilotRef, 123, 'ownerDN', testGroup, )
  assert res['OK'] is True, res['Message']

  index = 0
  for j, num in enumerate(stateCount):
    for i in range(num):
      pNum = i + index
      res = paDB.setPilotStatus('pilotRef_' + str(pNum), PivotedPilotSummaryTable.pstates[j], destination=testCE,
                                statusReason='Test States', gridSite=testSite, queue=None,
                                benchmark=None, currentJob=num,
                                updateTime=None, conn=False)
      assert res['OK'] is True, res['Message']

    index += num
  return pilotRef


def cleanUpPilots(pilotRef):
  """
  Delete all pilots pointed to by pilotRef

  :param  lipilotRef:
  :return:
  """

  for elem in pilotRef:
    res = paDB.deletePilot(elem)
    assert res['OK'] is True, res['Message']


def test_basic():
  """ usual insert/verify
  """
  res = paDB.addPilotTQReference(['pilotRef'], 123, 'ownerDN', 'ownerGroup',)
  assert res['OK'] is True

  res = paDB.deletePilot('pilotRef')

  # FIXME: to expand...


@patch('DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB.getVOForGroup')
def test_getGroupedPilotSummary(mocked_fcn):
  """
  Test 'pivoted' pilot summary method.

  :return: None
  """
  stateCount = [10, 50, 7, 3, 12, 8, 6, 4]
  testGroup = 'ownerGroup'
  testGroupVO = 'ownerGroupVO'
  testCE = 'TestCE'
  testSite = 'TestSite'

  mocked_fcn.return_value = 'ownerGroupVO'

  pilotRef = preparePilots(stateCount, testSite, testCE, testGroup)
  selectDict = {}
  columnList = ['GridSite', 'DestinationSite', 'OwnerGroup']
  res = paDB.getGroupedPilotSummary(selectDict, columnList)

  cleanUpPilots(pilotRef)
  expectedParameterList = ['Site', 'CE', 'OwnerGroup', 'Submitted', 'Done', 'Failed',
                           'Aborted', 'Running', 'Waiting', 'Scheduled', 'Ready',
                           'Total', 'PilotsPerJob', 'PilotJobEff', 'Status']

  assert res['OK'] is True, res['Message']
  values = res['Value']
  assert 'ParameterNames' in values, "ParameterNames key missing in result"
  assert values['ParameterNames'] == expectedParameterList, "Expected and obtained ParameterNames differ"

  assert 'Records' in values, "Records key missing in result"
  # in the setup with one Site/CE/OwnerGroup there will be only one record:
  assert len(values['Records']) == 1
  record = values['Records'][0]
  assert len(record) == len(expectedParameterList)
  assert record[0] == testSite
  assert record[1] == testCE
  assert record[2] == testGroupVO

  # pilot state counts:
  for i, entry in enumerate(record[3:10]):
    assert entry == stateCount[i], " found entry: %s, expected stateCount: %d " % (str(entry), stateCount[i])
  # Total
  total = record[expectedParameterList.index('Total')]
  assert total == sum(stateCount)
  # pilot efficiency
  delta = 0.01
  accuracy = record[expectedParameterList.index('PilotJobEff')] - 100.0 * \
      (total - record[expectedParameterList.index('Aborted')]) / total
  assert accuracy <= delta, " Pilot eff accuracy %d should be < %d " % (accuracy, delta)
  # there aren't any jobs, so:
  assert record[expectedParameterList.index('Status')] == 'Idle'


def test_PivotedPilotSummaryTable():
  """
  Test the 'pivoted' query only. Check whether the number of pilots in different states returned by
  the query is correct.

  :return: None
  """

  # PivotedPilotSummaryTable pstates gives pilot possible states (table.pstates)
  # pstates = ['Submitted', 'Done', 'Failed', 'Aborted', 'Running', 'Waiting', 'Scheduled', 'Ready']

  stateCount = [10, 50, 7, 3, 12, 8, 6, 4]
  testGroup = 'ownerGroup'
  testCE = 'TestCE'
  testSite = 'TestSite'

  pilotRef = preparePilots(stateCount, testSite, testCE, testGroup)

  table = PivotedPilotSummaryTable(['GridSite', 'DestinationSite', 'OwnerGroup'])

  sqlQuery = table.buildSQL()
  res = paDB._query(sqlQuery)
  assert res['OK'] is True, res['Message']

  columns = table.getColumnList()
  # first 3 columns are: Site, CE and a group (VO mapping comes later, not in the SQL above)
  assert 'Site' in columns
  assert columns.index('Site') == 0
  assert 'CE' in columns
  assert columns.index('CE') == 1
  assert 'OwnerGroup' in columns
  assert columns.index('OwnerGroup') == 2

  # pilot numbers by states:
  assert 'Total' in columns

  # with the setup above there will be only one row, first 3 elements must match the columns.
  row = res['Value'][0]
  assert row[0] == testSite
  assert row[1] == testCE
  assert row[2] == testGroup

  total = row[columns.index('Total')]

  assert total == sum(stateCount), res['Value']

  for i, state in enumerate(table.pstates):
    assert state in columns
    assert row[columns.index(state)] == stateCount[i], " state: %s, stateCount: %d " % (state, stateCount[i])

  cleanUpPilots(pilotRef)
