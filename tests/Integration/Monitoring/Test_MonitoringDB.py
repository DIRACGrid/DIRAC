""" Test for MonitoringDB
"""

import time
import json

from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB

#  Add a time delay to allow updating the modified index before querying it.
SLEEP_DELAY = 2

gLogger.setLevel('DEBUG')
monitoringDB = MonitoringDB()

#############################################
# hacking to work on a test index, which is just the same as WMSHistory
mapping = {
    "properties": {
	"Status": {"type": "keyword"},
	"Site": {"type": "keyword"},
	"JobSplitType": {"type": "keyword"},
	"ApplicationStatus": {"type": "keyword"},
	"MinorStatus": {"type": "keyword"},
	"User": {"type": "keyword"},
	"JobGroup": {"type": "keyword"},
	"UserGroup": {"type": "keyword"}}
}
monitoringDB.documentTypes.setdefault(
    'test',
    {'indexName': 'test',
     'mapping': mapping,
     'monitoringFields': ['Jobs', 'Reschedules'],
     'period': 'day'})

# Test data
fj = find_all('WMSHistory_testData.json', '../', 'tests/Integration/Monitoring')[0]
with open(fj) as fp:
  data = json.load(fp)

#############################################


def test_deleteWMSIndex():
  result = monitoringDB.getIndexName('WMSHistory')
  assert result['OK']

  today = time.strftime("%Y-%m-%d")
  indexName = "%s-%s" % (result['Value'], today)
  res = monitoringDB.deleteIndex(indexName)
  assert res['OK']


def test_putAndGetWMSHistory():
  # put
  res = monitoringDB.put(data, 'test')
  assert res['OK']
  time.sleep(SLEEP_DELAY)

  # get
  res = monitoringDB.getDataForAGivenPeriod(
      'test', {}, initialDate='16/03/2016 03:46', endDate='20/03/2016 00:00')
  assert res['OK']
  assert len(res['Value']) == 40

  # delete
  res = monitoringDB.deleteIndex('test-*')
  assert res['OK']


def test_aggregations():
  # put
  res = monitoringDB.put(data, 'test')
  assert res['OK']
  time.sleep(SLEEP_DELAY)

  # get
  res = monitoringDB.retrieveBucketedData(
      typeName='test',
      startTime=1458100000,
      endTime=1458300000,
      interval='1h',
      selectFields='',
      condDict={},
      grouping='Status')
  assert res['OK']

  # delete
  res = monitoringDB.deleteIndex('test-*')
  assert res['OK']
