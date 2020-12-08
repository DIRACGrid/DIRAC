""" Test for MonitoringDB
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import time
import json

from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB

#  Add a time delay to allow updating the modified index before querying it.
SLEEP_DELAY = 4

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


def test_retrieveBucketedData():
  # put
  res = monitoringDB.put(data, 'test')
  assert res['OK']
  time.sleep(SLEEP_DELAY)

  # get (wrong)
  res = monitoringDB.retrieveBucketedData(
      typeName='test',
      startTime=1458100000,
      endTime=1458500000,
      interval='1h',
      selectField='',
      condDict={},
      grouping='Status')
  assert not res['OK']  # selectField is empty

  # get - simple
  res = monitoringDB.retrieveBucketedData(
      typeName='test',
      startTime=1458100000,
      endTime=1458500000,
      interval='1h',
      selectField='Jobs',
      condDict={},
      grouping='Status')
  assert res['OK']
  assert isinstance(res['Value'], dict)
  assert res['Value'] == {
      u'Running': {1458216000: 22.333333333333332,
                   1458219600: 44.0,
                   1458223200: 43.0},
      u'Waiting': {1458129600: 5.0,
                   1458133200: None,
                   1458136800: None,
                   1458140400: 158.0,
                   1458144000: None,
                   1458147600: None,
                   1458151200: None,
                   1458154800: None,
                   1458158400: None,
                   1458162000: None,
                   1458165600: None,
                   1458169200: None,
                   1458172800: None,
                   1458176400: None,
                   1458180000: None,
                   1458183600: None,
                   1458187200: None,
                   1458190800: None,
                   1458194400: None,
                   1458198000: 227.0,
                   1458201600: None,
                   1458205200: None,
                   1458208800: None,
                   1458212400: None,
                   1458216000: None,
                   1458219600: None,
                   1458223200: 8.0}}

  # delete
  res = monitoringDB.deleteIndex('test-*')
  assert res['OK']


def test_retrieveAggregatedData():
  # put
  res = monitoringDB.put(data, 'test')
  assert res['OK']
  time.sleep(SLEEP_DELAY)

  # get
  res = monitoringDB.retrieveAggregatedData(
      typeName='test',
      startTime=1458100000,
      endTime=1458500000,
      interval='1h',
      selectField='',
      condDict={},
      grouping='Status')
  assert not res['OK']  # selectField is empty

  # delete
  res = monitoringDB.deleteIndex('test-*')
  assert res['OK']
