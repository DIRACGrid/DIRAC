""" Test for MonitoringDB
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import time
import json
import pytest

from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB


#############################################

gLogger.setLevel('DEBUG')

# Aggs result

# For bucketed data
aggResult = {
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

aggResultStatusRunning = {
    u'Running': {1458216000: 22.333333333333332,
                 1458219600: 44.0,
                 1458223200: 43.0}}

aggResultStatusRunningAndSite = {
    u'Running': {1458223200: 43.0}}


# for aggregated data
aggResult_aggregated = {
    u'Running': {1458216000: 6.090909090909091,
                 1458219600: 7.333333333333333,
                 1458223200: 10.75},
    u'Waiting': {1458129600: 1.25,
                 1458140400: 31.6,
                 1458198000: 75.66666666666667,
                 1458223200: 1.1428571428571428}}

aggResultStatusRunning_aggregated = {
    u'Running': {1458216000: 6.090909090909091,
                 1458219600: 7.333333333333333,
                 1458223200: 10.75}}

aggResultStatusRunningAndSite_aggregated = {
    u'Running': {1458223200: 10.75}}


# create the MonitoringDB object and document type
monitoringDB = MonitoringDB()


# fixture for preparation + teardown
@pytest.fixture
def putAndDelete():
  # Find the test data
  fj = find_all('WMSHistory_testData.json', '../', 'tests/Integration/Monitoring')[0]
  with open(fj) as fp:
    data = json.load(fp)

  # hack to work on a test index, which is just the same as WMSHistory
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

  # put
  res = monitoringDB.put(data, 'test')
  assert res['OK']
  #  Add a time delay to allow updating the modified index before querying it.
  time.sleep(4)

  yield putAndDelete

  # from here on is teardown

  # delete the index
  monitoringDB.deleteIndex('test-*')


#############################################
#  actual tests
#############################################


def test_deleteWMSIndex():
  result = monitoringDB.getIndexName('WMSHistory')
  assert result['OK'], result['Message']

  today = time.strftime("%Y-%m-%d")
  indexName = "%s-%s" % (result['Value'], today)
  res = monitoringDB.deleteIndex(indexName)
  assert res['OK']


def test_putAndGetWMSHistory(putAndDelete):
  res = monitoringDB.getDataForAGivenPeriod(
      'test', {}, initialDate='16/03/2016 03:46', endDate='20/03/2016 00:00')
  assert res['OK']
  assert len(res['Value']) == 40


@pytest.mark.parametrize("selectField_input, condDict_input, expected, expected_result", [
    ('', {}, False, None),
    ('Jobs', {}, True, aggResult),
    ('Jobs', {'': ''}, True, aggResult),
    ('Jobs', {'Status': ['']}, True, aggResult),
    ('Jobs', {'Status': ['Running']}, True, aggResultStatusRunning),
    ('Jobs', {'Status': ['Running', '']}, True, aggResultStatusRunning),
    ('Jobs', {'Status': ['Running', 'Waiting']}, True, aggResult),
    ('Jobs', {'Status': ['Done']}, True, {}),
    ('Jobs', {'Status': ['Running'], 'Site': ['LCG.DESYZN.de']}, True, aggResultStatusRunningAndSite),
])
def test_retrieveBucketedData(selectField_input, condDict_input, expected, expected_result, putAndDelete):
  res = monitoringDB.retrieveBucketedData(
      typeName='test',
      startTime=1458100000,
      endTime=1458500000,
      interval='1h',
      selectField=selectField_input,
      condDict=condDict_input,
      grouping='Status')
  assert res['OK'] is expected
  if res['OK']:
    print(res['Value'])
    assert res['Value'] == expected_result


@pytest.mark.parametrize("selectField_input, condDict_input, expected, expected_result", [
    ('', {}, False, None),
    ('Jobs', {}, True, aggResult_aggregated),
    ('Jobs', {'': ''}, True, aggResult_aggregated),
    ('Jobs', {'Status': ['']}, True, aggResult_aggregated),
    ('Jobs', {'Status': ['Running']}, True, aggResultStatusRunning_aggregated),
    ('Jobs', {'Status': ['Running', '']}, True, aggResultStatusRunning_aggregated),
    ('Jobs', {'Status': ['Running', 'Waiting']}, True, aggResult_aggregated),
    ('Jobs', {'Status': ['Done']}, True, {}),
    ('Jobs', {'Status': ['Running'], 'Site': ['LCG.DESYZN.de']}, True, aggResultStatusRunningAndSite_aggregated),
])
def test_retrieveAggregatedData(selectField_input, condDict_input, expected, expected_result, putAndDelete):
  res = monitoringDB.retrieveAggregatedData(
      typeName='test',
      startTime=1458100000,
      endTime=1458500000,
      interval='1h',
      selectField=selectField_input,
      condDict=condDict_input,
      grouping='Status')
  assert res['OK'] is expected
  if res['OK']:
    print(res['Value'])
    assert res['Value'] == expected_result
