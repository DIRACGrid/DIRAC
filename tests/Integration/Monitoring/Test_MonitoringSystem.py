"""
It is used to test client->db-> service.
  It requires the Monitoring service to be running and installed (so discoverable in the .cfg),
  and this monitoring service should be connecting to an ElasticSeach instance
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

import time
import json
from datetime import datetime

import pytest

from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Core.Base import Script
Script.parseCommandLine()


from DIRAC import gLogger
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.Utilities.JEncode import strToIntDict


#############################################

gLogger.setLevel('DEBUG')

client = MonitoringClient()


# fixture for preparation + teardown
@pytest.fixture
def putAndDelete():
  # Find the test data
  fj = find_all('WMSHistory_testData.json', '../', 'tests/Integration/Monitoring')[0]
  with open(fj) as fp:
    data = json.load(fp)

  # put
  res = client.addRecords("wmshistory_index", "WMSHistory", data)
  assert res['OK']
  assert res['Value'] == len(data)
  time.sleep(5)

  yield putAndDelete

  # from here on is teardown

  # delete the index
  today = datetime.today().strftime("%Y-%m-%d")
  result = "%s-%s" % ('wmshistory_index', today)
  client.deleteIndex(result)


#############################################
#  actual tests
#############################################


def test_listReports(putAndDelete):

  result = client.listReports('WMSHistory')
  assert result['OK'], result['Message']
  assert result['Value'] == ['AverageNumberOfJobs', 'NumberOfJobs', 'NumberOfReschedules']


def test_listUniqueKeyValues(putAndDelete):

  result = client.listUniqueKeyValues('WMSHistory')
  assert result['OK'], result['Message']
  assert 'Status' in result['Value']
  assert 'JobSplitType' in result['Value']
  assert 'MinorStatus' in result['Value']
  assert 'Site' in result['Value']
  assert 'ApplicationStatus' in result['Value']
  assert 'User' in result['Value']
  assert 'JobGroup' in result['Value']
  assert 'UserGroup' in result['Value']
  assert result['Value'] == {u'Status': [],
                             u'JobSplitType': [],
                             u'MinorStatus': [],
                             u'Site': [],
                             u'ApplicationStatus': [],
                             u'User': [],
                             u'JobGroup': [],
                             u'UserGroup': []}


def test_generateDelayedPlot(putAndDelete):

  params = (
      'WMSHistory', 'NumberOfJobs', datetime(
          2016, 3, 16, 12, 30, 0, 0), datetime(
          2016, 3, 17, 19, 29, 0, 0), {
          'grouping': ['Site']}, 'Site', {})
  result = client.generateDelayedPlot(*params)
  assert result['OK'], result['Message']
  # self.assertEqual(
  #     result['Value'],
  #     {
  #     plot = 'Z:eNpljcEKwjAQRH8piWLbvQkeRLAeKnhOm7Us2CTsbsH69UYUFIQZZvawb4LUMKQYdjRoKH3kNGeK403W0JEiolSAMZ\
  #     xpwodXcsZukFZItipukFyxeSmiNIB3Zb_lUQL-wD4ssQYYc2Jt_VQuB-089cin6yH1Ur5FPev_\
  #     UgnrSjXfpRp0yfjGGLgcuz2JJl7wCYg6Slo='
  #         'plot': plot,
  #         'thumbnail': False})

  # tempFile = tempfile.TemporaryFile()
  # transferClient = TransferClient('Monitoring/Monitoring')

  # result = transferClient.receiveFile(tempFile, result['Value']['plot'])
  # assert result['OK'], result['Message']


def test_getReport(putAndDelete):

  params = (
      'WMSHistory',
      'NumberOfJobs',
      datetime(2016, 3, 16, 12, 30, 0, 0),
      datetime(2016, 3, 17, 19, 29, 0, 0),
      {'grouping': ['Site']},
      'Site',
      {})
  result = client.getReport(*params)
  assert result['OK'], result['Message']
  result['Value']['data'] = {site: strToIntDict(value) for site, value in result['Value']['data'].items()}
  assert result['Value'] == {'data': {u'Multiple': {1458198000: 227.0},
                                      u'LCG.RRCKI.ru': {1458225000: 3.0},
                                      u'LCG.IHEP.su': {1458217800: 18.0},
                                      u'LCG.CNAF.it': {1458144000: None,
                                                       1458172800: None,
                                                       1458194400: None,
                                                       1458145800: None,
                                                       1458189000: None,
                                                       1458147600: None,
                                                       1458178200: None,
                                                       1458183600: None,
                                                       1458212400: None,
                                                       1458149400: None,
                                                       1458207000: None,
                                                       1458151200: None,
                                                       1458169200: None,
                                                       1458201600: None,
                                                       1458153000: None,
                                                       1458196200: None,
                                                       1458154800: None,
                                                       1458174600: None,
                                                       1458190800: None,
                                                       1458156600: None,
                                                       1458185400: None,
                                                       1458214200: None,
                                                       1458158400: None,
                                                       1458180000: None,
                                                       1458216000: None,
                                                       1458208800: None,
                                                       1458160200: None,
                                                       1458203400: None,
                                                       1458162000: None,
                                                       1458142200: None,
                                                       1458198000: None,
                                                       1458163800: None,
                                                       1458192600: None,
                                                       1458165600: None,
                                                       1458176400: None,
                                                       1458187200: None,
                                                       1458167400: None,
                                                       1458210600: None,
                                                       1458140400: 4.0,
                                                       1458181800: None,
                                                       1458205200: None,
                                                       1458171000: None,
                                                       1458217800: 22.0,
                                                       1458199800: None},
                                      u'LCG.NIKHEF.nl': {1458217800: 27.0},
                                      u'LCG.Bari.it': {1458221400: 34.0},
                                      u'Group.RAL.uk': {1458140400: 34.0},
                                      u'LCG.DESYZN.de': {1458225000: 43.0},
                                      u'LCG.RAL.uk': {1458144000: None,
                                                      1458158400: None,
                                                      1458194400: None,
                                                      1458145800: None,
                                                      1458223200: None,
                                                      1458189000: None,
                                                      1458221400: None,
                                                      1458225000: 5.0,
                                                      1458147600: None,
                                                      1458135000: None,
                                                      1458183600: None,
                                                      1458212400: None,
                                                      1458149400: None,
                                                      1458178200: None,
                                                      1458207000: None,
                                                      1458151200: None,
                                                      1458169200: None,
                                                      1458172800: None,
                                                      1458219600: None,
                                                      1458201600: None,
                                                      1458153000: None,
                                                      1458196200: None,
                                                      1458154800: None,
                                                      1458160200: None,
                                                      1458190800: None,
                                                      1458156600: None,
                                                      1458185400: None,
                                                      1458214200: None,
                                                      1458129600: 2.0,
                                                      1458165600: None,
                                                      1458180000: None,
                                                      1458216000: None,
                                                      1458208800: None,
                                                      1458131400: None,
                                                      1458174600: None,
                                                      1458203400: None,
                                                      1458162000: None,
                                                      1458171000: None,
                                                      1458198000: None,
                                                      1458163800: None,
                                                      1458192600: None,
                                                      1458136800: None,
                                                      1458133200: None,
                                                      1458187200: None,
                                                      1458167400: None,
                                                      1458181800: None,
                                                      1458210600: None,
                                                      1458140400: None,
                                                      1458138600: None,
                                                      1458176400: None,
                                                      1458205200: None,
                                                      1458142200: None,
                                                      1458217800: None,
                                                      1458199800: None},
                                      u'LCG.PIC.es': {1458129600: 1.0},
                                      u'LCG.GRIDKA.de': {1458129600: 2.0},
                                      u'LCG.Bristol.uk': {1458221400: 9.0},
                                      u'LCG.CERN.ch': {1458140400: 120.0},
                                      u'LCG.Bologna.it': {1458221400: 1.0}},
                             'granularity': 1800}


def test_getLastDayData(putAndDelete):
  params = {'Status': 'Running', 'Site': 'LCG.NIKHEF.nl'}
  result = client.getLastDayData('WMSHistory', params)
  assert result['OK'], result['Message']
  assert len(result['Value']) == 2
  assert sorted(result['Value'][0]) == sorted([u'Status',
                                               u'Jobs',
                                               u'JobSplitType',
                                               u'timestamp',
                                               u'MinorStatus',
                                               u'Site',
                                               u'Reschedules',
                                               u'ApplicationStatus',
                                               u'User',
                                               u'JobGroup',
                                               u'UserGroup'])
