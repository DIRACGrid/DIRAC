"""
It is used to test client->db-> service.
  It requires the  Monitoring service to be running and installed (so discoverable in the .cfg),
  and this monitoring service should be connecting to an ElasticSeach instance
"""

# pylint: disable=invalid-name,wrong-import-position

import unittest
import tempfile
import time
import sys
from datetime import datetime

from DIRAC.Core.Base import Script
Script.parseCommandLine()


from DIRAC import gLogger
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.Utilities.JEncode import strToIntDict


class MonitoringTestCase(unittest.TestCase):

  def setUp(self):
    gLogger.setLevel('DEBUG')

    self.client = MonitoringClient()

    self.data = [{u'Status': u'Waiting', 'Jobs': 2, u'timestamp': 1458130176, u'JobSplitType': u'MCStripping',
                  u'MinorStatus': u'unset', u'Site': u'LCG.GRIDKA.de', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00049848', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458130176, u'JobSplitType': u'User',
                  u'MinorStatus': u'unset', u'Site': u'LCG.PIC.es', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'olupton', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458130176, u'JobSplitType': u'User',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'olupton', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458130176, u'JobSplitType': u'MCStripping',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00049845', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 34, u'timestamp': 1458141578, u'JobSplitType': u'DataStripping',
                  u'MinorStatus': u'unset', u'Site': u'Group.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050299', u'UserGroup': u'lhcb_data', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 120, u'timestamp': 1458141578, u'JobSplitType': u'User',
                  u'MinorStatus': u'unset', u'Site': u'LCG.CERN.ch', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'mvesteri', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458141578, u'JobSplitType': u'MCStripping',
                  u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00049845', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 2, u'timestamp': 1458141578, u'JobSplitType': u'MCStripping',
                  u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00049848', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458141578, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050286', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 95, u'timestamp': 1458199202, u'JobSplitType': u'User',
                  u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'mamartin', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 3, u'timestamp': 1458199202, u'JobSplitType': u'User',
                  u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'olupton', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 129, u'timestamp': 1458199202, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00049844', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 5, u'timestamp': 1458217812, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050232', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 7, u'timestamp': 1458217812, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050234', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458217812, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 1, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050236', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 3, u'timestamp': 1458217812, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050238', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 2, u'timestamp': 1458217812, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 12, u'timestamp': 1458218413, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 5, u'timestamp': 1458218413, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050250', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 4, u'timestamp': 1458218413, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050251', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458218413, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050280', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 24, u'timestamp': 1458219012, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.NIKHEF.nl', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 3, u'timestamp': 1458219012, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.NIKHEF.nl', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050251', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458222013, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.Bologna.it', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050303',
                  u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 7, u'timestamp': 1458222013, u'JobSplitType': u'User',
                  u'MinorStatus': u'unset', u'Site': u'LCG.Bristol.uk', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset', u'User': u'clangenb', u'JobGroup': u'lhcb',
                  u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 2, u'timestamp': 1458222013, u'JobSplitType': u'User',
                  u'MinorStatus': u'unset', u'Site': u'LCG.Bristol.uk', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset', u'User': u'mrwillia', u'JobGroup': u'lhcb',
                  u'UserGroup': u'lhcb_user',
                  u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458222013, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050244', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 11, u'timestamp': 1458222013, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050246', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 22, u'timestamp': 1458222013, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 23, u'timestamp': 1458225013, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00049844', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 18, u'timestamp': 1458225013, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00049847', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458225013, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050238', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458225013, u'JobSplitType': u'MCSimulation',
                  u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050246', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0,
                  u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050243',
                  u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050251', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCStripping',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050256', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050229', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050241', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050243', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 2, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
                  u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset',
                  u'User': u'phicharp', u'JobGroup': u'00050247', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'}]

  def tearDown(self):
    pass


class MonitoringInsertData(MonitoringTestCase):
  def test_addMonitoringRecords(self):
    result = self.client.addMonitoringRecords('moni', 'WMSHistory', self.data)
    self.assertTrue(result['Message'])

  def test_bulkinsert(self):
    result = self.client.addRecords("wmshistory_index", "WMSHistory", self.data)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], len(self.data))
    time.sleep(10)


class MonitoringTestChain(MonitoringTestCase):

  def test_listReports(self):
    result = self.client.listReports('WMSHistory')
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ['AverageNumberOfJobs', 'NumberOfJobs', 'NumberOfReschedules'])

  def test_listUniqueKeyValues(self):
    result = self.client.listUniqueKeyValues('WMSHistory')
    self.assertTrue(result['OK'])
    self.assertTrue('Status' in result['Value'])
    self.assertTrue('JobSplitType' in result['Value'])
    self.assertTrue('MinorStatus' in result['Value'])
    self.assertTrue('Site' in result['Value'])
    self.assertTrue('ApplicationStatus' in result['Value'])
    self.assertTrue('User' in result['Value'])
    self.assertTrue('JobGroup' in result['Value'])
    self.assertTrue('UserGroup' in result['Value'])
    self.assertDictEqual(result['Value'], {u'Status': [],
                                           u'JobSplitType': [],
                                           u'MinorStatus': [],
                                           u'Site': [],
                                           u'ApplicationStatus': [],
                                           u'User': [],
                                           u'JobGroup': [],
                                           u'UserGroup': []})

  def test_generatePlot(self):
    params = (
        'WMSHistory', 'NumberOfJobs', datetime(
            2016, 3, 16, 12, 30, 0, 0), datetime(
            2016, 3, 17, 19, 29, 0, 0), {
            'grouping': ['Site']}, 'Site', {})
    result = self.client.generateDelayedPlot(*params)
    self.assertTrue(result['OK'])
    # self.assertEqual(
    #     result['Value'],
    #     {
    #     plot = 'Z:eNpljcEKwjAQRH8piWLbvQkeRLAeKnhOm7Us2CTsbsH69UYUFIQZZvawb4LUMKQYdjRoKH3kNGeK403W0JEiolSAMZ\
    #     xpwodXcsZukFZItipukFyxeSmiNIB3Zb_lUQL-wD4ssQYYc2Jt_VQuB-089cin6yH1Ur5FPev_\
    #     UgnrSjXfpRp0yfjGGLgcuz2JJl7wCYg6Slo='
    #         'plot': plot,
    #         'thumbnail': False})

  def test_getPlot(self):
    tempFile = tempfile.TemporaryFile()
    transferClient = TransferClient('Monitoring/Monitoring')
    params = (
        'WMSHistory', 'NumberOfJobs', datetime(
            2016, 3, 16, 12, 30, 0, 0), datetime(
            2016, 3, 17, 19, 29, 0, 0), {
            'grouping': ['Site']}, 'Site', {})
    result = self.client.generateDelayedPlot(*params)
    self.assertTrue(result['OK'])
    result = transferClient.receiveFile(tempFile, result['Value']['plot'])
    self.assertTrue(result['OK'])

  def test_getReport(self):
    params = (
        'WMSHistory', 'NumberOfJobs', datetime(
            2016, 3, 16, 12, 30, 0, 0), datetime(
            2016, 3, 17, 19, 29, 0, 0), {
            'grouping': ['Site']}, 'Site', {})
    result = self.client.getReport(*params)
    self.assertTrue(result['OK'])
    result['Value']['data'] = {site: strToIntDict(value) for site, value in result['Value']['data'].iteritems()}
    self.assertDictEqual(result['Value'],
                         {'data': {u'Multiple': {1458198000: 227.0},
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
                          'granularity': 1800})

  def test_getLastDayData(self):
    params = {'Status': 'Running', 'Site': 'LCG.NIKHEF.nl'}
    result = self.client.getLastDayData('WMSHistory', params)
    self.assertTrue(result['OK'])
    self.assertEqual(len(result['Value']), 2)
    self.assertEqual(sorted(result['Value'][0].keys()), sorted([u'Status',
                                                                u'Jobs',
                                                                u'JobSplitType',
                                                                u'timestamp',
                                                                u'MinorStatus',
                                                                u'Site',
                                                                u'Reschedules',
                                                                u'ApplicationStatus',
                                                                u'User',
                                                                u'JobGroup',
                                                                u'UserGroup']))


class MonitoringDeleteChain(MonitoringTestCase):

  def test_deleteNonExistingIndex(self):
    res = self.client.deleteIndex("alllaaaaa")
    self.assertTrue(res['Message'])

  def test_deleteIndex(self):
    today = datetime.today().strftime("%Y-%m-%d")
    result = "%s-%s" % ('wmshistory_index', today)
    res = self.client.deleteIndex(result)
    self.assertTrue(res['OK'])
    self.assertTrue('_index-%s' % today in res['Value'])


if __name__ == '__main__':
  testSuite = unittest.defaultTestLoader.loadTestsFromTestCase(MonitoringTestCase)
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MonitoringInsertData))
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MonitoringTestChain))
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MonitoringDeleteChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(testSuite)
  sys.exit(not testResult.wasSuccessful())
