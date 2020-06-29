""" This is a test of the chain
    PilotsLoggingClient -> PilotsLoggingHandler -> PilotsLoggingDB

    It supposes that the DB is present, and that the service is running
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import sys

from DIRAC.WorkloadManagementSystem.Client.PilotsLoggingClient import PilotsLoggingClient

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


class TestPilotsLogging(unittest.TestCase):

  def setUp(self):
    self.pilotsLoggingClient = PilotsLoggingClient()

  def tearDown(self):
    pass


class PilotsLogging(TestPilotsLogging):
  def test_PilotsLoggingAddGetDelete(self):
    resp = self.pilotsLoggingClient.addPilotsLogging(
        '11111111-1111-1111-1111-111111111111',
        'timestamp1',
        'test',
        'phase',
        'status',
        'messageContent')
    self.assertTrue(resp['OK'], 'Failed to add PilotsLogging')
    resp = self.pilotsLoggingClient.addPilotsLogging(
        '11111111-1111-1111-1111-111111111111',
        'timestamp2',
        'test2',
        'phase2',
        'status2',
        'messageContent2')
    self.assertTrue(resp['OK'], 'Failed to add PilotsLogging')
    resp = self.pilotsLoggingClient.getPilotsLogging('11111111-1111-1111-1111-111111111111')
    self.assertTrue(resp['OK'], 'Failed to get PilotsLogging')
    test_sample = {
        'pilotUUID': '11111111-1111-1111-1111-111111111111',
        'timestamp': 'timestamp1',
        'source': 'test',
        'phase': 'phase',
        'status': 'status',
        'messageContent': 'messageContent',
    }
    test_sample2 = {
        'pilotUUID': '11111111-1111-1111-1111-111111111111',
        'timestamp': 'timestamp2',
        'source': 'test2',
        'phase': 'phase2',
        'status': 'status2',
        'messageContent': 'messageContent2',
    }
    self.assertEqual(resp['Value'], [test_sample, test_sample2], 'Wrong data comes out of Service')
    resp = self.pilotsLoggingClient.deletePilotsLogging('11111111-1111-1111-1111-111111111111')
    self.assertTrue(resp['OK'], 'Failed to delete PilotsLogging')
    resp = self.pilotsLoggingClient.getPilotsLogging('11111111-1111-1111-1111-111111111111')
    self.assertTrue(resp['OK'])
    self.assertEqual(resp['Value'], [], 'PilotsLogging was not really deleted')

  def test_PilotsLoggingEmptyGetDelete(self):

    resp = self.pilotsLoggingClient.getPilotsLogging('11111111-1111-1111-1111-111111111111')
    self.assertTrue(resp['OK'], 'Failed to get PilotsLogging')
    resp = self.pilotsLoggingClient.deletePilotsLogging('11111111-1111-1111-1111-111111111111')
    self.assertTrue(resp['OK'], 'Failed to delete PilotsLogging')

  def test_PilotsLoggingDeleteList(self):

    test_sample1 = {
        'pilotUUID': '11111111-1111-1111-1111-111111111111',
        'timestamp': 'timestamp1',
        'source': 'test',
        'phase': 'phase1',
        'status': 'status1',
        'messageContent': 'messageContent1',
    }
    test_sample2 = {
        'pilotUUID': '22222222-2222-2222-2222-222222222222',
        'timestamp': 'timestamp2',
        'source': 'test',
        'phase': 'phase2',
        'status': 'status2',
        'messageContent': 'messageContent2',
    }

    resp = self.pilotsLoggingClient.addPilotsLogging(
        '11111111-1111-1111-1111-111111111111',
        'timestamp1',
        'test',
        'phase1',
        'status1',
        'messageContent1')
    self.assertTrue(resp['OK'], 'Failed to add PilotsLogging')
    resp = self.pilotsLoggingClient.addPilotsLogging(
        '22222222-2222-2222-2222-222222222222',
        'timestamp2',
        'test',
        'phase2',
        'status2',
        'messageContent2')
    resp = self.pilotsLoggingClient.getPilotsLogging('11111111-1111-1111-1111-111111111111')
    self.assertTrue(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [test_sample1], 'Wrong data comes out of Service')
    resp = self.pilotsLoggingClient.getPilotsLogging('22222222-2222-2222-2222-222222222222')
    self.assertTrue(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [test_sample2], 'Wrong data comes out of Service')
    resp = self.pilotsLoggingClient.deletePilotsLogging(
        ['11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222'])
    self.assertTrue(resp['OK'], 'Failed to delete PilotsLogging')
    resp = self.pilotsLoggingClient.getPilotsLogging('11111111-1111-1111-1111-111111111111')
    self.assertTrue(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [], 'PilotsLogging was not really deleted')
    resp = self.pilotsLoggingClient.getPilotsLogging('22222222-2222-2222-2222-222222222222')
    self.assertTrue(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [], 'PilotsLogging was not really deleted')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestPilotsLogging)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsLogging))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
