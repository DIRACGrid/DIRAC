''' Test_RSS_Command_VOBOXAvailabilityCommand

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import mock

import DIRAC.ResourceStatusSystem.Command.VOBOXAvailabilityCommand as moduleTested
from six.moves import reload_module

__RCSID__ = '$Id$'

try:
  # Python 2: "reload" is built-in
  reload
except NameError:
  from importlib import reload  # pylint: disable=no-name-in-module


class VOBOXAvailabilityCommand_TestCase(unittest.TestCase):

  def setUp(self):
    '''
    Setup
    '''

    # Mock external libraries / modules not interesting for the unit test
    mock_RPC = mock.Mock()
    mock_RPC.ping.return_value = {'OK': True, 'Value': {}}

    mock_RPCClient = mock.Mock()
    mock_RPCClient.return_value = mock_RPC
    self.mock_RPCClient = mock_RPCClient

    # Add mocks to moduleTested
    moduleTested.RPCClient = self.mock_RPCClient

    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.VOBOXAvailabilityCommand

  def tearDown(self):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested
    del self.mock_RPCClient

################################################################################
# Tests


class VOBOXAvailabilityCommand_Success(VOBOXAvailabilityCommand_TestCase):

  def test_instantiate(self):
    ''' tests that we can instantiate one object of the tested class
    '''

    command = self.testClass()
    self.assertEqual('VOBOXAvailabilityCommand', command.__class__.__name__)

  def test_init(self):
    ''' tests that the init method does what it should do
    '''

    command = self.testClass()
    self.assertEqual({'onlyCache': False}, command.args)
    self.assertEqual({}, command.apis)

  def test_doCommand(self):
    ''' tests the doCommand method
    '''

    command = self.testClass()
    res = command.doCommand()

    self.assertEqual(False, res['OK'])

    command = self.testClass(args={'serviceURL': ''})
    res = command.doCommand()
    self.assertEqual(False, res['OK'])

    command = self.testClass(args={'serviceURL': 'protocol://site:port/path1/path2'})
    res = command.doCommand()
    self.assertTrue(res['OK'])
    self.assertEqual(0, res['Value']['serviceUpTime'])
    self.assertEqual(0, res['Value']['machineUpTime'])
    self.assertEqual('site', res['Value']['site'])
    self.assertEqual('path1', res['Value']['system'])
    self.assertEqual('path2', res['Value']['service'])

    mock_RPC = mock.Mock()
    mock_RPC.ping.return_value = {'OK': True,
                                  'Value': {'service uptime': 1,
                                            'host uptime': 2}}

    self.moduleTested.RPCClient.return_value = mock_RPC
    command = self.testClass(args={'serviceURL': 'protocol://site:port/path1/path2'})
    res = command.doCommand()
    self.assertTrue(res['OK'])
    self.assertEqual(1, res['Value']['serviceUpTime'])
    self.assertEqual(2, res['Value']['machineUpTime'])
    self.assertEqual('site', res['Value']['site'])
    self.assertEqual('path1', res['Value']['system'])
    self.assertEqual('path2', res['Value']['service'])

    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload_module(self.moduleTested)

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
