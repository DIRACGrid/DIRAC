""" Test_RSS_Command_GOCDBStatusCommand
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from datetime import datetime, timedelta

import mock
import pytest

from DIRAC import gLogger, S_OK
from DIRAC.ResourceStatusSystem.Command.DowntimeCommand import DowntimeCommand

"""
Setup
"""
gLogger.setLevel('DEBUG')

# Mock external libraries / modules not interesting for the unit test
seMock = mock.MagicMock()
seMock.options = {'SEType': 'T0D1'}

mock_GOCDBClient = mock.MagicMock()
mock_RMClient = mock.MagicMock()
mock_RMClient.addOrModifyDowntimeCache.return_value = S_OK()

args = {'name': 'aName', 'element': 'Resource', 'elementType': 'StorageElement'}


def test_instantiate():
  """ tests that we can instantiate one object of the tested class
  """

  command = DowntimeCommand()
  assert command.__class__.__name__ == 'DowntimeCommand'


def test_init():
  """ tests that the init method does what it should do
  """

  command = DowntimeCommand()
  assert command.args == {'onlyCache': False}
  assert command.apis == {}

  command = DowntimeCommand(clients={'GOCDBClient': mock_GOCDBClient})
  assert command.args == {'onlyCache': False}
  assert command.apis == {'GOCDBClient': mock_GOCDBClient}

  command = DowntimeCommand(args)
  _args = dict(args)
  _args.update({'onlyCache': False})
  assert command.args == _args
  assert command.apis == {}


def test_doCache(mocker):
  """ tests the doCache method
  """
  mocker.patch("DIRAC.ResourceStatusSystem.Command.DowntimeCommand.StorageElement", return_value=seMock)
  mocker.patch("DIRAC.ResourceStatusSystem.Command.DowntimeCommand.getSEHosts",
               return_value=S_OK(['someHost', 'aSecondHost']))

  command = DowntimeCommand(args, {'ResourceManagementClient': mock_RMClient})
  res = command.doCache()
  assert res['OK'] is True

  # CASE01: get ongoing DT from 2 DTs where one ongoing the other in the future
  now = datetime.utcnow()
  resFromDB = {'OK': True,
               'Value': ((now - timedelta(hours=2),
                          '1 aRealName',
                          'https://blah',
                          now + timedelta(hours=3),
                          'aRealName',
                          now - timedelta(hours=2),
                          'maintenance',
                          'OUTAGE',
                          now,
                          'Resource'),
                         (now + timedelta(hours=12),
                          '2 aRealName',
                          'https://blah',
                          now + timedelta(hours=14),
                          'aRealName',
                          now + timedelta(hours=12),
                          'maintenance',
                          'OUTAGE',
                          now,
                          'Resource')
                         ),
               'Columns': ['StartDate', 'DowntimeID', 'Link', 'EndDate', 'Name',
                           'DateEffective', 'Description', 'Severity', 'LastCheckTime', 'Element']}

  mock_RMClient.selectDowntimeCache.return_value = resFromDB
  command = DowntimeCommand(args, {'ResourceManagementClient': mock_RMClient})
  res = command.doCache()
  assert res['OK'] is True
  assert res['Value']['DowntimeID'] == '1 aRealName'

  mock_RMClient.selectDowntimeCache.return_value = resFromDB
  args.update({'hours': 2})
  command = DowntimeCommand(args, {'ResourceManagementClient': mock_RMClient})
  res = command.doCache()
  assert res['OK'] is True
  assert res['Value']['DowntimeID'] == '1 aRealName'

  # CASE02: get future DT from 2 DTs where one ongoing the other in the future
  resFromDB = {'OK': True,
               'Value': ((now - timedelta(hours=12),
                          '1 aRealName',
                          'https://blah',
                          now - timedelta(hours=2),
                          'aRealName',
                          now - timedelta(hours=12),
                          'maintenance',
                          'OUTAGE',
                          now,
                          'Resource'),
                         (now + timedelta(hours=2),
                          '2 aRealName',
                          'https://blah',
                          now + timedelta(hours=14),
                          'aRealName',
                          now + timedelta(hours=2),
                          'maintenance',
                          'OUTAGE',
                          now,
                          'Resource')
                         ),
               'Columns': ['StartDate', 'DowntimeID', 'Link', 'EndDate', 'Name',
                           'DateEffective', 'Description', 'Severity', 'LastCheckTime', 'Element']}

  mock_RMClient.selectDowntimeCache.return_value = resFromDB
  args.update({'hours': 3})
  command = DowntimeCommand(args, {'ResourceManagementClient': mock_RMClient})
  res = command.doCache()
  assert res['OK'] is True
  assert res['Value']['DowntimeID'] == '2 aRealName'

  # CASE03: get DT from 2 overlapping OUTAGE DTs, one ongoing the other starting in the future
  resFromDB = {'OK': True,
               'Value': ((now - timedelta(hours=12),
                          '1 aRealName',
                          'https://blah',
                          now + timedelta(hours=2),
                          'aRealName',
                          now - timedelta(hours=12),
                          'maintenance',
                          'OUTAGE',
                          now,
                          'Resource'),
                         (now + timedelta(hours=2),
                          '2 aRealName',
                          'https://blah',
                          now + timedelta(hours=14),
                          'aRealName',
                          now + timedelta(hours=2),
                          'maintenance',
                          'OUTAGE',
                          now,
                          'Resource')
                         ),
               'Columns': ['StartDate', 'DowntimeID', 'Link', 'EndDate', 'Name',
                           'DateEffective', 'Description', 'Severity', 'LastCheckTime', 'Element']}

  mock_RMClient.selectDowntimeCache.return_value = resFromDB
  args.update({'hours': 0})
  command = DowntimeCommand(
      args, {'ResourceManagementClient': mock_RMClient})
  res = command.doCache()
  assert res['OK'] is True
  assert res['Value']['DowntimeID'] == '1 aRealName'

  # CASE04: get DT from 2 ongoing DTs, first OUTAGE the other WARNING
  resFromDB = {'OK': True,
               'Value': ((now - timedelta(hours=10),
                          '1 aRealName',
                          'https://blah',
                          now + timedelta(hours=2),
                          'aRealName',
                          now - timedelta(hours=12),
                          'maintenance',
                          'OUTAGE',
                          now,
                          'Resource'),
                         (now - timedelta(hours=12),
                          '2 aRealName',
                          'https://blah',
                          now + timedelta(hours=4),
                          'aRealName',
                          now + timedelta(hours=2),
                          'maintenance',
                          'WARNING',
                          now,
                          'Resource')
                         ),
               'Columns': ['StartDate', 'DowntimeID', 'Link', 'EndDate', 'Name',
                           'DateEffective', 'Description', 'Severity', 'LastCheckTime', 'Element']
               }

  mock_RMClient.selectDowntimeCache.return_value = resFromDB
  args.update({'hours': 0})
  command = DowntimeCommand(
      args, {'ResourceManagementClient': mock_RMClient})
  res = command.doCache()
  assert res['OK'] is True
  assert res['Value']['DowntimeID'] == '1 aRealName'

  # CASE05: get DT from 2 overlapping future DTs, the first WARNING the other OUTAGE
  resFromDB = {'OK': True,
               'Value': ((now + timedelta(hours=8),
                          '1 aRealName',
                          'https://blah',
                          now + timedelta(hours=12),
                          'aRealName',
                          now + timedelta(hours=8),
                          'maintenance',
                          'WARNING',
                          now,
                          'Resource'),
                         (now + timedelta(hours=9),
                          '2 aRealName',
                          'https://blah',
                          now + timedelta(hours=11),
                          'aRealName',
                          now + timedelta(hours=9),
                          'maintenance',
                          'OUTAGE',
                          now,
                          'Resource')
                         ),
               'Columns': ['StartDate', 'DowntimeID', 'Link', 'EndDate', 'Name',
                           'DateEffective', 'Description', 'Severity', 'LastCheckTime', 'Element']
               }

  mock_RMClient.selectDowntimeCache.return_value = resFromDB
  args.update({'hours': 10})
  command = DowntimeCommand(args, {'ResourceManagementClient': mock_RMClient})
  res = command.doCache()
  assert res['OK'] is True
  assert res['Value']['DowntimeID'] == '2 aRealName'


@pytest.mark.parametrize("downtimeCommandArgs, gocDBClientRV, expectedRes, expectedValue", [
    ({'element': 'X'}, None, False, None),
    ({'element': 'Site', "name": 'aSite', 'elementType': 'Z'}, S_OK(), True, None),
    ({'element': 'Resource', 'name': '669 devel.edu.mk', 'elementType': 'Z'},
     {'OK': True,
      'Value': {'669 devel.edu.mk': {
                'HOSTED_BY': 'MK-01-UKIM_II',
                'DESCRIPTION': 'Problem with SE server',
                'SEVERITY': 'OUTAGE',
                'HOSTNAME': 'devel.edu.mk',
                'GOCDB_PORTAL_URL': 'myURL',
                'FORMATED_END_DATE': '2011-07-20 00:00',
                'FORMATED_START_DATE': '2011-07-16 00:00'
                }}},
     True,
     None),
    ({'element': 'Resource', 'name': '669 devel.edu.mk', 'elementType': 'Z'},
     {'OK': True,
      'Value': {'669 devel.edu.mk': {
                'HOSTED_BY': 'MK-01-UKIM_II',
                'DESCRIPTION': 'Problem with SE server',
                'SEVERITY': 'OUTAGE',
                'HOSTNAME': 'devel.edu.mk',
                'URL': 'anotherDevel.edu.mk',
                'GOCDB_PORTAL_URL': 'myURL',
                'FORMATED_END_DATE': '2011-07-20 00:00',
                'FORMATED_START_DATE': '2011-07-16 00:00'
                }}},
     True,
     None)
])
def test_doNew(downtimeCommandArgs, gocDBClientRV, expectedRes, expectedValue):
  """ tests the doNew method
  """

  mock_GOCDBClient.getStatus.return_value = gocDBClientRV

  command = DowntimeCommand(downtimeCommandArgs, {'GOCDBClient': mock_GOCDBClient,
                                                  'ResourceManagementClient': mock_RMClient})
  res = command.doNew()
  assert res['OK'] is expectedRes
  if res['OK']:
    assert res['Value'] == expectedValue


def test_doMaster(mocker):
  """ tests the doMaster method
  """

  mocker.patch("DIRAC.ResourceStatusSystem.Command.DowntimeCommand.getGOCSites", return_value=S_OK())
  mocker.patch("DIRAC.ResourceStatusSystem.Command.DowntimeCommand.getStorageElementsHosts", return_value=S_OK())
  mocker.patch("DIRAC.ResourceStatusSystem.Command.DowntimeCommand.getFTS3Servers", return_value=S_OK())
  mocker.patch("DIRAC.ResourceStatusSystem.Command.DowntimeCommand.getGOCSites", return_value=S_OK())
  mocker.patch("DIRAC.ResourceStatusSystem.Command.DowntimeCommand.getCESiteMapping", return_value=S_OK())

  command = DowntimeCommand({'element': 'Resource', 'name': '669 devel.edu.mk', 'elementType': 'Z'},
                            {'GOCDBClient': mock_GOCDBClient,
                             'ResourceManagementClient': mock_RMClient})
  res = command.doMaster()
  assert res['OK'] is True
