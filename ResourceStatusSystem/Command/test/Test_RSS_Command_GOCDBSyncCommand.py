"""
Test_RSS_Command_GOCDBStatusCommand
"""

import mock
import unittest
import importlib

from DIRAC.ResourceStatusSystem.Command.GOCDBSyncCommand import GOCDBSyncCommand
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC import gLogger, S_OK

__RCSID__ = '$Id:  $'

################################################################################

class GOCDBSyncCommand_TestCase( unittest.TestCase ):

  def setUp( self ):
    """
    Setup
    """
    gLogger.setLevel( 'DEBUG' )
    self.GOCDBSyncCommandModule = importlib.import_module( 'DIRAC.ResourceStatusSystem.Command.GOCDBSyncCommand' )
    self.mock_GOCDBClient = mock.MagicMock()
    self.mock_rsClient = ResourceManagementClient
    self.mock_rsClient = mock.MagicMock()

  def tearDown( self ):
    """
    TearDown
    """
    del self.mock_GOCDBClient
    del self.mock_rsClient
    del self.GOCDBSyncCommandModule

################################################################################
# Tests

class GOCDBSyncCommand_Success( GOCDBSyncCommand_TestCase ):

  def test_instantiate( self ):
    """ tests that we can instantiate the object
    """

    command = GOCDBSyncCommand()
    self.assertEqual( 'GOCDBSyncCommand', command.__class__.__name__ )

  def test_init( self ):
    """ tests that the init method
    """

    command = GOCDBSyncCommand()
    self.assertEqual( {}, command.apis )

    command = GOCDBSyncCommand( clients = {'GOCDBClient': self.mock_GOCDBClient} )
    self.assertEqual( {'onlyCache': False}, command.args )
    self.assertEqual( {'GOCDBClient': self.mock_GOCDBClient}, command.apis )


  def test_doNew( self ):
    """ tests the doNew method
    """

    command = GOCDBSyncCommand()
    res = command.doNew()
    self.assertEqual( False, res['OK'] )

    res = command.doNew('dummy.host.dummy')
    self.assertEqual( True, res['OK'] )
