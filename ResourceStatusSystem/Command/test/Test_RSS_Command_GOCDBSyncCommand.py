# """
# Test_RSS_Command_GOCDBStatusCommand
# """

# import mock
# import unittest
# import importlib

# from datetime import datetime, timedelta
# from DIRAC.ResourceStatusSystem.Command.GOCDBSyncCommand import GOCDBSyncCommand
# from DIRAC import gLogger, S_OK

# __RCSID__ = '$Id:  $'

# ################################################################################

# class GOCDBSyncCommand_TestCase( unittest.TestCase ):

#   def setUp( self ):
#     """
#     Setup
#     """
#     gLogger.setLevel( 'DEBUG' )
#     self.GOCDBSyncCommandModule = importlib.import_module( 'DIRAC.ResourceStatusSystem.Command.GOCDBSyncCommand' )
#     self.mock_GOCDBClient = mock.MagicMock()

#   def tearDown( self ):
#     """
#     TearDown
#     """
#     del self.mock_GOCDBClient
#     del self.GOCDBSyncCommandModule

# ################################################################################
# # Tests

# class GOCDBSyncCommand_Success( GOCDBSyncCommand_TestCase ):

#   def test_instantiate( self ):
#     """ tests that we can instantiate the object
#     """

#     command = GOCDBSyncCommand()
#     self.assertEqual( 'GOCDBSyncCommand', command.__class__.__name__ )

#   def test_init( self ):
#     """ tests that the init method
#     """

#     command = GOCDBSyncCommand()
#     self.assertEqual( {}, command.apis )

#     command = GOCDBSyncCommand( clients = {'GOCDBClient': self.mock_GOCDBClient} )
#     self.assertEqual( {'onlyCache': False}, command.args )
#     self.assertEqual( {'GOCDBClient': self.mock_GOCDBClient}, command.apis )


#   def test_doNew( self ):
#     """ tests the doNew method
#     """

#     command = GOCDBSyncCommand()
#     command.rmClient = mock.MagicMock()
#     command.gClient = mock.MagicMock()

#     now = datetime.utcnow()
#     resFromDB = {'OK':True,
#                  'Value':( (now - timedelta( hours = 2 ),
#                            'dummy.host1.dummy',
#                            'https://a1.domain',
#                            now + timedelta( hours = 3 ),
#                            'dummy.host.dummy',
#                            now - timedelta( hours = 2 ),
#                            'maintenance',
#                            'OUTAGE',
#                            now,
#                            'Resource',
#                            'APEL'
#                           ),
#                           (now - timedelta( hours = 2 ),
#                            'dummy.host2.dummy',
#                            'https://a2.domain',
#                            now + timedelta( hours = 3 ),
#                            'dummy.host2.dummy',
#                            now - timedelta( hours = 2 ),
#                            'maintenance',
#                            'OUTAGE',
#                            now,
#                            'Resource',
#                            'CREAM'
#                           )
#                  ),
#                  'Columns': ['StartDate','DowntimeID', 'Link','EndDate', 'Name', 'DateEffective', 'Description',
#                              'Severity','LastCheckTime', 'Element', 'GOCDBServiceType']}

#     command.rmClient.selectDowntimeCache.return_value = resFromDB

#     resFromGOCDBclient = { 'OK': True, 'Value':
#                       '''<?xml version="1.0" encoding="UTF-8"?>
#                       <results>
#                         <DOWNTIME ID="dummy.host1.dummy" PRIMARY_KEY="dummy.host1.dummy" CLASSIFICATION="SCHEDULED">
#                           <PRIMARY_KEY>dummy.host1.dummy</PRIMARY_KEY>
#                           <HOSTNAME>dummy.host1.dummy</HOSTNAME>
#                           <SERVICE_TYPE>gLExec</SERVICE_TYPE>
#                           <ENDPOINT>dummy.host1.dummy</ENDPOINT>
#                           <HOSTED_BY>dummy.host1.dummy</HOSTED_BY>
#                           <GOCDB_PORTAL_URL>https://a1.domain</GOCDB_PORTAL_URL>
#                           <AFFECTED_ENDPOINTS/>
#                           <SEVERITY>OUTAGE</SEVERITY>
#                           <DESCRIPTION>Network connectivity problems</DESCRIPTION>
#                           <INSERT_DATE>1473460659</INSERT_DATE>
#                           <START_DATE>1473547200</START_DATE>
#                           <END_DATE>1473677747</END_DATE>
#                           <FORMATED_START_DATE>2016-09-10 22:40</FORMATED_START_DATE>
#                           <FORMATED_END_DATE>2016-09-12 10:55</FORMATED_END_DATE>
#                         </DOWNTIME>
#                       </results>''' }

#     command.gClient.getHostnameDowntime.return_value = resFromGOCDBclient

#     res = command.doNew()
#     self.assertEqual( False, res['OK'] )

#     res = command.doNew('dummy.host1.dummy')
#     print res
#     self.assertEqual( True, res['OK'] )
