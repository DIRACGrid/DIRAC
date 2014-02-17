# FIXME: to be fixed... does not work as of today

# import unittest
# from datetime import datetime, timedelta
# from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
# from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
# from DIRAC.Core.LCG.SLSClient import *
# from DIRAC.Core.LCG.SAMResultsClient import *
# from DIRAC.Core.LCG.GGUSTicketsClient import GGUSTicketsClient
# #from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
# #from DIRAC.ResourceStatusSystem.Utilities.Utils import *
#
# #############################################################################
#
# class ClientsTestCase(unittest.TestCase):
#   """ Base class for the clients test cases
#   """
#   def setUp(self):
#
#     from DIRAC.Core.Base.Script import parseCommandLine
#     parseCommandLine()
#
#     self.mockRSS = Mock()
#
#     self.GOCCli = GOCDBClient()
#     self.SLSCli = SLSClient()
#     self.SAMCli = SAMResultsClient()
#     self.GGUSCli = GGUSTicketsClient()
#
# #############################################################################
#
# class GOCDBClientSuccess(ClientsTestCase):
#
#   def test__downTimeXMLParsing(self):
#     now = datetime.utcnow().replace(microsecond = 0, second = 0)
#     tomorrow = datetime.utcnow().replace(microsecond = 0, second = 0) + timedelta(hours = 24)
#     inAWeek = datetime.utcnow().replace(microsecond = 0, second = 0) + timedelta(days = 7)
#
#     nowLess12h = str( now - timedelta(hours = 12) )[:-3]
#     nowPlus8h = str( now + timedelta(hours = 8) )[:-3]
#     nowPlus24h = str( now + timedelta(hours = 24) )[:-3]
#     nowPlus40h = str( now + timedelta(hours = 40) )[:-3]
#     nowPlus50h = str( now + timedelta(hours = 50) )[:-3]
#     nowPlus60h = str( now + timedelta(hours = 60) )[:-3]
#
#     XML_site_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowLess12h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus24h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#     XML_node_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME><HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowLess12h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus24h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#     XML_nodesite_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME><HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowLess12h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus8h+'</FORMATED_END_DATE></DOWNTIME><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowLess12h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus24h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#
#     XML_site_startingIn8h = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowPlus8h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus24h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#     XML_node_startingIn8h = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME><HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowPlus8h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus24h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#
#     XML_site_ongoing_and_site_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowLess12h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus8h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE 2</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowPlus24h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus40h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#
#     XML_site_startingIn24h_and_site_startingIn50h = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowPlus24h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus40h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowPlus50h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus60h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#
#     XML_site_ongoing_and_other_site_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowLess12h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus8h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>CERN-PROD</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE 2</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowPlus24h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus40h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#     XML_node_ongoing_and_other_node_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME><HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems RESOURCE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowLess12h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus8h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><HOSTNAME>ce112.cern.ch</HOSTNAME><HOSTED_BY>CERN-PROD</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems RESOURCE 2</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>'+nowPlus24h+'</FORMATED_START_DATE><FORMATED_END_DATE>'+nowPlus40h+'</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
#
#
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing, 'Site')
#     self.assertEqual(res.keys()[0], '28490G0 GRISU-ENEA-GRID')
#     self.assertEqual(res['28490G0 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID')
#
#     res = self.GOCCli._downTimeXMLParsing(XML_node_ongoing, 'Resource')
#     self.assertEqual(res.keys()[0], '28490G0 egse-cresco.portici.enea.it')
#     self.assertEqual(res['28490G0 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it')
#     self.assertEqual(res['28490G0 egse-cresco.portici.enea.it']['HOSTED_BY'], 'GRISU-ENEA-GRID')
#
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing, 'Resource')
#     self.assertEqual(res, {})
#     res = self.GOCCli._downTimeXMLParsing(XML_node_ongoing, 'Site')
#     self.assertEqual(res, {})
#
#     res = self.GOCCli._downTimeXMLParsing(XML_nodesite_ongoing, 'Site')
#     self.assertEquals(len(res), 1)
#     self.assertEqual(res.keys()[0], '28490G0 GRISU-ENEA-GRID')
#     self.assertEqual(res['28490G0 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID')
#
#     res = self.GOCCli._downTimeXMLParsing(XML_nodesite_ongoing, 'Resource')
#     self.assertEquals(len(res), 1)
#     self.assertEqual(res.keys()[0], '28490G0 egse-cresco.portici.enea.it')
#     self.assertEqual(res['28490G0 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it')
#
#     res = self.GOCCli._downTimeXMLParsing(XML_site_startingIn8h, 'Site', None, now)
#     self.assertEqual(res, {})
#     res = self.GOCCli._downTimeXMLParsing(XML_node_startingIn8h, 'Resource', None, now)
#     self.assertEqual(res, {})
#
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing_and_site_starting_in_24_hours, 'Site', None, now)
#     self.assertEqual(res.keys()[0], '28490G1 GRISU-ENEA-GRID')
#     self.assertEqual(res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID')
#
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing_and_site_starting_in_24_hours, 'Resource', None, now)
#     self.assertEqual(res, {})
#     res = self.GOCCli._downTimeXMLParsing(XML_site_startingIn24h_and_site_startingIn50h, 'Site', None, now)
#     self.assertEqual(res, {})
#
#     res = self.GOCCli._downTimeXMLParsing(XML_site_startingIn24h_and_site_startingIn50h, 'Site', None, tomorrow)
#     self.assertEqual(res.keys()[0], '28490G1 GRISU-ENEA-GRID')
#     self.assertEqual(res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID')
#
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', ['GRISU-ENEA-GRID'])
#     self.assertEqual(res.keys()[0], '28490G1 GRISU-ENEA-GRID')
#     self.assertEqual(res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID')
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', ['GRISU-ENEA-GRID', 'CERN-PROD'])
#     self.assert_('28490G1 GRISU-ENEA-GRID' in res.keys())
#     self.assert_('28490G0 CERN-PROD' in res.keys())
#     self.assertEqual(res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID')
#     self.assertEqual(res['28490G0 CERN-PROD']['SITENAME'], 'CERN-PROD')
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', 'CERN-PROD')
#     self.assertEqual(res.keys()[0], '28490G0 CERN-PROD')
#     self.assertEqual(res['28490G0 CERN-PROD']['SITENAME'], 'CERN-PROD')
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', 'CNAF-T1')
#     self.assertEqual(res, {})
#
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', ['GRISU-ENEA-GRID', 'CERN-PROD'], now)
#     self.assertEqual(res.keys()[0], '28490G1 GRISU-ENEA-GRID')
#     self.assertEqual(res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID')
#     res = self.GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', ['GRISU-ENEA-GRID', 'CERN-PROD'], inAWeek)
#     self.assertEqual(res.keys()[0], '28490G0 CERN-PROD')
#     self.assertEqual(res['28490G0 CERN-PROD']['SITENAME'], 'CERN-PROD')
#
#     res = self.GOCCli._downTimeXMLParsing(XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', ['egse-cresco.portici.enea.it'])
#     self.assertEqual(res.keys()[0], '28490G1 egse-cresco.portici.enea.it')
#     self.assertEqual(res['28490G1 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it')
#     res = self.GOCCli._downTimeXMLParsing(XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', ['egse-cresco.portici.enea.it', 'ce112.cern.ch'])
#     self.assert_('28490G1 egse-cresco.portici.enea.it' in res.keys())
#     self.assert_('28490G0 ce112.cern.ch' in res.keys())
#     self.assertEqual(res['28490G1 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it')
#     self.assertEqual(res['28490G0 ce112.cern.ch']['HOSTNAME'], 'ce112.cern.ch')
#     res = self.GOCCli._downTimeXMLParsing(XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', 'ce112.cern.ch')
#     self.assertEqual(res.keys()[0], '28490G0 ce112.cern.ch')
#     self.assertEqual(res['28490G0 ce112.cern.ch']['HOSTNAME'], 'ce112.cern.ch')
#     res = self.GOCCli._downTimeXMLParsing(XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', 'grid0.fe.infn.it')
#     self.assertEqual(res, {})
#
#     res = self.GOCCli._downTimeXMLParsing(XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', ['egse-cresco.portici.enea.it', 'ce112.cern.ch'], now)
#     self.assert_('28490G1 egse-cresco.portici.enea.it' in res.keys())
#     self.assertEqual(res['28490G1 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it')
#     res = self.GOCCli._downTimeXMLParsing(XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', ['egse-cresco.portici.enea.it', 'ce112.cern.ch'], inAWeek)
#     self.assertEqual(res.keys()[0], '28490G0 ce112.cern.ch')
#     self.assertEqual(res['28490G0 ce112.cern.ch']['HOSTNAME'], 'ce112.cern.ch')
#
#
#   def test_getStatus(self):
#     for granularity in ('Site', 'Resource'):
#       res = self.GOCCli.getStatus(granularity, 'XX')['Value']
#       self.assertEqual(res, None)
#       res = self.GOCCli.getStatus(granularity, 'XX', datetime.utcnow())['Value']
#       self.assertEqual(res, None)
#       res = self.GOCCli.getStatus(granularity, 'XX', datetime.utcnow(), 12)['Value']
#       self.assertEqual(res, None)
#
#     res = self.GOCCli.getStatus('Site', 'pic')['Value']
#     self.assertEqual(res, None)
#
#   def test_getServiceEndpointInfo(self):
#     for granularity in ('hostname', 'sitename', 'roc',
#                         'country', 'service_type', 'monitored'):
#       res = self.GOCCli.getServiceEndpointInfo(granularity, 'XX')['Value']
#       self.assertEqual(res, [])
#
# #############################################################################
#
# class SAMResultsClientSuccess(ClientsTestCase):
#
#   def test_getStatus(self):
#     res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA')['Value']
#     self.assertEqual(res, {'SS':'ok'})
#     res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA', ['ver'])['Value']
#     self.assertEqual(res, {'ver':'ok'})
#     res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA', ['LHCb CE-lhcb-os', 'PilotRole'])['Value']
#     self.assertEqual(res, {'PilotRole':'ok', 'LHCb CE-lhcb-os':'ok'})
#     res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA', ['wrong'])['Value']
#     self.assertEqual(res, None)
#     res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA', ['ver', 'wrong'])['Value']
#     self.assertEqual(res, {'ver':'ok'})
#     res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA')['Value']
#     self.assertEqual(res, {'SS':'ok'})
#
#     res = self.SAMCli.getStatus('Site', 'INFN-FERRARA')['Value']
#     self.assertEqual(res, {'SiteStatus':'ok'})
#
# #############################################################################
#
# #class SAMResultsClientFailure(ClientsTestCase):
# #
# #  def test_getStatus(self):
# #    self.failUnlessRaises(NoSAMTests, self.SAMCli.getStatus, 'Resource', 'XX', 'INFN-FERRARA')
#
# #############################################################################
#
# class SLSClientSuccess(ClientsTestCase):
#
#   def test_getAvailabilityStatus(self):
#     res = self.SLSCli.getAvailabilityStatus('RAL-LHCb_FAILOVER')['Value']
#     self.assertEqual(res, 100)
#
#   def test_getServiceInfo(self):
#     res = self.SLSCli.getServiceInfo('CASTORLHCB_LHCBMDST', ["Volume to be recallled GB"])['Value']
#     self.assertEqual(res["Volume to be recallled GB"], 0.0)
#
# #############################################################################
#
# #class SLSClientFailure(ClientsTestCase):
# #
# #  def test_getStatus(self):
# #    self.failUnlessRaises(NoServiceException, self.SLSCli.getAvailabilityStatus, 'XX')
#
# #############################################################################
#
# class GGUSTicketsClientSuccess(ClientsTestCase):
#
#   def test_getTicketsList(self):
#     res = self.GGUSCli.getTicketsList('INFN-CAGLIARI')['Value']
#     self.assertEqual(res[0]['open'], 0)
#
#
# #############################################################################
#
# if __name__ == '__main__':
#   suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBClientSuccess))
# #  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBClient_Failure))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResultsClientSuccess))
# #  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResultsClientFailure))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SLSClientSuccess))
# #  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SLSClientFailure))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTicketsClientSuccess))
#   testResult = unittest.TextTestRunner(verbosity=2).run(suite)
