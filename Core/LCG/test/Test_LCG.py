""" Few unit tests for LCG clients
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import mock

from datetime import datetime, timedelta
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
# #from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
# #from DIRAC.ResourceStatusSystem.Utilities.Utils import *
#
#############################################################################

__RCSID__ = "$Id$"


class ClientsTestCase( unittest.TestCase ):
  """ Base class for the clients test cases
  """
  def setUp( self ):

    self.mockRSS = mock.MagicMock()

    self.GOCCli = GOCDBClient()

# #############################################################################

class GOCDBClientSuccess( ClientsTestCase ):

  def test__downTimeXMLParsing( self ):
    now = datetime.utcnow().replace( microsecond = 0, second = 0 )
    tomorrow = datetime.utcnow().replace( microsecond = 0, second = 0 ) + timedelta( hours = 24 )
    inAWeek = datetime.utcnow().replace( microsecond = 0, second = 0 ) + timedelta( days = 7 )
    nowLess12h = str( now - timedelta( hours = 12 ) )[:-3]
    nowPlus8h = str( now + timedelta( hours = 8 ) )[:-3]
    nowPlus24h = str( now + timedelta( hours = 24 ) )[:-3]
    nowPlus40h = str( now + timedelta( hours = 40 ) )[:-3]
    nowPlus50h = str( now + timedelta( hours = 50 ) )[:-3]
    nowPlus60h = str( now + timedelta( hours = 60 ) )[:-3]

    XML_site_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowLess12h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus24h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
    XML_node_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME><HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowLess12h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus24h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
    XML_nodesite_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME><HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowLess12h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus8h + '</FORMATED_END_DATE></DOWNTIME><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowLess12h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus24h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'

    XML_site_startingIn8h = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowPlus8h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus24h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
    XML_node_startingIn8h = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME><HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowPlus8h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus24h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'

    XML_site_ongoing_and_site_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowLess12h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus8h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE 2</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowPlus24h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus40h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'

    XML_site_startingIn24h_and_site_startingIn50h = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowPlus24h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus40h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowPlus50h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus60h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'

    XML_site_ongoing_and_other_site_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowLess12h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus8h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><SITENAME>CERN-PROD</SITENAME><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems SITE 2</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowPlus24h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus40h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'
    XML_node_ongoing_and_other_node_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME><HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems RESOURCE</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowLess12h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus8h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0" CLASSIFICATION="SCHEDULED"><HOSTNAME>ce112.cern.ch</HOSTNAME><HOSTED_BY>CERN-PROD</HOSTED_BY><SEVERITY>OUTAGE</SEVERITY><DESCRIPTION>Software problems RESOURCE 2</DESCRIPTION><INSERT_DATE>1276273965</INSERT_DATE><START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE><FORMATED_START_DATE>' + nowPlus24h + '</FORMATED_START_DATE><FORMATED_END_DATE>' + nowPlus40h + '</FORMATED_END_DATE><GOCDB_PORTAL_URL>https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&amp;object_id=18509&amp;grid_id=0</GOCDB_PORTAL_URL></DOWNTIME></ROOT>\n'


    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing, 'Site' )
    self.assertEqual( res.keys()[0], '28490G0 GRISU-ENEA-GRID' )
    self.assertEqual( res['28490G0 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID' )

    res = self.GOCCli._downTimeXMLParsing( XML_node_ongoing, 'Resource' )
    self.assertEqual( res.keys()[0], '28490G0 egse-cresco.portici.enea.it' )
    self.assertEqual( res['28490G0 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it' )
    self.assertEqual( res['28490G0 egse-cresco.portici.enea.it']['HOSTED_BY'], 'GRISU-ENEA-GRID' )

    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing, 'Resource' )
    self.assertEqual( res, {} )
    res = self.GOCCli._downTimeXMLParsing( XML_node_ongoing, 'Site' )
    self.assertEqual( res, {} )

    res = self.GOCCli._downTimeXMLParsing( XML_nodesite_ongoing, 'Site' )
    self.assertEqual( len( res ), 1 )
    self.assertEqual( res.keys()[0], '28490G0 GRISU-ENEA-GRID' )
    self.assertEqual( res['28490G0 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID' )

    res = self.GOCCli._downTimeXMLParsing( XML_nodesite_ongoing, 'Resource' )
    self.assertEqual( len( res ), 1 )
    self.assertEqual( res.keys()[0], '28490G0 egse-cresco.portici.enea.it' )
    self.assertEqual( res['28490G0 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it' )

    res = self.GOCCli._downTimeXMLParsing( XML_site_startingIn8h, 'Site', None, now )
    self.assertEqual( res, {} )
    res = self.GOCCli._downTimeXMLParsing( XML_node_startingIn8h, 'Resource', None, now )
    self.assertEqual( res, {} )

    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing_and_site_starting_in_24_hours, 'Site', None, now )
    self.assertEqual( res.keys()[0], '28490G1 GRISU-ENEA-GRID' )
    self.assertEqual( res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID' )

    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing_and_site_starting_in_24_hours, 'Resource', None, now )
    self.assertEqual( res, {} )
    res = self.GOCCli._downTimeXMLParsing( XML_site_startingIn24h_and_site_startingIn50h, 'Site', None, now )
    self.assertEqual( res, {} )

    res = self.GOCCli._downTimeXMLParsing( XML_site_startingIn24h_and_site_startingIn50h, 'Site', None, tomorrow )
    self.assertEqual( res.keys()[0], '28490G1 GRISU-ENEA-GRID' )
    self.assertEqual( res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID' )

    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', ['GRISU-ENEA-GRID'] )
    self.assertEqual( res.keys()[0], '28490G1 GRISU-ENEA-GRID' )
    self.assertEqual( res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID' )
    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing_and_other_site_starting_in_24_hours,
                                          'Site', ['GRISU-ENEA-GRID', 'CERN-PROD'] )
    self.assertTrue( '28490G1 GRISU-ENEA-GRID' in res.keys() )
    self.assertTrue( '28490G0 CERN-PROD' in res.keys() )
    self.assertEqual( res['28490G1 GRISU-ENEA-GRID']['SITENAME'], 'GRISU-ENEA-GRID' )
    self.assertEqual( res['28490G0 CERN-PROD']['SITENAME'], 'CERN-PROD' )
    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', 'CERN-PROD' )
    self.assertEqual( res.keys()[0], '28490G0 CERN-PROD' )
    self.assertEqual( res['28490G0 CERN-PROD']['SITENAME'], 'CERN-PROD' )
    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing_and_other_site_starting_in_24_hours, 'Site', 'CNAF-T1' )
    self.assertEqual( res, {} )

    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing_and_other_site_starting_in_24_hours,
                                           'Site', ['GRISU-ENEA-GRID', 'CERN-PROD'], now )
    self.assertEqual( res.keys()[0], '28490G1 GRISU-ENEA-GRID' ), self.assertEqual( res['28490G1 GRISU-ENEA-GRID']['SITENAME'],
                                                                                    'GRISU-ENEA-GRID' )
    res = self.GOCCli._downTimeXMLParsing( XML_site_ongoing_and_other_site_starting_in_24_hours,
    'Site', ['GRISU-ENEA-GRID', 'CERN-PROD'], inAWeek )
    self.assertEqual( res.keys()[0], '28490G0 CERN-PROD' )
    self.assertEqual( res['28490G0 CERN-PROD']['SITENAME'], 'CERN-PROD' )

    res = self.GOCCli._downTimeXMLParsing( XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource',
                                          ['egse-cresco.portici.enea.it'] )
    self.assertEqual( res.keys()[0], '28490G1 egse-cresco.portici.enea.it' )
    self.assertEqual( res['28490G1 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it' )
    res = self.GOCCli._downTimeXMLParsing( XML_node_ongoing_and_other_node_starting_in_24_hours,
                                           'Resource', ['egse-cresco.portici.enea.it', 'ce112.cern.ch'] )
    self.assertTrue( '28490G1 egse-cresco.portici.enea.it' in res.keys() )
    self.assertTrue( '28490G0 ce112.cern.ch' in res.keys() )
    self.assertEqual( res['28490G1 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it' )
    self.assertEqual( res['28490G0 ce112.cern.ch']['HOSTNAME'], 'ce112.cern.ch' )
    res = self.GOCCli._downTimeXMLParsing( XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', 'ce112.cern.ch' )
    self.assertEqual( res.keys()[0], '28490G0 ce112.cern.ch' )
    self.assertEqual( res['28490G0 ce112.cern.ch']['HOSTNAME'], 'ce112.cern.ch' )
    res = self.GOCCli._downTimeXMLParsing( XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', 'grid0.fe.infn.it' )
    self.assertEqual( res, {} )

    res = self.GOCCli._downTimeXMLParsing( XML_node_ongoing_and_other_node_starting_in_24_hours, 'Resource', ['egse-cresco.portici.enea.it', 'ce112.cern.ch'], now )
    self.assertTrue( '28490G1 egse-cresco.portici.enea.it' in res.keys() )
    self.assertEqual( res['28490G1 egse-cresco.portici.enea.it']['HOSTNAME'], 'egse-cresco.portici.enea.it' )
    res = self.GOCCli._downTimeXMLParsing( XML_node_ongoing_and_other_node_starting_in_24_hours,
                                           'Resource', ['egse-cresco.portici.enea.it', 'ce112.cern.ch'], inAWeek )
    self.assertEqual( res.keys()[0], '28490G0 ce112.cern.ch' )
    self.assertEqual( res['28490G0 ce112.cern.ch']['HOSTNAME'], 'ce112.cern.ch' )

  def test_getServiceEndpointInfo( self ):
    for granularity in ( 'hostname', 'sitename', 'roc',
                        'country', 'service_type', 'monitored' ):
      res = self.GOCCli.getServiceEndpointInfo( granularity, 'XX' )
      if res['OK']:
        self.assertTrue( isinstance(res['Value'],list) )


# #############################################################################
#
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GOCDBClientSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
