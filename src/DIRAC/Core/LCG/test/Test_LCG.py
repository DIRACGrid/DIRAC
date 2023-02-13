""" Few unit tests for LCG clients
"""
from unittest import mock

from datetime import datetime, timedelta
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient


mockRSS = mock.MagicMock()
GOCCli = GOCDBClient()

# data

now = datetime.utcnow().replace(microsecond=0, second=0)
tomorrow = datetime.utcnow().replace(microsecond=0, second=0) + timedelta(hours=24)
inAWeek = datetime.utcnow().replace(microsecond=0, second=0) + timedelta(days=7)
nowLess12h = str(now - timedelta(hours=12))[:-3]
nowPlus8h = str(now + timedelta(hours=8))[:-3]
nowPlus24h = str(now + timedelta(hours=24))[:-3]
nowPlus40h = str(now + timedelta(hours=40))[:-3]
nowPlus50h = str(now + timedelta(hours=50))[:-3]
nowPlus60h = str(now + timedelta(hours=60))[:-3]

XML_site_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0"'
XML_site_ongoing += ' CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME>'
XML_site_ongoing += "<SEVERITY>OUTAGE</SEVERITY>"
XML_site_ongoing += "<DESCRIPTION>Software problems SITE</DESCRIPTION>"
XML_site_ongoing += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_site_ongoing += "<START_DATE>1276360500</START_DATE>"
XML_site_ongoing += "<END_DATE>1276878660</END_DATE>"
XML_site_ongoing += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
XML_site_ongoing += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
XML_site_ongoing += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_site_ongoing += "</DOWNTIME></ROOT>\n"

XML_node_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0"'
XML_node_ongoing += ' CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME>'
XML_node_ongoing += "<HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY>"
XML_node_ongoing += "<SEVERITY>OUTAGE</SEVERITY>"
XML_node_ongoing += "<DESCRIPTION>Software problems</DESCRIPTION>"
XML_node_ongoing += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_node_ongoing += "<START_DATE>1276360500</START_DATE><END_DATE>1276878660</END_DATE>"
XML_node_ongoing += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
XML_node_ongoing += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
XML_node_ongoing += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_node_ongoing += "</DOWNTIME></ROOT>\n"

xml_endpoint_and_affected_ongoing = '<?xml version="1.0"?>\n<ROOT>'
xml_endpoint_and_affected_ongoing += '<DOWNTIME ID="29118" PRIMARY_KEY="109962G0" CLASSIFICATION="UNSCHEDULED">'
xml_endpoint_and_affected_ongoing += "<PRIMARY_KEY>109962G0</PRIMARY_KEY>"
xml_endpoint_and_affected_ongoing += "<HOSTNAME>lhcbsrm-kit.gridka.de</HOSTNAME>"
xml_endpoint_and_affected_ongoing += "<SERVICE_TYPE>SRM</SERVICE_TYPE>"
xml_endpoint_and_affected_ongoing += "<ENDPOINT>lhcbsrm-kit.gridka.deSRM</ENDPOINT>"
xml_endpoint_and_affected_ongoing += "<HOSTED_BY>FZK-LCG2</HOSTED_BY>"
xml_endpoint_and_affected_ongoing += "<GOCDB_PORTAL_URL>https://goc.egi.eu/bof</GOCDB_PORTAL_URL>"
xml_endpoint_and_affected_ongoing += "<AFFECTED_ENDPOINTS>"
xml_endpoint_and_affected_ongoing += "<ENDPOINT>"
xml_endpoint_and_affected_ongoing += "<ID>7517</ID>"
xml_endpoint_and_affected_ongoing += "<NAME>lhcbsrm-disk-kit</NAME>"
xml_endpoint_and_affected_ongoing += "<URL>lhcbsrm-disk-kit.gridka.de</URL>"
xml_endpoint_and_affected_ongoing += "<INTERFACENAME>SRM</INTERFACENAME>"
xml_endpoint_and_affected_ongoing += "<ENDPOINT_MONITORED>N</ENDPOINT_MONITORED>"
xml_endpoint_and_affected_ongoing += "</ENDPOINT>"
xml_endpoint_and_affected_ongoing += "</AFFECTED_ENDPOINTS>"
xml_endpoint_and_affected_ongoing += "<SEVERITY>OUTAGE</SEVERITY>"
xml_endpoint_and_affected_ongoing += "<DESCRIPTION>Namespace reordering</DESCRIPTION>"
xml_endpoint_and_affected_ongoing += "<INSERT_DATE>1595233003</INSERT_DATE>"
xml_endpoint_and_affected_ongoing += "<START_DATE>1595314800</START_DATE>"
xml_endpoint_and_affected_ongoing += "<END_DATE>1595343600</END_DATE>"
xml_endpoint_and_affected_ongoing += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
xml_endpoint_and_affected_ongoing += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
xml_endpoint_and_affected_ongoing += "</DOWNTIME></ROOT>\n"

xml_endpoint_and_affected_ongoing_diffURL = '<?xml version="1.0"?>\n<ROOT>'
xml_endpoint_and_affected_ongoing_diffURL += '<DOWNTIME ID="29118" PRIMARY_KEY="109962G0" CLASSIFICATION="UNSCHEDULED">'
xml_endpoint_and_affected_ongoing_diffURL += "<PRIMARY_KEY>109962G0</PRIMARY_KEY>"
xml_endpoint_and_affected_ongoing_diffURL += "<HOSTNAME>lhcbsrm-kit.gridka.de</HOSTNAME>"
xml_endpoint_and_affected_ongoing_diffURL += "<SERVICE_TYPE>SRM</SERVICE_TYPE>"
xml_endpoint_and_affected_ongoing_diffURL += "<ENDPOINT>lhcbsrm-kit.gridka.deSRM</ENDPOINT>"
xml_endpoint_and_affected_ongoing_diffURL += "<HOSTED_BY>FZK-LCG2</HOSTED_BY>"
xml_endpoint_and_affected_ongoing_diffURL += "<GOCDB_PORTAL_URL>https://goc.egi.eu/bof</GOCDB_PORTAL_URL>"
xml_endpoint_and_affected_ongoing_diffURL += "<AFFECTED_ENDPOINTS>"
xml_endpoint_and_affected_ongoing_diffURL += "<ENDPOINT>"
xml_endpoint_and_affected_ongoing_diffURL += "<ID>7517</ID>"
xml_endpoint_and_affected_ongoing_diffURL += "<NAME>lhcbsrm-disk-kit</NAME>"
xml_endpoint_and_affected_ongoing_diffURL += "<URL>https://lhcbsrm-disk-kit.gridka.de:123</URL>"
xml_endpoint_and_affected_ongoing_diffURL += "<INTERFACENAME>SRM</INTERFACENAME>"
xml_endpoint_and_affected_ongoing_diffURL += "<ENDPOINT_MONITORED>N</ENDPOINT_MONITORED>"
xml_endpoint_and_affected_ongoing_diffURL += "</ENDPOINT>"
xml_endpoint_and_affected_ongoing_diffURL += "</AFFECTED_ENDPOINTS>"
xml_endpoint_and_affected_ongoing_diffURL += "<SEVERITY>OUTAGE</SEVERITY>"
xml_endpoint_and_affected_ongoing_diffURL += "<DESCRIPTION>Namespace reordering</DESCRIPTION>"
xml_endpoint_and_affected_ongoing_diffURL += "<INSERT_DATE>1595233003</INSERT_DATE>"
xml_endpoint_and_affected_ongoing_diffURL += "<START_DATE>1595314800</START_DATE>"
xml_endpoint_and_affected_ongoing_diffURL += "<END_DATE>1595343600</END_DATE>"
xml_endpoint_and_affected_ongoing_diffURL += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
xml_endpoint_and_affected_ongoing_diffURL += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
xml_endpoint_and_affected_ongoing_diffURL += "</DOWNTIME></ROOT>\n"

xml_endpoint_and_affected_ongoing_broken = '<?xml version="1.0"?>\n<ROOT>'
xml_endpoint_and_affected_ongoing_broken += '<DOWNTIME ID="29118" PRIMARY_KEY="109962G0" CLASSIFICATION="UNSCHEDULED">'
xml_endpoint_and_affected_ongoing_broken += "<PRIMARY_KEY>109962G0</PRIMARY_KEY>"
xml_endpoint_and_affected_ongoing_broken += "<HOSTNAME>lhcbsrm-kit.gridka.de</HOSTNAME>"
xml_endpoint_and_affected_ongoing_broken += "<SERVICE_TYPE>SRM</SERVICE_TYPE>"
xml_endpoint_and_affected_ongoing_broken += "<ENDPOINT>lhcbsrm-kit.gridka.deSRM</ENDPOINT>"
xml_endpoint_and_affected_ongoing_broken += "<HOSTED_BY>FZK-LCG2</HOSTED_BY>"
xml_endpoint_and_affected_ongoing_broken += "<GOCDB_PORTAL_URL>https://goc.egi.eu/bof</GOCDB_PORTAL_URL>"
xml_endpoint_and_affected_ongoing_broken += "<AFFECTED_ENDPOINTS>"
xml_endpoint_and_affected_ongoing_broken += "<ENDPOINT>"
xml_endpoint_and_affected_ongoing_broken += "<ID>7517</ID>"
xml_endpoint_and_affected_ongoing_broken += "<NAME>lhcbsrm-disk-kit</NAME>"
xml_endpoint_and_affected_ongoing_broken += "<INTERFACENAME>SRM</INTERFACENAME>"
xml_endpoint_and_affected_ongoing_broken += "<ENDPOINT_MONITORED>N</ENDPOINT_MONITORED>"
xml_endpoint_and_affected_ongoing_broken += "</ENDPOINT>"
xml_endpoint_and_affected_ongoing_broken += "</AFFECTED_ENDPOINTS>"
xml_endpoint_and_affected_ongoing_broken += "<SEVERITY>OUTAGE</SEVERITY>"
xml_endpoint_and_affected_ongoing_broken += "<DESCRIPTION>Namespace reordering</DESCRIPTION>"
xml_endpoint_and_affected_ongoing_broken += "<INSERT_DATE>1595233003</INSERT_DATE>"
xml_endpoint_and_affected_ongoing_broken += "<START_DATE>1595314800</START_DATE>"
xml_endpoint_and_affected_ongoing_broken += "<END_DATE>1595343600</END_DATE>"
xml_endpoint_and_affected_ongoing_broken += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
xml_endpoint_and_affected_ongoing_broken += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
xml_endpoint_and_affected_ongoing_broken += "</DOWNTIME></ROOT>\n"

xml_endpoint_and_affected_ongoing_no_url = '<?xml version="1.0"?>\n<ROOT>'
xml_endpoint_and_affected_ongoing_no_url += '<DOWNTIME ID="29118" PRIMARY_KEY="109962G0" CLASSIFICATION="UNSCHEDULED">'
xml_endpoint_and_affected_ongoing_no_url += "<PRIMARY_KEY>109962G0</PRIMARY_KEY>"
xml_endpoint_and_affected_ongoing_no_url += "<HOSTNAME>lhcbsrm-kit.gridka.de</HOSTNAME>"
xml_endpoint_and_affected_ongoing_no_url += "<SERVICE_TYPE>SRM</SERVICE_TYPE>"
xml_endpoint_and_affected_ongoing_no_url += "<ENDPOINT>lhcbsrm-kit.gridka.deSRM</ENDPOINT>"
xml_endpoint_and_affected_ongoing_no_url += "<HOSTED_BY>FZK-LCG2</HOSTED_BY>"
xml_endpoint_and_affected_ongoing_no_url += "<GOCDB_PORTAL_URL>https://goc.egi.eu/bof</GOCDB_PORTAL_URL>"
xml_endpoint_and_affected_ongoing_no_url += "<AFFECTED_ENDPOINTS/>"
xml_endpoint_and_affected_ongoing_no_url += "<SEVERITY>OUTAGE</SEVERITY>"
xml_endpoint_and_affected_ongoing_no_url += "<DESCRIPTION>Namespace reordering</DESCRIPTION>"
xml_endpoint_and_affected_ongoing_no_url += "<INSERT_DATE>1595233003</INSERT_DATE>"
xml_endpoint_and_affected_ongoing_no_url += "<START_DATE>1595314800</START_DATE>"
xml_endpoint_and_affected_ongoing_no_url += "<END_DATE>1595343600</END_DATE>"
xml_endpoint_and_affected_ongoing_no_url += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
xml_endpoint_and_affected_ongoing_no_url += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
xml_endpoint_and_affected_ongoing_no_url += "</DOWNTIME></ROOT>\n"


xml_endpoint_and_affected_ongoing_2_endpoints = '<?xml version="1.0"?>\n<ROOT>'
xml_endpoint_and_affected_ongoing_2_endpoints += '<DOWNTIME ID="29109" PRIMARY_KEY="123" CLASSIFICATION="SCHEDULED">'
xml_endpoint_and_affected_ongoing_2_endpoints += "<PRIMARY_KEY>123</PRIMARY_KEY>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<HOSTNAME>appsgrycap.i3m.upv.es</HOSTNAME>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<SERVICE_TYPE>es.upv.grycap.im</SERVICE_TYPE>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<ENDPOINT>appsgrycap.i3m.upv.eses.upv.grycap.im</ENDPOINT>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<HOSTED_BY>UPV-GRyCAP</HOSTED_BY>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<GOCDB_PORTAL_URL>https://goc.egi.eu/bof</GOCDB_PORTAL_URL>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<AFFECTED_ENDPOINTS>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<ENDPOINT>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<ID>6735</ID>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<NAME>IM Web portal</NAME>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<URL>https://appsgrycap.i3m.upv.es:31443/im-web/</URL>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<INTERFACENAME>es.upv.grycap.im</INTERFACENAME>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<ENDPOINT_MONITORED>N</ENDPOINT_MONITORED>"
xml_endpoint_and_affected_ongoing_2_endpoints += "</ENDPOINT>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<ENDPOINT>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<ID>6737</ID>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<NAME>IM REST API</NAME>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<URL>https://appsgrycap.i3m.upv.es:31443/im/</URL>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<INTERFACENAME>es.upv.grycap.im</INTERFACENAME>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<ENDPOINT_MONITORED>N</ENDPOINT_MONITORED>"
xml_endpoint_and_affected_ongoing_2_endpoints += "</ENDPOINT>"
xml_endpoint_and_affected_ongoing_2_endpoints += "</AFFECTED_ENDPOINTS>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<SEVERITY>OUTAGE</SEVERITY>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<DESCRIPTION>maintenance of the facilities</DESCRIPTION>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<INSERT_DATE>1594986508</INSERT_DATE>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<START_DATE>1596196800</START_DATE>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<END_DATE>1598961600</END_DATE>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
xml_endpoint_and_affected_ongoing_2_endpoints += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
xml_endpoint_and_affected_ongoing_2_endpoints += "</DOWNTIME></ROOT>\n"

XML_nodesite_ongoing = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0"'
XML_nodesite_ongoing += ' CLASSIFICATION="SCHEDULED"><HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME>'
XML_nodesite_ongoing += "<HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY>"
XML_nodesite_ongoing += "<SEVERITY>OUTAGE</SEVERITY>"
XML_nodesite_ongoing += "<DESCRIPTION>Software problems</DESCRIPTION>"
XML_nodesite_ongoing += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_nodesite_ongoing += "<START_DATE>1276360500</START_DATE>"
XML_nodesite_ongoing += "<END_DATE>1276878660</END_DATE>"
XML_nodesite_ongoing += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
XML_nodesite_ongoing += "<FORMATED_END_DATE>" + nowPlus8h + "</FORMATED_END_DATE>"
XML_nodesite_ongoing += '</DOWNTIME><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0"'
XML_nodesite_ongoing += ' CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME>'
XML_nodesite_ongoing += "<SEVERITY>OUTAGE</SEVERITY>"
XML_nodesite_ongoing += "<DESCRIPTION>Software problems SITE</DESCRIPTION>"
XML_nodesite_ongoing += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_nodesite_ongoing += "<START_DATE>1276360500</START_DATE>"
XML_nodesite_ongoing += "<END_DATE>1276878660</END_DATE>"
XML_nodesite_ongoing += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
XML_nodesite_ongoing += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
XML_nodesite_ongoing += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_nodesite_ongoing += "</DOWNTIME></ROOT>\n"

XML_site_startingIn8h = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456" PRIMARY_KEY="28490G0"'
XML_site_startingIn8h += ' CLASSIFICATION="SCHEDULED"><SITENAME>GRISU-ENEA-GRID</SITENAME>'
XML_site_startingIn8h += "<SEVERITY>OUTAGE</SEVERITY>"
XML_site_startingIn8h += "<DESCRIPTION>Software problems SITE</DESCRIPTION>"
XML_site_startingIn8h += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_site_startingIn8h += "<START_DATE>1276360500</START_DATE>"
XML_site_startingIn8h += "<END_DATE>1276878660</END_DATE>"
XML_site_startingIn8h += "<FORMATED_START_DATE>" + nowPlus8h + "</FORMATED_START_DATE>"
XML_site_startingIn8h += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
XML_site_startingIn8h += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_site_startingIn8h += "</DOWNTIME></ROOT>\n"

XML_node_startingIn8h = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505455" PRIMARY_KEY="28490G0"'
XML_node_startingIn8h += ' CLASSIFICATION="SCHEDULED">'
XML_node_startingIn8h += "<HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME>"
XML_node_startingIn8h += "<HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY>"
XML_node_startingIn8h += "<SEVERITY>OUTAGE</SEVERITY>"
XML_node_startingIn8h += "<DESCRIPTION>Software problems</DESCRIPTION>"
XML_node_startingIn8h += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_node_startingIn8h += "<START_DATE>1276360500</START_DATE>"
XML_node_startingIn8h += "<END_DATE>1276878660</END_DATE>"
XML_node_startingIn8h += "<FORMATED_START_DATE>" + nowPlus8h + "</FORMATED_START_DATE>"
XML_node_startingIn8h += "<FORMATED_END_DATE>" + nowPlus24h + "</FORMATED_END_DATE>"
XML_node_startingIn8h += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_node_startingIn8h += "</DOWNTIME></ROOT>\n"

XML_site_ongoing_and_site_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT><DOWNTIME ID="78505456"'
XML_site_ongoing_and_site_starting_in_24_hours += ' PRIMARY_KEY="28490G1" CLASSIFICATION="SCHEDULED">'
XML_site_ongoing_and_site_starting_in_24_hours += "<SITENAME>GRISU-ENEA-GRID</SITENAME>"
XML_site_ongoing_and_site_starting_in_24_hours += "<SEVERITY>OUTAGE</SEVERITY>"
XML_site_ongoing_and_site_starting_in_24_hours += "<DESCRIPTION>Software problems SITE</DESCRIPTION>"
XML_site_ongoing_and_site_starting_in_24_hours += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<START_DATE>1276360500</START_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<END_DATE>1276878660</END_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<FORMATED_END_DATE>" + nowPlus8h + "</FORMATED_END_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_site_ongoing_and_site_starting_in_24_hours += "</DOWNTIME>"
XML_site_ongoing_and_site_starting_in_24_hours += '<DOWNTIME ID="78505457" PRIMARY_KEY="28490G0"'
XML_site_ongoing_and_site_starting_in_24_hours += ' CLASSIFICATION="SCHEDULED">'
XML_site_ongoing_and_site_starting_in_24_hours += "<SITENAME>GRISU-ENEA-GRID</SITENAME>"
XML_site_ongoing_and_site_starting_in_24_hours += "<SEVERITY>OUTAGE</SEVERITY>"
XML_site_ongoing_and_site_starting_in_24_hours += "<DESCRIPTION>Software problems SITE 2</DESCRIPTION>"
XML_site_ongoing_and_site_starting_in_24_hours += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<START_DATE>1276360500</START_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<END_DATE>1276878660</END_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<FORMATED_START_DATE>" + nowPlus24h + "</FORMATED_START_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<FORMATED_END_DATE>" + nowPlus40h + "</FORMATED_END_DATE>"
XML_site_ongoing_and_site_starting_in_24_hours += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_site_ongoing_and_site_starting_in_24_hours += "</DOWNTIME></ROOT>\n"

XML_site_startingIn24h_and_site_startingIn50h = '<?xml version="1.0"?>\n<ROOT>'
XML_site_startingIn24h_and_site_startingIn50h += '<DOWNTIME ID="78505456" PRIMARY_KEY="28490G1"'
XML_site_startingIn24h_and_site_startingIn50h += ' CLASSIFICATION="SCHEDULED">'
XML_site_startingIn24h_and_site_startingIn50h += "<SITENAME>GRISU-ENEA-GRID</SITENAME>"
XML_site_startingIn24h_and_site_startingIn50h += "<SEVERITY>OUTAGE</SEVERITY>"
XML_site_startingIn24h_and_site_startingIn50h += "<DESCRIPTION>Software problems SITE</DESCRIPTION>"
XML_site_startingIn24h_and_site_startingIn50h += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<START_DATE>1276360500</START_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<END_DATE>1276878660</END_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<FORMATED_START_DATE>" + nowPlus24h + "</FORMATED_START_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<FORMATED_END_DATE>" + nowPlus40h + "</FORMATED_END_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_site_startingIn24h_and_site_startingIn50h += '</DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0"'
XML_site_startingIn24h_and_site_startingIn50h += ' CLASSIFICATION="SCHEDULED">'
XML_site_startingIn24h_and_site_startingIn50h += "<SITENAME>GRISU-ENEA-GRID</SITENAME>"
XML_site_startingIn24h_and_site_startingIn50h += "<SEVERITY>OUTAGE</SEVERITY>"
XML_site_startingIn24h_and_site_startingIn50h += "<DESCRIPTION>Software problems SITE</DESCRIPTION>"
XML_site_startingIn24h_and_site_startingIn50h += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<START_DATE>1276360500</START_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<END_DATE>1276878660</END_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<FORMATED_START_DATE>" + nowPlus50h + "</FORMATED_START_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<FORMATED_END_DATE>" + nowPlus60h + "</FORMATED_END_DATE>"
XML_site_startingIn24h_and_site_startingIn50h += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_site_startingIn24h_and_site_startingIn50h += "</DOWNTIME></ROOT>\n"

XML_site_ongoing_and_other_site_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT>'
XML_site_ongoing_and_other_site_starting_in_24_hours += '<DOWNTIME ID="78505456" PRIMARY_KEY="28490G1"'
XML_site_ongoing_and_other_site_starting_in_24_hours += ' CLASSIFICATION="SCHEDULED">'
XML_site_ongoing_and_other_site_starting_in_24_hours += "<SITENAME>GRISU-ENEA-GRID</SITENAME>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<SEVERITY>OUTAGE</SEVERITY>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<DESCRIPTION>Software problems SITE</DESCRIPTION>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<START_DATE>1276360500</START_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<END_DATE>1276878660</END_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<FORMATED_END_DATE>" + nowPlus8h + "</FORMATED_END_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "</DOWNTIME>"
XML_site_ongoing_and_other_site_starting_in_24_hours += '<DOWNTIME ID="78505457" PRIMARY_KEY="28490G0"'
XML_site_ongoing_and_other_site_starting_in_24_hours += ' CLASSIFICATION="SCHEDULED">'
XML_site_ongoing_and_other_site_starting_in_24_hours += "<SITENAME>CERN-PROD</SITENAME>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<SEVERITY>OUTAGE</SEVERITY>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<DESCRIPTION>Software problems SITE 2</DESCRIPTION>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<START_DATE>1276360500</START_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<END_DATE>1276878660</END_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<FORMATED_START_DATE>" + nowPlus24h + "</FORMATED_START_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<FORMATED_END_DATE>" + nowPlus40h + "</FORMATED_END_DATE>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_site_ongoing_and_other_site_starting_in_24_hours += "</DOWNTIME></ROOT>\n"

XML_node_ongoing_and_other_node_starting_in_24_hours = '<?xml version="1.0"?>\n<ROOT>'
XML_node_ongoing_and_other_node_starting_in_24_hours += '<DOWNTIME ID="78505456" PRIMARY_KEY="28490G1"'
XML_node_ongoing_and_other_node_starting_in_24_hours += ' CLASSIFICATION="SCHEDULED">'
XML_node_ongoing_and_other_node_starting_in_24_hours += "<HOSTNAME>egse-cresco.portici.enea.it</HOSTNAME>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<HOSTED_BY>GRISU-ENEA-GRID</HOSTED_BY>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<SEVERITY>OUTAGE</SEVERITY>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<DESCRIPTION>Software problems RESOURCE</DESCRIPTION>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<START_DATE>1276360500</START_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<END_DATE>1276878660</END_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<FORMATED_START_DATE>" + nowLess12h + "</FORMATED_START_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<FORMATED_END_DATE>" + nowPlus8h + "</FORMATED_END_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_node_ongoing_and_other_node_starting_in_24_hours += '</DOWNTIME><DOWNTIME ID="78505457" PRIMARY_KEY="28490G0"'
XML_node_ongoing_and_other_node_starting_in_24_hours += ' CLASSIFICATION="SCHEDULED">'
XML_node_ongoing_and_other_node_starting_in_24_hours += "<HOSTNAME>ce112.cern.ch</HOSTNAME>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<HOSTED_BY>CERN-PROD</HOSTED_BY>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<SEVERITY>OUTAGE</SEVERITY>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<DESCRIPTION>Software problems RESOURCE 2</DESCRIPTION>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<INSERT_DATE>1276273965</INSERT_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<START_DATE>1276360500</START_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<END_DATE>1276878660</END_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<FORMATED_START_DATE>" + nowPlus24h + "</FORMATED_START_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<FORMATED_END_DATE>" + nowPlus40h + "</FORMATED_END_DATE>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "<GOCDB_PORTAL_URL>https://bof</GOCDB_PORTAL_URL>"
XML_node_ongoing_and_other_node_starting_in_24_hours += "</DOWNTIME></ROOT>\n"


# test


def test__downTimeXMLParsing_affected():

    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing, "Resource")
    assert set(res) == {"109962G0 lhcbsrm-kit.gridka.deSRM"}
    assert res["109962G0 lhcbsrm-kit.gridka.deSRM"]["HOSTNAME"] == "lhcbsrm-kit.gridka.de"
    assert res["109962G0 lhcbsrm-kit.gridka.deSRM"]["URL"] == "lhcbsrm-disk-kit.gridka.de"
    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing, "Site")
    assert res == {}

    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing_diffURL, "Resource")
    assert set(res) == {"109962G0 lhcbsrm-kit.gridka.deSRM"}
    assert res["109962G0 lhcbsrm-kit.gridka.deSRM"]["HOSTNAME"] == "lhcbsrm-kit.gridka.de"
    assert res["109962G0 lhcbsrm-kit.gridka.deSRM"]["URL"] == "lhcbsrm-disk-kit.gridka.de"
    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing_diffURL, "Site")
    assert res == {}

    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing_broken, "Resource")
    assert list(res)[0] == "109962G0 lhcbsrm-kit.gridka.deSRM"
    assert res["109962G0 lhcbsrm-kit.gridka.deSRM"]["HOSTNAME"] == "lhcbsrm-kit.gridka.de"
    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing_broken, "Site")
    assert res == {}

    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing_no_url, "Resource")
    assert list(res)[0] == "109962G0 lhcbsrm-kit.gridka.deSRM"
    assert res["109962G0 lhcbsrm-kit.gridka.deSRM"]["HOSTNAME"] == "lhcbsrm-kit.gridka.de"
    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing_no_url, "Site")
    assert res == {}

    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing_2_endpoints, "Resource")
    assert list(res)[0] == "123 appsgrycap.i3m.upv.eses.upv.grycap.im"
    assert res["123 appsgrycap.i3m.upv.eses.upv.grycap.im"]["HOSTNAME"] == "appsgrycap.i3m.upv.es"
    assert res["123 appsgrycap.i3m.upv.eses.upv.grycap.im"]["URL"] == "appsgrycap.i3m.upv.es"
    res = GOCCli._downTimeXMLParsing(xml_endpoint_and_affected_ongoing_2_endpoints, "Site")
    assert res == {}


def test__downTimeXMLParsing():

    res = GOCCli._downTimeXMLParsing(XML_site_ongoing, "Site")
    assert set(res) == {"28490G0 GRISU-ENEA-GRID"}
    assert res["28490G0 GRISU-ENEA-GRID"]["SITENAME"] == "GRISU-ENEA-GRID"

    res = GOCCli._downTimeXMLParsing(XML_node_ongoing, "Resource")
    assert set(res) == {"28490G0 egse-cresco.portici.enea.it"}
    assert res["28490G0 egse-cresco.portici.enea.it"]["HOSTNAME"] == "egse-cresco.portici.enea.it"
    assert res["28490G0 egse-cresco.portici.enea.it"]["HOSTED_BY"] == "GRISU-ENEA-GRID"

    res = GOCCli._downTimeXMLParsing(XML_site_ongoing, "Resource")
    assert res == {}
    res = GOCCli._downTimeXMLParsing(XML_node_ongoing, "Site")
    assert res == {}

    res = GOCCli._downTimeXMLParsing(XML_nodesite_ongoing, "Site")
    assert len(res) == 1
    assert set(res) == {"28490G0 GRISU-ENEA-GRID"}
    assert res["28490G0 GRISU-ENEA-GRID"]["SITENAME"] == "GRISU-ENEA-GRID"

    res = GOCCli._downTimeXMLParsing(XML_nodesite_ongoing, "Resource")
    assert len(res) == 1
    assert set(res) == {"28490G0 egse-cresco.portici.enea.it"}
    assert res["28490G0 egse-cresco.portici.enea.it"]["HOSTNAME"] == "egse-cresco.portici.enea.it"

    res = GOCCli._downTimeXMLParsing(XML_site_startingIn8h, "Site", None, now)
    assert res == {}
    res = GOCCli._downTimeXMLParsing(XML_node_startingIn8h, "Resource", None, now)
    assert res == {}

    res = GOCCli._downTimeXMLParsing(XML_site_ongoing_and_site_starting_in_24_hours, "Site", None, now)
    assert set(res) == {"28490G1 GRISU-ENEA-GRID"}
    assert res["28490G1 GRISU-ENEA-GRID"]["SITENAME"] == "GRISU-ENEA-GRID"

    res = GOCCli._downTimeXMLParsing(XML_site_ongoing_and_site_starting_in_24_hours, "Resource", None, now)
    assert res == {}
    res = GOCCli._downTimeXMLParsing(XML_site_startingIn24h_and_site_startingIn50h, "Site", None, now)
    assert res == {}

    res = GOCCli._downTimeXMLParsing(XML_site_startingIn24h_and_site_startingIn50h, "Site", None, tomorrow)
    assert set(res) == {"28490G1 GRISU-ENEA-GRID"}
    assert res["28490G1 GRISU-ENEA-GRID"]["SITENAME"] == "GRISU-ENEA-GRID"

    res = GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, "Site", ["GRISU-ENEA-GRID"])
    assert list(res)[0], "28490G1 GRISU-ENEA-GRID"
    assert res["28490G1 GRISU-ENEA-GRID"]["SITENAME"], "GRISU-ENEA-GRID"
    res = GOCCli._downTimeXMLParsing(
        XML_site_ongoing_and_other_site_starting_in_24_hours, "Site", ["GRISU-ENEA-GRID", "CERN-PROD"]
    )
    assert "28490G1 GRISU-ENEA-GRID" in res
    assert "28490G0 CERN-PROD" in res
    assert res["28490G1 GRISU-ENEA-GRID"]["SITENAME"] == "GRISU-ENEA-GRID"
    assert res["28490G0 CERN-PROD"]["SITENAME"] == "CERN-PROD"
    res = GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, "Site", "CERN-PROD")
    assert set(res) == {"28490G0 CERN-PROD"}
    assert res["28490G0 CERN-PROD"]["SITENAME"] == "CERN-PROD"
    res = GOCCli._downTimeXMLParsing(XML_site_ongoing_and_other_site_starting_in_24_hours, "Site", "CNAF-T1")
    assert res == {}

    res = GOCCli._downTimeXMLParsing(
        XML_site_ongoing_and_other_site_starting_in_24_hours, "Site", ["GRISU-ENEA-GRID", "CERN-PROD"], now
    )
    assert set(res) == {"28490G1 GRISU-ENEA-GRID"}
    assert res["28490G1 GRISU-ENEA-GRID"]["SITENAME"] == "GRISU-ENEA-GRID"
    res = GOCCli._downTimeXMLParsing(
        XML_site_ongoing_and_other_site_starting_in_24_hours, "Site", ["GRISU-ENEA-GRID", "CERN-PROD"], inAWeek
    )
    assert set(res) == {"28490G0 CERN-PROD", "28490G1 GRISU-ENEA-GRID"}
    assert res["28490G0 CERN-PROD"]["SITENAME"] == "CERN-PROD"

    res = GOCCli._downTimeXMLParsing(
        XML_node_ongoing_and_other_node_starting_in_24_hours, "Resource", ["egse-cresco.portici.enea.it"]
    )
    assert set(res) == {"28490G1 egse-cresco.portici.enea.it"}
    assert res["28490G1 egse-cresco.portici.enea.it"]["HOSTNAME"] == "egse-cresco.portici.enea.it"
    res = GOCCli._downTimeXMLParsing(
        XML_node_ongoing_and_other_node_starting_in_24_hours,
        "Resource",
        ["egse-cresco.portici.enea.it", "ce112.cern.ch"],
    )
    assert "28490G1 egse-cresco.portici.enea.it" in res
    assert "28490G0 ce112.cern.ch" in res
    assert res["28490G1 egse-cresco.portici.enea.it"]["HOSTNAME"] == "egse-cresco.portici.enea.it"
    assert res["28490G0 ce112.cern.ch"]["HOSTNAME"] == "ce112.cern.ch"
    res = GOCCli._downTimeXMLParsing(XML_node_ongoing_and_other_node_starting_in_24_hours, "Resource", "ce112.cern.ch")
    assert set(res) == {"28490G0 ce112.cern.ch"}
    assert res["28490G0 ce112.cern.ch"]["HOSTNAME"] == "ce112.cern.ch"
    res = GOCCli._downTimeXMLParsing(
        XML_node_ongoing_and_other_node_starting_in_24_hours, "Resource", "grid0.fe.infn.it"
    )
    assert res == {}

    res = GOCCli._downTimeXMLParsing(
        XML_node_ongoing_and_other_node_starting_in_24_hours,
        "Resource",
        ["egse-cresco.portici.enea.it", "ce112.cern.ch"],
        now,
    )
    assert "28490G1 egse-cresco.portici.enea.it" in res
    assert res["28490G1 egse-cresco.portici.enea.it"]["HOSTNAME"] == "egse-cresco.portici.enea.it"
    res = GOCCli._downTimeXMLParsing(
        XML_node_ongoing_and_other_node_starting_in_24_hours,
        "Resource",
        ["egse-cresco.portici.enea.it", "ce112.cern.ch"],
        inAWeek,
    )
    assert set(res) == {"28490G1 egse-cresco.portici.enea.it", "28490G0 ce112.cern.ch"}
    assert res["28490G0 ce112.cern.ch"]["HOSTNAME"] == "ce112.cern.ch"
