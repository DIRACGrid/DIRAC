""" GOCDBClient module is a client for the GOC DB, looking for Downtimes.
"""
__RCSID__ = "$Id$"

import urllib2
import time
import socket

from datetime import datetime, timedelta
from xml.dom import minidom

from DIRAC import S_OK, S_ERROR, gLogger

def _parseSingleElement( element, attributes = None ):
  """
  Given a DOM Element, return a dictionary of its child elements and values (as strings).
  """

  handler = {}
  for child in element.childNodes:
    attrName = str( child.nodeName )
    if attributes is not None:
      if attrName not in attributes:
        continue
    try:
      nodeValue = child.childNodes[0].nodeValue
      attrValue = nodeValue.encode('utf-8')
    except IndexError:
      continue
    handler[attrName] = attrValue

  return handler

#############################################################################


class GOCDBClient( object ):
  """ Class for dealing with GOCDB. Class because of easier use from RSS
  """

#############################################################################

  def getStatus( self, granularity, name = None, startDate = None,
                 startingInHours = None, timeout = None ):
    """
    Return actual GOCDB status of entity in `name`

    :params:
      :attr:`granularity`: string: should be a ValidRes, e.g. "Resource"

      :attr:`name`: should be the name(s) of the ValidRes.
      Could be a list of basestring or simply one basestring.
      If not given, fetches the complete list.

      :attr:`startDate`: if not given, takes only ongoing DownTimes.
      if given, could be a datetime or a string ("YYYY-MM-DD"), and download
      DownTimes starting after that date.

      :attr:`startingInHours`: optional integer. If given, donwload
      DownTimes starting in the next given hours (startDate is then useless)

    :return: (example)
      {'OK': True,
       'Value': {'92569G0': {'DESCRIPTION': 'Annual site downtime for various major tasks in the area of network, storage, etc.',
                             'FORMATED_END_DATE': '2014-05-27 15:21',
                             'FORMATED_START_DATE': '2014-05-26 04:00',
                             'GOCDB_PORTAL_URL': 'https://goc.egi.eu/portal/index.php?Page_Type=Downtime&id=14051',
                             'HOSTED_BY': 'FZK-LCG2',
                             'HOSTNAME': 'lhcbsrm-kit.gridka.de',
                             'SERVICE_TYPE': 'SRM.nearline',
                             'SEVERITY': 'OUTAGE'},
                 '93293G0': {'DESCRIPTION': 'Maintenance on KIT campus border routers. In the unlikely event that redundancy should fail, FZK-LCG2 connection to the GPN will be down. LHCOPN/LHCONE will stay up.',
                             'FORMATED_END_DATE': '2014-07-12 14:00',
                             'FORMATED_START_DATE': '2014-07-12 06:00',
                             'GOCDB_PORTAL_URL': 'https://goc.egi.eu/portal/index.php?Page_Type=Downtime&id=14771',
                             'HOSTED_BY': 'FZK-LCG2',
                             'HOSTNAME': 'lhcbsrm-kit.gridka.de',
                             'SERVICE_TYPE': 'SRM.nearline',
                             'SEVERITY': 'WARNING'}
                 }
        }


    """

    startDate_STR = None
    startDateMax = None

    if startingInHours is not None:
      startDate = datetime.utcnow()
      startDateMax = startDate + timedelta( hours = startingInHours )

    if startDate is not None:
      if isinstance( startDate, basestring ):
        startDate_STR = startDate
        startDate = datetime( *time.strptime( startDate, "%Y-%m-%d" )[0:3] )
      elif isinstance( startDate, datetime ):
        startDate_STR = startDate.isoformat( ' ' )[0:10]

    if timeout is not None:
      socket.setdefaulttimeout( 10 )

    if startingInHours is not None:
    # make 2 queries and later merge the results

      # first call: pass the startDate argument as None,
      # so the curlDownload method will search for only ongoing DTs
      resXML_ongoing = self._downTimeCurlDownload( name )
      if resXML_ongoing is None:
        res_ongoing = {}
      else:
        res_ongoing = self._downTimeXMLParsing( resXML_ongoing, granularity, name )

      # second call: pass the startDate argument
      resXML_startDate = self._downTimeCurlDownload( name, startDate_STR )
      if resXML_startDate is None:
        res_startDate = {}
      else:
        res_startDate = self._downTimeXMLParsing( resXML_startDate, granularity,
                                                 name, startDateMax )

      # merge the results of the 2 queries:
      res = res_ongoing
      for k in res_startDate.keys():
        if k not in res.keys():
          res[k] = res_startDate[k]

    else:
      #just query for onGoing downtimes
      resXML = self._downTimeCurlDownload( name, startDate_STR )
      if resXML is None:
        return S_OK( None )

      res = self._downTimeXMLParsing( resXML, granularity, name, startDateMax )

    # Common: build URL
#    if res is None or res == []:
#      return S_OK(None)
#
#    self.buildURL(res)


    if res == {}:
      res = None

    return S_OK( res )


#############################################################################

  def getServiceEndpointInfo( self, granularity, entity ):
    """
    Get service endpoint info (in a dictionary)

    :params:
      :attr:`granularity` : a string. Could be in ('hostname', 'sitename', 'roc',
      'country', 'service_type', 'monitored')

      :attr:`entity` : a string. Actual name of the entity.
    """
    assert( type( granularity ) == str and type( entity ) == str )
    try:
      serviceXML = self._getServiceEndpointCurlDownload( granularity, entity )
      return S_OK( self._serviceEndpointXMLParsing( serviceXML ) )
    except Exception, e:
      _msg = 'Exception getting information for %s %s: %s' % ( granularity, entity, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

#############################################################################

#  def getSiteInfo(self, site):
#    """
#    Get site info (in a dictionary)
#
#    :params:
#      :attr:`entity` : a string. Actual name of the site.
#    """
#
#    siteXML = self._getSiteCurlDownload(site)
#    return S_OK(self._siteXMLParsing(siteXML))

#############################################################################

#  def buildURL(self, DTList):
#    '''build the URL relative to the DT '''
#    baseURL = "https://goc.egi.eu/downtime/list?id="
#    for dt in DTList:
#      id = str(dt['id'])
#      url = baseURL + id
#      dt['URL'] = url

#############################################################################

  def _downTimeCurlDownload( self, entity = None, startDate = None ):
    """ Download ongoing downtimes for entity using the GOC DB programmatic interface
    """

    #GOCDB-PI url and method settings
    #
    # Set the GOCDB URL
    gocdbpi_url = "https://goc.egi.eu/gocdbpi_v4/public/?method=get_downtime"
    # Set the desidered start date
    if startDate is None:
      when = "&ongoing_only=yes"
      gocdbpi_startDate = ""
    else:
      when = "&startdate="
      gocdbpi_startDate = startDate

    # GOCDB-PI to query
    gocdb_ep = gocdbpi_url
    if entity is not None:
      if isinstance( entity, basestring ):
        gocdb_ep = gocdb_ep + "&topentity=" + entity
    gocdb_ep = gocdb_ep + when + gocdbpi_startDate

    req = urllib2.Request( gocdb_ep )
    dtPage = urllib2.urlopen( req )

    dt = dtPage.read()

    return dt

#############################################################################

  def _getServiceEndpointCurlDownload( self, granularity, entity ):
    """
    Calls method `get_service_endpoint` from the GOC DB programmatic interface.

    :params:
      :attr:`granularity` : a string. Could be in ('hostname', 'sitename', 'roc',
      'country', 'service_type', 'monitored')

      :attr:`entity` : a string. Actual name of the entity.
    """
    if type( granularity ) != str or type( entity ) != str:
      raise ValueError( "Arguments must be strings." )

    # GOCDB-PI query
    gocdb_ep = "https://goc.egi.eu/gocdbpi_v4/public/?method=get_service_endpoint&" \
        + granularity + '=' + entity

    service_endpoint_page = urllib2.urlopen( gocdb_ep )

    return service_endpoint_page.read()

#############################################################################

#  def _getSiteCurlDownload(self, site):
#    """
#    Calls method `get_site` from the GOC DB programmatic interface.
#
#    :params:
#      :attr:`site` : a string. Actual name of the site.
#    """
#
#    # GOCDB-PI query
#    gocdb_ep = "https://goc.egi.eu/gocdbpi_v4/public/?method=get_site&sitename="+site
#
#    req = urllib2.Request(gocdb_ep)
#    site_page = urllib2.urlopen(req)
#
#    return site_page.read()

#############################################################################

  def _downTimeXMLParsing( self, dt, siteOrRes, entities = None, startDateMax = None ):
    """ Performs xml parsing from the dt string (returns a dictionary)
    """
    doc = minidom.parseString( dt )

    downtimeElements = doc.getElementsByTagName( "DOWNTIME" )
    dtDict = {}

    for dtElement in downtimeElements:
      elements = _parseSingleElement( dtElement, ['SEVERITY', 'SITENAME', 'HOSTNAME', 'ENDPOINT',
                                                  'HOSTED_BY', 'FORMATED_START_DATE',
                                                  'FORMATED_END_DATE', 'DESCRIPTION',
                                                  'GOCDB_PORTAL_URL', 'SERVICE_TYPE' ] )

      try:
        dtDict[ str( dtElement.getAttributeNode( "PRIMARY_KEY" ).nodeValue ) + ' ' + elements['ENDPOINT'] ] = elements
      except Exception:
        try:
          dtDict[ str( dtElement.getAttributeNode( "PRIMARY_KEY" ).nodeValue ) + ' ' + elements['HOSTNAME'] ] = elements
        except Exception:
          dtDict[ str( dtElement.getAttributeNode( "PRIMARY_KEY" ).nodeValue ) + ' ' + elements['SITENAME'] ] = elements

    for dt_ID in dtDict.keys():
      if siteOrRes in ( 'Site', 'Sites' ):
        if not ( 'SITENAME' in dtDict[dt_ID].keys() ):
          dtDict.pop( dt_ID )
          continue
        if entities is not None:
          if not isinstance( entities, list ):
            entities = [entities]
          if not ( dtDict[dt_ID]['SITENAME'] in entities ):
            dtDict.pop( dt_ID )

      elif siteOrRes in ( 'Resource', 'Resources' ):
        if not ( 'HOSTNAME' in dtDict[dt_ID].keys() ):
          dtDict.pop( dt_ID )
          continue
        if entities is not None:
          if not isinstance( entities, list ):
            entities = [entities]
          if not ( dtDict[dt_ID]['HOSTNAME'] in entities ):
            dtDict.pop( dt_ID )

    if startDateMax is not None:
      for dt_ID in dtDict.keys():
        startDateMaxFromKeys = datetime( *time.strptime( dtDict[dt_ID]['FORMATED_START_DATE'],
                                                        "%Y-%m-%d %H:%M" )[0:5] )
        if startDateMaxFromKeys > startDateMax:
          dtDict.pop( dt_ID )

    return dtDict

#############################################################################

  def _serviceEndpointXMLParsing( self, serviceXML ):
    """ Performs xml parsing from the service endpoint string
    Returns a list.
    """
    doc = minidom.parseString( serviceXML )
    services = doc.getElementsByTagName( "SERVICE_ENDPOINT" )
    services = [_parseSingleElement( s ) for s in services]
    return services
