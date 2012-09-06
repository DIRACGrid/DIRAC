# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/DISET/private/Transports/SSL/ThreadSafeSSLObject.py $
""" SAMResultsClient class is a client for the SAM Results DB.
"""
__RCSID__ = "$Id: ThreadSafeSSLObject.py 18161 2009-11-11 12:07:09Z acasajus $"
# it crashes epydoc
#__docformat__ = "restructuredtext en"

import urllib2
from datetime import datetime
from xml.dom import minidom
import socket

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals

class SAMResultsClient:
  # FIXME: Why is this a class and not just few methods?

#############################################################################

  def getStatus( self, granularity, name, siteName, tests = None, timeout = None ):
    """
    Return stats of entity in args

    :params:
      :attr:`granularity`: string: 'Site'  or 'Resource'

      :attr:`name`: string: the name of the site or of the resource

      :attr:`siteName`: string for the sitename

      :attr:`tests`: optional (list of) tests.
      If omitted, takes only the service status metrics

      :attr:`timeout`: optional timeout.
      If omitted, there will be no timeout.

    :returns:
      {
        'SAM-Status': {'SS'|'js'...:ok|down|na|degraded|partial|maint}'
      }
    """

    if granularity in ( 'Site', 'Sites' ):
      siteName = name
    elif granularity in ( 'Resource', 'Resources' ):
      siteName = siteName

    if timeout is not None:
      socket.setdefaulttimeout( timeout )

    sam = self._curlDownload( granularity, site = siteName, tests = tests )

    if sam is None:
      return S_OK( None )

    samStatus = self._xmlParsing( granularity, sam, name, tests )

    if samStatus is None or samStatus == {}:
      return S_OK( None )

    return S_OK( samStatus )

#############################################################################

  def _curlDownload( self, granularity, site, tests ):
    """ Download SAM status for entity using the SAM DB programmatic interface
    """

    samdbpi_url = "http://lcg-sam.cern.ch:8080/same-pi/"
    # Set your method
    if granularity in ( 'Site', 'Sites' ):
      samdbpi_method = "site_status.jsp?"
    elif granularity in ( 'Resource', 'Resources' ):
      samdbpi_method = "service_endpoint_status.jsp?"
    # Set your site
    samdbpi_site = site
    # set test
    samdbpi_test = ""
    if tests is None:
      samdbpi_test = "&only_ss"

    extension = CSGlobals.getCSExtensions()[0]

    samdb_ep = samdbpi_url + samdbpi_method + "VO_name=" + extension + "&Site_name=" + samdbpi_site + samdbpi_test

    req = urllib2.Request( samdb_ep )
    samPage = urllib2.urlopen( req )

    sam = samPage.read()

    return sam

#############################################################################

  def _xmlParsing( self, granularity, sam, entity, tests ):
    """ Performs xml parsing from the sam string
        Returns a dictionary containing status of entity
    """

    status = {}

    doc = minidom.parseString( sam )

    if granularity in ( 'Site', 'Sites' ):
      try:
        state = doc.getElementsByTagName( "status" )[0].childNodes
        status['SiteStatus'] = str( state[0].nodeValue )
      except IndexError:
        return None

    elif granularity in ( 'Resource', 'Resources' ):

      services = doc.getElementsByTagName( "Service" )

      serviceToCheck = None
      for service in services:
        if service.getAttributeNode( "endpoint" ):
          endpoint = service.attributes["endpoint"]
          res = str( endpoint.value )
          if res == entity:
            serviceToCheck = service
            break

      if serviceToCheck is None:
        return S_ERROR( "There are no SAM tests for this service" )

      if tests is None or tests == []:
        tests = ['SS']

      for test in tests:

        metrics = serviceToCheck.getElementsByTagName( "ServiceMetric" )
        metricToCheck = None

        for metric in metrics:
          if metric.getAttributeNode( "abbreviation" ):
            metricName = metric.attributes["abbreviation"]
            res = str( metricName.value )
            if res == test:
              metricToCheck = metric
              break

        if metricToCheck is None:
          continue

        state = metricToCheck.getElementsByTagName( "status" )[0].childNodes
        status[test] = str( state[0].nodeValue )

    return status

#############################################################################
