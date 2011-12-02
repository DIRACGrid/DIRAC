"""
SLSClient class is a client for the SLS DB, looking for Status of a given Service.
"""

import socket
import urllib2
from xml.dom import minidom

from DIRAC import S_OK, S_ERROR

def getAvailabilityStatus( sls_id, timeout = None ):
  """
  Return actual SLS availability status of entity in sls_id

  :params:
  :attr:`sls_id`: string - sls_id of the service
  """
  socket.setdefaulttimeout( timeout )
  try:
    res = urllib2.urlopen("http://sls.cern.ch/sls/getServiceAvailability.php?id=" + sls_id).read()
  except urllib2.URLError as exc:
    return S_ERROR(str(exc))

  if "ERROR: Couldn't find service" in res:
    return S_ERROR( "The service is not monitored with SLS" )
  elif "ERROR:" in res:
    return S_ERROR("Unknown SLS error")
  else:
    return S_OK(int(res))

def getInfo( sls_id, timeout = None ):
  """
  Use getStatus to return actual SLS status of entity in sls_id
  and return link to SLS page

  :params:
  :attr:`sls_id`: string - sls_id of the service

  returns:
  {
  'SLS':availability
  'WebLink':link
  }

  """
  status = getAvailabilityStatus( sls_id, timeout )
  status['Weblink'] = 'https://sls.cern.ch/sls/service.php?id=' + sls_id

  return status # Already a S_OK/S_ERROR value.

def getServiceInfo( sls_id, timeout = None ):
  """
  Return actual SLS "additional service information" as a dict

  :params:
  :attr:`sls_id` : string - sls_id of the service
  """
  socket.setdefaulttimeout( timeout )
  try:
    sls = urllib2.urlopen("http://sls.cern.ch/sls/update/" + sls_id + '.xml')
    doc = minidom.parse( sls )
    numericValues = doc.getElementsByTagName( "numericvalue" )
  except Exception as exc:
    return S_ERROR(str(exc))


  return S_OK(dict([(nv.getAttribute("name"), float(nv.firstChild.nodeValue)) for nv in numericValues]))
