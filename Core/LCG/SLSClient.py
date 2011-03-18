# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/DISET/private/Transports/SSL/ThreadSafeSSLObject.py $
""" SLSClient class is a client for the SLS DB, looking for Status of a given Service.
"""
__RCSID__ = "$Id: ThreadSafeSSLObject.py 18161 2009-11-11 12:07:09Z acasajus $"

import socket
import urllib2
from xml.dom import minidom

from DIRAC import S_OK, S_ERROR

class SLSClient:
  # FIXME: Why is this a class and not just few methods?

#############################################################################

  def getInfo( self, name, timeout = None ):
    """  
    Use getStatus to return actual SLS status of entity in name 
    and return link to SLS page
     
    :params:
      :attr:`name`: string - name of the service

    returns:
    {
      'SLS':availability
      'WebLink':link
    }

    """

    status = self.getAvailabilityStatus( name, timeout )
    status['Weblink'] = self.getLink( name )['WebLink']

    return S_OK( status )

#############################################################################

  def getAvailabilityStatus( self, name, timeout = None ):
    """  
    Return actual SLS availability status of entity in name
     
    :params:
      :attr:`name`: string - name of the service
    """

    res = self._read_availability_from_url( name, timeout )

    if "ERROR: Couldn't find service" in res:
      return S_ERROR( "The service is not instrumented with SLS sensors" )
#      raise NoServiceException
    elif "ERROR:" in res:
#      return S_ERROR("ERROR with SLS sensors : %s" %res)
      raise Exception, "ERROR with SLS sensors : %s" % res
    elif res == -1:
      raise Exception, "ERROR with SLS sensors : %s" % res

    return S_OK( int( res ) )

#############################################################################

  def getServiceInfo( self, name, infos, timeout = None ):
    """  
    Return actual SLS "additional service information"
     
    :params:
      :attr:`name` : string - name of the service
      
      :attr:`infos` : list - list of info names

    returns:
    {
      'info_name_1':info_1, 'info_name_2':info_2, 'info_name_3':info_3, ...   
    }

    """

    sls = self._urlDownload( name, timeout )

    res = self._xmlParsing( sls, infos )

    return S_OK( res )

#############################################################################

  def getLink( self, name ):

    return S_OK( 'https://sls.cern.ch/sls/service.php?id=' + name )

#############################################################################

  def _read_availability_from_url( self, service, timeout = None ):
    """ download from SLS PI the value of the availability as returned for
        the service  
    """

    if timeout is not None:
      socket.setdefaulttimeout( timeout )

    # Set the SLS URL
    sls_base = "http://sls.cern.ch/sls/getServiceAvailability.php?id="
    sls_url = sls_base + service

    req = urllib2.Request( sls_url )
    slsPage = urllib2.urlopen( req )

    sls_res = slsPage.read()

    return sls_res


#############################################################################

  def _urlDownload( self, service, timeout = None ):
    """ download from SLS the XML of info regarding service
    """

    if timeout is not None:
      socket.setdefaulttimeout( timeout )

    # Set the SLS URL
    sls_base = "http://sls.cern.ch/sls/update/"
    sls_url = sls_base + service + '.xml'

    req = urllib2.Request( sls_url )
    slsPage = urllib2.urlopen( req )

    sls_res = slsPage.read()

    return sls_res

#############################################################################

  def _xmlParsing( self, sls, infos ):
    """ Performs xml parsing from the sls string 
        Returns a dictionary containing infos
    """

    status = {}

    doc = minidom.parseString( sls )
    numericValues = doc.getElementsByTagName( "numericvalue" )

    for info in infos:

      infoToCheck = None

      for nv in numericValues:
        if nv.getAttributeNode( "name" ):
          nv_name = nv.attributes["name"]
          res = str( nv_name.value )
          if res == info:
            infoToCheck = nv
            break

      if infoToCheck is None:
        return S_ERROR( "The service is not instrumented with SLS sensors" )
#        raise NoServiceException

      res = infoToCheck.childNodes[0].nodeValue.strip()
      status[info] = float( res )

    return status


#############################################################################

#class NoServiceException(Exception):
#  
#  def __init__(self, message = ""):
#    self.message = message
#    Exception.__init__(self, message)
#  
#  def __str__(self):
#    return "The service is not instrumented with SLS sensors \n" + repr(self.message)

#############################################################################
