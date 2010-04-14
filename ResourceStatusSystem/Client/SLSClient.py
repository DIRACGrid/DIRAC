""" SLSClient class is a client for the SLS DB, looking for Status of a given Service.
"""

import socket
import urllib2

class SLSClient:

#############################################################################

  def getInfo(self, name):
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
    
    status = self.getStatus(name)
    status['Weblink'] = self.getLink(name)['WebLink']
    
    return status

#############################################################################

  def getStatus(self, name):
    """  
    Return actual SLS status of entity in name
     
    :params:
      :attr:`name`: string - name of the service

    returns:
    {
      'SLS':availability
    }

    """
    
    res = self._read_from_url(name)

    if "ERROR: Couldn't find service" in res:
      raise NoServiceException
    elif "ERROR:" in res:
      raise Exception
    
    return int(res)
  
#############################################################################
  

  def getLink(self, name):

    return 'https://sls.cern.ch/sls/service.php?id='
  
#############################################################################
 
  def _read_from_url(self, service):
    #for more information like space occupancy we have to overload this method.

    """ download from SLS PI the value of the availability as returned for
        the service  
    """

    socket.setdefaulttimeout(10)
    
    # Set the SLS URL
    sls_base = "http://sls.cern.ch/sls/getServiceAvailability.php?id="
    sls_url= sls_base+service

    req = urllib2.Request(sls_url)
    slsPage = urllib2.urlopen(req)

    sls_res = slsPage.read()

    return sls_res
    

#############################################################################

class NoServiceException(Exception):
  
  def __init__(self, message = ""):
    self.message = message
    Exception.__init__(self, message)
  
  def __str__(self):
    return "The service is not instrumented with SLS sensors \n" + repr(self.message)
  
#############################################################################
