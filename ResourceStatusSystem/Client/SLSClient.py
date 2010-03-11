""" SLSClient class is a client for the SLS DB, looking for Status of a given Service.
"""

import urllib2
from DIRAC import gLogger

class SLSClient:

#############################################################################

  def getStatus(self, name):
    """  
    Return actual SLS status of entity in args[0]
     
    :params:
      :attr:`name`: string - name of the service

    returns:
    {
      'SLS':availability
    }

    """
    
    res = self._read_from_url(name)

    if "ERROR" in res:
      return {'SLS':None, 'Reason': res}
    
    return {'SLS':int(res)}
  
#############################################################################
  
  def _read_from_url(self, service):
    #for more information like space occupancy we have to overload this method.

    """ download from SLS PI the value of the availability as returned for
        the service  
    """

    # Set the SLS URL
    sls_base = "http://sls.cern.ch/sls/getServiceAvailability.php?id="
    sls_url= sls_base+service
    opener = urllib2.build_opener()
    try:
      sls_page = opener.open(sls_url)
    except IOError, errorMsg:
      exceptStr = where(self, self._curlDownload) + " while opening %s." % sls_url
      gLogger.exception(exceptStr,'',errorMsg)

    sls_res = sls_page.read()
    
    opener.close()

    return sls_res
    

#############################################################################
