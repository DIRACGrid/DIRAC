""" SLSClient class is a client for the SLS DB, looking for Status of a given Service.
"""

import urllib2
#from datetime import datetime
#from xml.dom import minidom

from DIRAC import gLogger
#from Exceptions import *
#from Utils import *


class SLSClient:
#############################################################################

  def getStatus(self, arg1):
    """  return actual SLS status of entity in args[0]
        - args[0] should be a ValidRes

        returns:
        {
        'Availability':availability
        }

    """
    res = self._read_from_url(arg1)
    return res
  

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
      dtPage = opener.open(sls_url)
    except IOError, errorMsg:
      exceptStr = where(self, self._curlDownload) + " while opening %s." % sls_url
      gLogger.exception(exceptStr,'',errorMsg)

    dt = dtPage.read()
    
    opener.close()

    return dt
    

