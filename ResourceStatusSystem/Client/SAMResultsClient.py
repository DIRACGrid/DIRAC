""" SAMResultsClient class is a client for the SAM Results DB.
"""

import urllib2
from datetime import datetime
from xml.dom import minidom

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class SAMResultsClient:
  
#############################################################################

  def getStatus(self, args):
    """  
    Return stats of entity in args
    
    :params:
      :attr:`args`: a tuple

        - args[0] should be the name of the site

        - args[1] should be the name of the resource

    :returns:
      {
        'Status': ok|down|na|degraded|partial|maint'
      }
    """
#    print args
    
    sam = self._curlDownload(getSiteRealName(args[0]))
    if sam == None:
      return {'Status':'na'}
    res = self._xmlParsing(sam, args[1])
    
    return res
    
#############################################################################

  def _curlDownload(self, site):
    """ Download SAM status for entity using the SAM DB programmatic interface
    """

    samdbpi_url = "http://lcg-sam.cern.ch:8080/same-pi/"
    # Set your method
    samdbpi_method = "service_endpoint_status.jsp?"
    # Set your site
    samdbpi_site = site
    # Set your service type
    #samdbpi_service = service
     
    # SAMDB-PI to query
    samdb_ep = samdbpi_url + samdbpi_method + "VO_name=LHCb" + "&Site_name=" + samdbpi_site + "&only_ss"

    opener = urllib2.build_opener()
    try:
      samPage = opener.open(samdb_ep)
    except IOError, errorMsg:
      exceptStr = where(self, self._curlDownload) + " while opening %s." % samdb_ep
      gLogger.exception(exceptStr,'',errorMsg)
      return None

    sam = samPage.read()
    
    opener.close()

    return sam
    
#############################################################################

  def _xmlParsing(self, sam, entity):
    """ Performs xml parsing from the sam string 
        Returns a dictionary containing status of entity
    """

    try:
      doc = minidom.parseString(sam)
    
      samRes = doc.getElementsByTagName("Service") 
      
      toCheck = None
      for sam in samRes:
        if sam.getAttributeNode("endpoint"):
          endpoint = sam.attributes["endpoint"]
          res = str(endpoint.value)
          if res == entity:
            toCheck = sam
            break
      
      if toCheck is None:
        return {'Status':'na'}
      
      s = toCheck.getElementsByTagName("status")[0].childNodes
      
      status = str(s[0].nodeValue)
    
    except Exception, errorMsg:
#      exceptStr = where(self, self._xmlParsing)
#      gLogger.exception(exceptStr,'',errorMsg)
      return {'Status':'na'}
    
    return {'Status':status}
    
    
#############################################################################
