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

  def getStatus(self, granularity, name, siteName = None, tests = None):
#TO CHANGE
    """  
    Return stats of entity in args
    
    :params:
      :attr:`site`: string: the name of the site
      
      :attr:`resource`: string: the name of the resource
      
      :attr:`tests`: optional (list of) tests. 
      If omitted, takes only the service status metrics

    :returns:
      {
        'SAM-Status': {'SS'|'js'...:ok|down|na|degraded|partial|maint}'
      }
    """

    if granularity in ('Site', 'Sites'):
      siteName = getSiteRealName(name)
    elif granularity in ('Resource', 'Resources'):
      if siteName is None:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
        rsc = ResourceStatusClient()
        siteName = rsc.getGeneralName(granularity, name, 'Site')
        if siteName is None or siteName == []:
          gLogger.info('%s is not a resource in DIRAC' %name)
          return {'SAM-Status':None}
        siteName = getSiteRealName(siteName)
      else:
        siteName = getSiteRealName(siteName)
    else:
      raise InvalidStatus, where(self, self.getStatus)
    
    sam = self._curlDownload(granularity, site = siteName, tests = tests)
    
    if sam is None:
      return {'SAM-Status':None}
    
    samStatus = self._xmlParsing(granularity, sam, name, tests)
    
    if samStatus is None:
      return {'SAM-Status':None} 
    
    return {'SAM-Status': samStatus}
    
#############################################################################

  def _curlDownload(self, granularity, site, tests):
    """ Download SAM status for entity using the SAM DB programmatic interface
    """

    samdbpi_url = "http://lcg-sam.cern.ch:8080/same-pi/"
    # Set your method
    if granularity in ('Site', 'Sites'):
      samdbpi_method = "site_status.jsp?"
    elif granularity in ('Resource', 'Resources'):
      samdbpi_method = "service_endpoint_status.jsp?"
    # Set your site
    samdbpi_site = site
    # set test
    samdbpi_test = ""
    if tests is None:
      samdbpi_test = "&only_ss"
      
    # Set your service type
    #samdbpi_service = service
     
    # SAMDB-PI to query
    samdb_ep = samdbpi_url + samdbpi_method + "VO_name=LHCb" + "&Site_name=" + samdbpi_site + samdbpi_test
    
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

  def _xmlParsing(self, granularity, sam, entity, tests):
    """ Performs xml parsing from the sam string 
        Returns a dictionary containing status of entity
    """

    status = {}

    try:
      doc = minidom.parseString(sam)
    
      if granularity in ('Site', 'Sites'):
          s = doc.getElementsByTagName("status")[0].childNodes
          status['SiteStatus'] = str(s[0].nodeValue)
        
      elif granularity in ('Resource', 'Resources'):
        
        services = doc.getElementsByTagName("Service") 
        
        serviceToCheck = None
        for service in services:
          if service.getAttributeNode("endpoint"):
            endpoint = service.attributes["endpoint"]
            res = str(endpoint.value)
            if res == entity:
              serviceToCheck = service
              break
        
        if serviceToCheck is None:
          return None
        
        
        if tests is None:
          s = serviceToCheck.getElementsByTagName("status")[0].childNodes
          status['SS'] = str(s[0].nodeValue)
          
        else:
          
          for test in tests:
            
            metrics = serviceToCheck.getElementsByTagName("ServiceMetric")
            metricToCheck = None
            
            for metric in metrics:
              if metric.getAttributeNode("abbreviation"):
                metricName = metric.attributes["abbreviation"]
                res = str(metricName.value)
                if res == test:
                  metricToCheck = metric
                  break
  
            s = metricToCheck.getElementsByTagName("status")[0].childNodes
            status[test] = str(s[0].nodeValue)
      
    
    except Exception, errorMsg:
#      exceptStr = where(self, self._xmlParsing)
#      gLogger.exception(exceptStr,'',errorMsg)
#      return {'Status':'na'}
      return None
    
    return status
    
    
#############################################################################