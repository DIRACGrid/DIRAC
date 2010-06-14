""" SAMResultsClient class is a client for the SAM Results DB.
"""

import urllib2
from datetime import datetime
from xml.dom import minidom
import socket

from DIRAC import S_OK, S_ERROR

class SAMResultsClient:
  
#############################################################################

  def getStatus(self, granularity, name, siteName = None, 
                tests = None, timeout = None):
    """  
    Return stats of entity in args
    
    :params:
      :attr:`granularity`: string: 'Site'  or 'Resource'
      
      :attr:`name`: string: the name of the site or of the resource
      
      :attr:`siteName`: optional (string) for the sitename, 
      in case you're looking for a resource status 

      :attr:`tests`: optional (list of) tests. 
      If omitted, takes only the service status metrics

    :returns:
      {
        'SAM-Status': {'SS'|'js'...:ok|down|na|degraded|partial|maint}'
      }
    """

    if granularity in ('Site', 'Sites'):
      siteName = name
    elif granularity in ('Resource', 'Resources'):
      siteName = siteName
    
    if timeout is not None:
      socket.setdefaulttimeout(timeout)
    
    sam = self._curlDownload(granularity, site = siteName, tests = tests)
    
    if sam is None:
      return S_OK(None)
    
    samStatus = self._xmlParsing(granularity, sam, name, tests)
    
    if samStatus is None or samStatus == {}:
      return S_OK(None)
    
    return S_OK(samStatus)
    
#############################################################################

#  def getLink(self, name, tests):
#
#    link = 'http://dashb-lhcb-sam.cern.ch/dashboard/request.py/latestresultssmry?siteSelect3=500&serviceTypeSelect3=0&sites=LCG.Bologna.it&services=CE&tests=37535&tests=398&tests=404&tests=405&tests=406&tests=403&tests=407&tests=37624&tests=399&tests=2&tests=5&tests=7&tests=14&tests=25&tests=37732&exitStatus=all'

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
      
    # SAMDB-PI to query
    samdb_ep = samdbpi_url + samdbpi_method + "VO_name=LHCb" + "&Site_name=" + samdbpi_site + samdbpi_test
    
    req = urllib2.Request(samdb_ep)
    samPage = urllib2.urlopen(req)

    sam = samPage.read()

    return sam
    
#############################################################################

  def _xmlParsing(self, granularity, sam, entity, tests):
    """ Performs xml parsing from the sam string 
        Returns a dictionary containing status of entity
    """

    status = {}

    doc = minidom.parseString(sam)
  
    if granularity in ('Site', 'Sites'):
        try:
          s = doc.getElementsByTagName("status")[0].childNodes
          status['SiteStatus'] = str(s[0].nodeValue)
        except IndexError:
          return None
      
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
        return S_ERROR("There are no SAM tests for this service")
#        raise NoSAMTests
      
      if tests is None or tests == []:
        tests = ['SS']
      
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
        
        if metricToCheck is None:
          continue

        s = metricToCheck.getElementsByTagName("status")[0].childNodes
        status[test] = str(s[0].nodeValue)
    
    return status
    
#############################################################################
#
#class NoSAMTests(Exception):
#  
#  def __init__(self, message = ""):
#    self.message = message
#    Exception.__init__(self, message)
#  
#  def __str__(self):
#    return "There are no SAM tests for this service \n" + repr(self.message)
  
#############################################################################
