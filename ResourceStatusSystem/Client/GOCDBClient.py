""" GOCDBClient class is a client for the GOC DB, looking for Downtimes.
"""

import urllib2
import time
from datetime import datetime, timedelta
from xml.dom import minidom

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *


class GOCDBClient:
  
#############################################################################

  def getStatus(self, granularity, name, startDate = None, startingInHours = None):
    """  
    Return actual GOCDB status of entity in `name`
        
    :params:
      :attr:`granularity`: string: should be a ValidRes
      
      :attr:`name`: should be the name of the ValidRes
      
      :attr:`startDate`: if not given, takes only ongoing DownTimes.
      if given, could be a datetime or a string ("YYYY-MM-DD"), and download 
      DownTimes starting after that date.
      
      :attr:`startingInHours`: optional integer. If given, donwload 
      DownTimes starting in the next given hours (startDate is then useless)  

    :return:
      {
        'DT':'OUTAGE in X hours'|'AT_RISK in X hours'|'OUTAGE'|'AT_RISK'|'None',
        'Startdate':datetime (in string)
        'Enddate':datetime (in string)
      }

    """

    if granularity in ('Site', 'Sites'):
      self._entity = getSiteRealName(name)
    elif granularity in ('Resource', 'Resources'):
      self._entity = name
    else:
      raise InvalidRes, where(self, self.getStatus)
    
    startDate_STR = None
    startDateMax = None
    startDateMax_STR = None
    
    if startingInHours is not None:
      startDate = datetime.utcnow()
      startDateMax = startDate + timedelta(hours = startingInHours)
      startDateMax_STR = startDateMax.isoformat(' ')[0:10]
        
    if startDate is not None:
      if isinstance(startDate, basestring):
        startDate_STR = startDate
        startDate = datetime(*time.strptime(startDate, "%Y-%m-%d")[0:3])
      elif isinstance(startDate, datetime):
        startDate_STR = startDate.isoformat(' ')[0:10]
    
    resCDL = self._curlDownload(self._entity, startDate_STR)
    if resCDL is None:
      return [{'DT':'None'}]
    
    res = self._xmlParsing(resCDL, granularity, startDate, startDateMax)
    
    if res is None or res == []:
      return [{'DT':'None'}]
      
    return res
  

#############################################################################
  
  def _curlDownload(self, entity, startDate=None):
    """ Download ongoing downtimes for entity using the GOC DB programmatic interface
    """

    #GOCDB-PI url and method settings
    #
    # Set the GOCDB URL
    gocdbpi_url = "https://goc.gridops.org/gocdbpi/public/?"
    # Set your method
    gocdbpi_method = "get_downtime"
    # Set your topentity
    gocdbpi_topEntity = entity
    # Set the desidered start date
    if startDate is None: 
      when = "&ongoing_only=yes" 
      gocdbpi_startDate = ""
    else:
      when = "&startdate="
      gocdbpi_startDate = startDate
     
    # GOCDB-PI to query
    gocdb_ep = gocdbpi_url + "method=" + gocdbpi_method + "&topentity=" + gocdbpi_topEntity + when + gocdbpi_startDate

    try:
      opener = urllib2.build_opener()
      dtPage = opener.open(gocdb_ep)
    except IOError, errorMsg:
      exceptStr = where(self, self._curlDownload) + " while opening %s." % gocdb_ep
      gLogger.exception(exceptStr,'',errorMsg)
      return None
    except Exception, errorMsg:
      exceptStr = where(self, self._curlDownload) + " while opening %s." % gocdb_ep
      gLogger.exception(exceptStr,'',errorMsg)
      return None
      
    dt = dtPage.read()
    
    opener.close()

    return dt
    

#############################################################################

  def _xmlParsing(self, dt, siteOrRes, startDate = None, startDateMax = None):
    """ Performs xml parsing from the dt string (returns a dictionary)
    """

    try:
      doc = minidom.parseString(dt)

      downtimes = doc.getElementsByTagName("DOWNTIME")
#      if downtimes == []:
#        return {'DT':'No Info'} 
      DTList = []  
      
      for dt in downtimes:
        handler = {}  
        if dt.getAttributeNode("CLASSIFICATION"):
          attrs_class = dt.attributes["CLASSIFICATION"]
          # List containing all the DOM elements
          dom_elements = dt.childNodes
          
          for elements in dom_elements:
            if siteOrRes == 'Site':
              if elements.nodeName == "HOSTNAME":
                break
            if elements.nodeName == "SEVERITY":
              for element in elements.childNodes:
                if element.nodeType == element.TEXT_NODE: 
                  severity = str(element.nodeValue)
                  handler['DT'] = severity
            elif elements.nodeName == "START_DATE":
              for element in elements.childNodes:
                if element.nodeType == element.TEXT_NODE: 
                  sdate = float(element.nodeValue)
                start_date = datetime.utcfromtimestamp(sdate)
                start_date_STR = start_date.isoformat(' ')
                handler['StartDate'] = start_date_STR
            elif elements.nodeName == "END_DATE":
              for element in elements.childNodes:
                if element.nodeType == element.TEXT_NODE: 
                  edate = float(element.nodeValue)
                end_date = datetime.utcfromtimestamp(edate)
                end_date_STR = end_date.isoformat(' ')
                handler['EndDate'] = end_date_STR
        
          try:
            if startDate is not None:
              if end_date < startDate:
                continue
            if startDateMax is not None:
              if start_date > startDateMax:
                continue
              
            if start_date > datetime.utcnow():
              hoursTo = convertTime(start_date - datetime.utcnow(), 'hours')
              handler['DT'] = handler['DT'] + " in %d hours" %hoursTo
          except NameError:
            pass
        
        if handler != {}:
          DTList.append(handler)
          
      return DTList
    
    except Exception, errorMsg:
      exceptStr = where(self, self._xmlParsing)
      gLogger.exception(exceptStr,'',errorMsg)
      return None    

    
#############################################################################
