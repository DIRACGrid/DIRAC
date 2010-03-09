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
        'EndDate':datetime (in string)
        'StartDate':datetime (in string)
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
    
    self.buildURL(res)  
    return res
  
#############################################################################

  def getInfo(self, granularity, name):
    
    pass

#############################################################################
  def buildURL(self, DTList):
    '''build the URL  for the downtime on the basis of the downtime ID '''
    baseURL = 'https://goc.gridops.org/downtime/list?id='
    for dt in DTList:
      id = dt['id']
      url = baseURL + str(id)
      dt['url'] = url
    
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
      gocdb_ep = gocdbpi_url + "method=" + gocdbpi_method + "&topentity=" + gocdbpi_topEntity + when + gocdbpi_startDate
      gocdb_ongoing=gocdb_ep
    else:
      when = "&startdate="
      gocdbpi_startDate = startDate
      gocdb_ep = gocdbpi_url + "method=" + gocdbpi_method + "&topentity=" + gocdbpi_topEntity + when + gocdbpi_startDate
      gocdb_ongoing = gocdbpi_url + "method=" + gocdbpi_method + "&topentity=" + gocdbpi_topEntity + "&ongoing_only=yes"
    

    # GOCDB-PI to query
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


    try:
      dtPage_ongoing = opener.open(gocdb_ongoing)
    except IOError, errorMsg:
      exceptStr = where(self, self._curlDownload) + " while opening %s." % gocdb_ep
      gLogger.exception(exceptStr,'',errorMsg)
      return dt
    except Exception, errorMsg:
      exceptStr = where(self, self._curlDownload) + " while opening %s." % gocdb_ep
      gLogger.exception(exceptStr,'',errorMsg)
      return dt

    dt_on= dtPage_ongoing.read()

    #now mangling and merging  the 2 XML documents (ongoing ones - that have always to be reported - and future ones if appliable)

    if dt_on.rfind("ROOT/") != -1:
      my_dt_on=dt_on.replace("</ROOT>","")
    else:
      my_dt_on=dt_on
      
    if dt.rfind("ROOT/") != -1:
      my_dt= dt.replace("<?xml version=\"1.0\"?>","")
      my_dt=my_dt.replace("<ROOT>","")
    else:
      my_dt=dt

    if (dt.rfind("ROOT/") == -1) and (dt_on.rfind("ROOT/") == -1):
      final=my_dt_on+my_dt
      final=final.replace("\n","")
      final=final.replace("</ROOT>","</ROOT>\n")
      final=final.replace("<?xml version=\"1.0\"?>","<?xml version=\"1.0\"?>\n")
    elif dt.rfind("ROOT/") == -1 and dt_on.rfind("ROOT/") != -1:
      final=dt
    elif dt.rfind("ROOT/") != -1 and dt_on.rfind("ROOT/") == -1:
      final=dt_on
    elif dt.rfind("ROOT/") != -1 and dt_on.rfind("ROOT/") != -1:
      final=dt_on
    opener.close()
    return final
    #return dt

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
        typeOfDT = 'unknown'
        handler = {}  
        if dt.getAttributeNode("CLASSIFICATION"):
          attrs_class = dt.attributes["CLASSIFICATION"]
          # List containing all the DOM elements
          dom_elements = dt.childNodes
          
          for elements in dom_elements:
            if siteOrRes == 'Site' and elements.nodeName == "HOSTNAME": # user asked for DT relative to the site
              typeOfDT = 'service'
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

          if siteOrRes == 'Site' and typeOfDT == 'service':
              print 'dont store the dt'
          else:
            print 'store the dt'
            if dt.getAttributeNode("ID"):
              attrs_id = dt.attributes["ID"]
              id=attrs_id.nodeValue.replace("u","")
              handler['id'] = id
            
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
