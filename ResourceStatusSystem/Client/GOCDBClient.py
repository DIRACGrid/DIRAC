""" GOCDBClient class is a client for the GOC DB, looking for Downtimes.
"""

import urllib2
import time
from datetime import datetime, timedelta
from xml.dom import minidom
import socket

class GOCDBClient:
  
#############################################################################

  def getStatus(self, granularity, name, startDate = None, 
                startingInHours = None, timeout = None):
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
    
    if timeout is not None:
      socket.setdefaulttimeout(10)

    if startingInHours is not None:
    # make 2 queries and later merge the results
      
      # first call: pass the startDate argument as None, 
      # so the curlDownload method will search for only ongoing DTs
      resXML_ongoing = self._curlDownload(name)
      if resXML_ongoing is None:
        res_ongoing = 'None'
      else:
        res_ongoing = self._xmlParsing(resXML_ongoing, granularity, startDate, startDateMax)
        if res_ongoing == []:
          res_ongoing = 'None'
      resXML_startDate = self._curlDownload(name, startDate_STR)

      # second call: pass the startDate argument
      if resXML_startDate is None:
        res_startDate = 'None'
      else:
        res_startDate = self._xmlParsing(resXML_startDate, granularity, startDate, startDateMax)
        if res_startDate == []:
          res_startDate = 'None'
      
      # merge the results of the 2 queries:
      if res_startDate is not 'None' and res_ongoing is not 'None':
        res = []
        for dt in res_startDate:
          res.append(dt)
        for dt in res_ongoing:
          if dt in res:
            #DT already appended
            pass
          else:
            res.append(dt)

      elif res_startDate is not 'None' and res_ongoing is 'None':
        res = res_startDate
      elif res_startDate is 'None' and res_ongoing is not 'None':
        res = res_ongoing
      else:
        return None
         
    else:
      #just query for onGoing downtimes
      resXML = self._curlDownload(name)
      if resXML is None:
        return None
    
      res = self._xmlParsing(resXML, granularity, startDate, startDateMax)
    
    self.buildURL(res)

    if res is None or res == []:
      return None
      
    if len(res) == 1:
      res = res[0]
    
    return res
  

#############################################################################

  def buildURL(self, DTList):
    '''build the URL relative to the DT '''
    baseURL = "https://goc.gridops.org/downtime/list?id="
    for dt in DTList:
      id = str(dt['id'])
      url = baseURL + id
      dt['URL'] = url

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

    req = urllib2.Request(gocdb_ep)
    dtPage = urllib2.urlopen(req)

    dt = dtPage.read()

    return dt
    

#############################################################################

  def _xmlParsing(self, dt, siteOrRes, startDate = None, startDateMax = None):
    """ Performs xml parsing from the dt string (returns a dictionary)
    """
    doc = minidom.parseString(dt)

    downtimes = doc.getElementsByTagName("DOWNTIME")
#      if downtimes == []:
#        return {'DT':'No Info'} 
    DTList = []  
    
    for dt in downtimes:
      DTtype = 'None' # can be a site or a resource DT
      handler = {}  
      if dt.getAttributeNode("CLASSIFICATION"):
        attrs_class = dt.attributes["CLASSIFICATION"]
        # List containing all the DOM elements
        dom_elements = dt.childNodes
        
        for elements in dom_elements:
          if siteOrRes == 'Site':
            if elements.nodeName == "HOSTNAME":
              DTtype = 'Resource'
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
	  if start_date is None or end_date is None:
    	    continue
          if startDate is not None:
            if end_date < startDate:
              continue
          if startDateMax is not None:
            if start_date > startDateMax:
              continue
          if start_date > datetime.utcnow():
            hoursTo = self.__convertTime(start_date - datetime.utcnow())
            handler['Type'] = 'Programmed'
            handler['InHours'] = hoursTo
          else:
            handler['Type'] = 'OnGoing'

        except NameError, UnboundLocalError:
          pass
      
      start_date = None
      end_date = None
      
      # get the DT ID:
      if dt.getAttributeNode("ID"):
        attrs_id = dt.attributes["ID"]
        id=attrs_id.nodeValue.replace("u","")
        if siteOrRes == 'Site' and DTtype == 'Resource': 
#            print 'it is a resource DT, do not store the ID'
          pass
        else: 
          handler['id'] = id


      if handler != {}:
        DTList.append(handler)
        
    return DTList
    
    
#############################################################################

  def __convertTime(self, t):
    
    hour = 0
    
    try:
      tms = t.milliseconds
      hour = hour + tms/36000
    except AttributeError:
      pass
    try:
      ts = t.seconds
      hour = hour + ts/3600
    except AttributeError:
      pass
    try:
      tm = t.minutes
      hour = hour + tm/60
    except AttributeError:
      pass
    try:
      th = t.hours
      hour = hour + th
    except AttributeError:
      pass
    try:
      td = t.days
      hour = hour + td * 24
    except AttributeError:
      pass
    try:
      tw = t.weeks
      hour = hour + tw * 168
    except AttributeError:
      pass
    
    return hour

#############################################################################