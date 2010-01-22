""" GOCDBClient class is a client for the GOC DB, looking for Downtimes.
"""

import urllib2
from datetime import datetime
from xml.dom import minidom

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *


class GOCDBClient:
  
#############################################################################

  def getStatus(self, granularity, name):
    """  
    Return actual GOCDB status of entity in `name`
        
    :params:
      :attr:`granularity`: string: should be a ValidRes
      
      :attr:`name` should be the name of the ValidRes

    :return:
      {
        'DT':'OUTAGE'|'AT_RISK'|'None',
        'Enddate':datetime
      }

    """

    if granularity in ('Site', 'Sites'):
      self._entity = getSiteRealName(name)
    elif granularity in ('Resource', 'Resources'):
      self._entity = name
    else:
      raise InvalidRes, where(self, self.getStatus)
    
    resCDL = self._curlDownload(self._entity)
    
    res = self._xmlParsing(resCDL, granularity)
    
    if res is None:
      return {'DT':'None'}
      
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
    gocdbpi_startDate = startDate
     
    # GOCDB-PI to query
    gocdb_ep = gocdbpi_url + "method=" + gocdbpi_method + "&topentity=" + gocdbpi_topEntity + "&ongoing_only=yes"
    #gocdb_ep = gocdbpi_url + "method=" + gocdbpi_method + "&topentity=" + gocdbpi_topEntity + "&startdate=" + gocdbpi_startDate

    opener = urllib2.build_opener()
    try:
      dtPage = opener.open(gocdb_ep)
    except IOError, errorMsg:
      exceptStr = where(self, self._curlDownload) + " while opening %s." % gocdb_ep
      gLogger.exception(exceptStr,'',errorMsg)
      #return S_ERROR("%s%s" % (exceptStr,errorMsg))

    dt = dtPage.read()
    
    opener.close()

    return dt
    

#############################################################################

  def _xmlParsing(self, dt, siteOrRes):
    """ Performs xml parsing from the dt string (returns a dictionary)
    """

    try:
      doc = minidom.parseString(dt)
#    except TypeError, errorMsg:
#      exceptStr = where(self, self._xmlParsing)
#      gLogger.exception(exceptStr,'',errorMsg)
#    except AttributeError, errorMsg:
#      exceptStr = where(self, self._xmlParsing)
#      gLogger.exception(exceptStr,'',errorMsg)
#      #return S_ERROR("%s%s" % (exceptStr,errorMsg))
    except Exception, errorMsg:
      return None    

    downtimes = doc.getElementsByTagName("DOWNTIME")
#    if downtimes == []:
#      return {'DT':'No Info'} 
    handler = {}    
    
    for dt in downtimes:
      if dt.getAttributeNode("CLASSIFICATION"):
        attrs_class = dt.attributes["CLASSIFICATION"]
        # List containing all the DOM elements
        dom_elements = dt.childNodes

        for elements in dom_elements:
          if siteOrRes == 'Site':
            if elements.nodeName == "HOSTNAME":
              return None
          if elements.nodeName == "SEVERITY":
            for element in elements.childNodes:
              if element.nodeType == element.TEXT_NODE: 
                severity = str(element.nodeValue)
                handler['DT'] = severity
          elif elements.nodeName == "END_DATE":
            for element in elements.childNodes:
              if element.nodeType == element.TEXT_NODE: 
                edate = float(element.nodeValue)
              end_date = datetime.utcfromtimestamp(edate).isoformat(' ')
              handler['Enddate'] = end_date
      return handler
    
    
#############################################################################
