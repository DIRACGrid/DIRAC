########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/DB/DataLoggingDB.py,v 1.1 2008/02/18 18:40:23 atsareg Exp $
########################################################################
""" DataLoggingDB class is a front-end to the Data Logging Database.
    The following methods are provided

    addFileRecord()
    getFileLoggingInfo()
"""    

__RCSID__ = "$Id: DataLoggingDB.py,v 1.1 2008/02/18 18:40:23 atsareg Exp $"

import re, os, sys
import time, datetime
from types import *

from DIRAC              import gLogger,S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Base.DB import DB  

MAGIC_EPOC_NUMBER = 1270000000

#############################################################################
class DataLoggingDB(DB):


  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """

    DB.__init__(self,'DataLoggingDB','DataManagement/DataLoggingDB',maxQueueSize)
    self.gLogger = gLogger
    
#############################################################################
  def addFileRecord(self,lfn,status,date='',source='Unknown'):
                       
    """ Add a new entry to the DataLoggingDB table. Optionaly the time stamp of the status can
        be provided in a form of a string in a format '%Y-%m-%d %H:%M:%S' or
        as datetime.datetime object. If the time stamp is not provided the current
        UTC time is used. 
    """
  
    self.gLogger.info("Adding record for file "+lfn+": '"+status+"' from "+source+" source")
  
    if not date:
      # Make the UTC datetime string and float
      _date = datetime.datetime.utcnow()
      epoc = time.mktime(_date.timetuple())+_date.microsecond/1000000. - MAGIC_EPOC_NUMBER
      time_order = round(epoc,3)      
    else:
      try:
        if type(date) in StringTypes:
          # The date is provided as a string in UTC 
          epoc = time.mktime(time.strptime(date,'%Y-%m-%d %H:%M:%S'))
          _date = datetime.datetime.fromtimestamp(epoc)
          time_order = epoc - MAGIC_EPOC_NUMBER
        elif type(date) == datetime.datetime:
          _date = date
          epoc = time.mktime(date.timetuple())+_date.microsecond/1000000. - MAGIC_EPOC_NUMBER
          time_order = round(epoc,3)  
        else:
          self.gLogger.error('Incorrect date for the logging record')
          _date = datetime.datetime.utcnow()
          epoc = time.mktime(_date.timetuple()) - MAGIC_EPOC_NUMBER
          time_order = round(epoc,3)  
      except:
        self.gLogger.exception('Exception while date evaluation')
        _date = datetime.datetime.utcnow()
        epoc = time.mktime(_date.timetuple()) - MAGIC_EPOC_NUMBER
        time_order = round(epoc,3)     

    cmd = "INSERT INTO DataLoggingInfo (LFN, Status, StatusTime, StatusTimeOrder, Source) " + \
          "VALUES ('%s','%s','%s',%f,'%s')" %  (lfn,status,str(_date),time_order,source)
            
    return self._update( cmd )
    
#############################################################################
  def getFileLoggingInfo(self, lfn):
    """ Returns a Status,StatusTime,StatusSource tuple 
        for each record found for the file specified by its LFN in historical order
    """

    cmd = "SELECT Status,StatusTime,Source FROM" \
          " DataLoggingInfo WHERE LFN='%s' ORDER BY StatusTimeOrder,StatusTime" % lfn

    result = self._query( cmd )
    if not result['OK']:
      return result
    if result['OK'] and not result['Value']:
      return S_ERROR('No Logging information for job %d' % int(jobID))
      
    return_value = []  
    for row in result['Value']:  		
      return_value.append((status,str(row[1]),row[2]))
      
    return S_OK(return_value)    
