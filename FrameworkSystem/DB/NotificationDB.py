########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/DB/NotificationDB.py,v 1.1 2009/06/13 23:21:11 atsareg Exp $
########################################################################
""" NotificationDB class is a front-end to the Notifications database
"""

__RCSID__ = "$Id: NotificationDB.py,v 1.1 2009/06/13 23:21:11 atsareg Exp $"

import time
import types
import threading
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.Core.Base.DB import DB

class NotificationDB(DB):

  def __init__( self, maxQueueSize = 10 ):

    DB.__init__( self, 'NotificationDB', 'Framework/NotificationDB', maxQueueSize )
    self.lock = threading.Lock()

  def setAlarm( self, name='', body='', group='', atype = 'Action' ):
    """ Create a new alarm record
    """
    
    names = ['AlarmName','AlarmBody','DestinationGroup','AlarmType']
    values = [name,body,group,atype]
    
    self.lock.acquire()
    result = self._getConnection()
    if result['OK']:
      connection = result['Value']
    else:
      self.lock.release()
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    res = self._insert('Alarms',names,values,connection)
    if not res['OK']:
      self.lock.release()
      return res
    req = "SELECT LAST_INSERT_ID();"
    res = self._query(req,connection)
    self.lock.release()
    if not res['OK']:
      return res
    alarmID = int(res['Value'][0][0])
    
  def getAlarmAttribute(self,alarmID,attribute):
    """ Get alarm info
    """  
    
    req = "SELECT %s FROM Alarms WHERE AlarmID=%d" % (attribute,int(alarmID))
    result = self._query(req)
    if not result['OK']:
      return result
    
    if not result['Value']:
      return S_ERROR('Alarm %d not found' % int(alarmID))
    
    value = result['Value'][0][0]
    return S_OK(value)
    
  def updateAlarm(self, alarmID, status='',actionTaken='',comment='',author):
    """ Update the given alarm
    """  
    
    updates = []
    if status:
      updates.append(" Status='%s'" % status)
    if actionTaken:
      updates.append(" Action='%s'" % actionTaken)
    new_comment = ''
    if comment:
      result = self.getAlarmAttribute(alarmID,'Comment')
      if not result['OK']:
        pass
      old_comment = result['Value']
      new_comment = old_comment+'\n'+comment
      
    if new_comment:
      updates.append(" Comment='%s'" % new_comment)  
        
    updates.append(" Author='%s'" % author)    
        
    updateString = updates.join(',')    
    
    req = "UPDATE Alarms SET %s, StatusDate=UTC_TIMESTAMP() WHERE AlarmID=%d" % (updateString,int(alarmID))
    result = self._update(req)
    return result
    
  def getAlarms(self,name='',group='',status='',startID=0,startTime='',endTime=''):
    """ Check for availability of alarms with given properties
    """  
    
    condDict = {}
    if name:
      condDict['AlarmName'] = name
    if group:
      condDict['DestinationGroup'] = group
    if status:
      condDict['Status'] = status
      
    order = 'AlarmID'
    startTime = condDict.get('FromDate','')
    endTime = condDict.get('ToDate','')
    
    result = selectAlarms(condDict,order,startID,startTime,endTime)
    return result
  
  def selectAlarms(self,selectDict,order='',startID=0,startTime='',endTime=''):
    """ 
    """
        
    condition = buildCondition(self, condDict, older=startTime, newer=endTime, 
                               timeStamp='StatusDate')
    if startID:
      if condition:
        condition += ' AND AlarmID > %d' % int(startID) 
      else:
        condition += ' WHERE AlarmID > %d' % int(startID)
        
    parameters = [ 'AlarmID', 'AlarmName', 'AlarmStatus', 'AlarmBody','DestinationView',
                   'Author', 'Source', 'CreationTime', 'AlarmType' ]    
    
    parString = parameters.join(',')    
        
    req = "SELECT %s FROM Alarms " % parString
    req += conditions
    if order:
      req += " %s" % order
    
    result = self._query(req)
    if not result['OK']:
      return result
    
    if not result['Value']:
      return S_OK([])

    resultDict = {}
    resultDict['ParameterNames'] = parameters
    resultDict['Records'] = result['Value']
    return S_OK(resultDict)
      
      
    
    
    
            
       
      
    
    
    
    
    
    