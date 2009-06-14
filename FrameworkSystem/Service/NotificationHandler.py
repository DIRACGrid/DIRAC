########################################################################
# $Id: NotificationHandler.py,v 1.4 2009/06/14 22:35:47 atsareg Exp $
########################################################################

""" The Notification service provides a toolkit to contact people via email
    (eventually SMS etc.) to trigger some actions.

    The original motivation for this is due to some sites restricting the
    sending of email but it is useful for e.g. crash reports to get to their
    destination.

    Another use-case is for users to request an email notification for the
    completion of their jobs.  When output data files are uploaded to the
    Grid, an email could be sent by default with the metadata of the file.
    
    It can also be used to set alarms to be promptly forwarded to those
    subscribing to them. 
"""

__RCSID__ = "$Id: NotificationHandler.py,v 1.4 2009/06/14 22:35:47 atsareg Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.DB.NotificationDB import NotificationDB

notificationDB = None

def initializeNotificationHandler( serviceInfo ):

  global notificationDB
  notificationDB = NotificationDB()
  return S_OK()

class NotificationHandler( RequestHandler ):

  def initialize(self):
    credDict = self.getRemoteCredentials()
    self.clientDN = credDict['DN']
    self.clientGroup = credDict['group']
    self.clientProperties = credDict[ 'properties' ]
    self.client = credDict[ 'username' ]

  ###########################################################################
  types_sendMail = [StringType,StringType,StringType,StringType]
  def export_sendMail(self,address,subject,body,fromAddress):
    """ Send an email with supplied body to the specified address using the Mail utility.
    """
    gLogger.verbose('Received signal to send the following mail to %s:\nSubject = %s\n%s' %(address,subject,body))
    m = Mail()
    m._subject = subject
    m._message = body
    m._mailAddress = address
    if not fromAddress=='None':
      m._fromAddress = fromAddress
    result = m._send()
    if not result['OK']:
      gLogger.warn('Could not send mail with the following message:\n%s' %result['Message'])
    else:
      gLogger.info('Mail sent successfully to %s with subject %s' %(address,subject))
      gLogger.debug(result['Value'])

    return result

  ###########################################################################
  types_sendSMS = [StringType,StringType,StringType]
  def export_sendSMS(self,userName,body,fromAddress):
    """ Send an SMS with supplied body to the specified DIRAC user using the Mail utility via an SMS switch.
    """
    gLogger.verbose('Received signal to send the following SMS to %s:\n%s' %(userName,body))
    mobile = gConfig.getValue('/Security/Users/%s/mobile' %userName,'')
    if not mobile:
      return S_ERROR('No registered mobile number for %s' %userName)

    csSection = PathFinder.getServiceSection( 'Framework/Notification' )
    smsSwitch = gConfig.getValue('%s/SMSSwitch' %csSection,'')
    if not smsSwitch:
      return S_ERROR('No SMS switch is defined in CS path %s/SMSSwitch' %csSection)

    address = '%s@%s' %(mobile,smsSwitch)
    subject = 'DIRAC SMS'
    m = Mail()
    m._subject = subject
    m._message = body
    m._mailAddress = address
    if not fromAddress=='None':
      m._fromAddress = fromAddress
    result = m._send()
    if not result['OK']:
      gLogger.warn('Could not send SMS to %s with the following message:\n%s' %(userName,result['Message']))
    else:
      gLogger.info('SMS sent successfully to %s ' %(userName))
      gLogger.debug(result['Value'])

    return result
  
  ###########################################################################
  types_setAlarm = [StringType,StringType,StringType,StringType]
  def export_setAlarm(self,name,body,group,alarmType,view='Any',comment='',source=''):
    """ Set a new alarm in the Notification database
    """
    
    result = notificationDB.setAlarm(name,body,group,alarmType,
                                     author=self.clientDN,
                                     view=view,
                                     comment=comment,
                                     source=source)   
    return result
  
  ###########################################################################
  types_updateAlarm = [IntType,StringType,StringType,StringType]
  def export_updateAlarm(self,alarmID,status,action,comment):
    """ update an existing alarm in the Notification database
    """
    
    result = notificationDB.updateAlarm(alarmID,status,action,comment,
                                        author=self.clientDN)   
    return result
  
  ###########################################################################
  types_closeAlarm = [StringType,StringType,StringType]
  def export_closeAlarm(self,name,body,group):
    """ Set a new alarm in the Notification database
    """
    
    result = notificationDB.updateAlarm(status='Closed',author=self.clientDN)   
    return result
   
  ###########################################################################
  types_getAlarmsWeb = [DictType, ListType, IntType, IntType]
  def export_getAlarmsWeb(self,selectDict, sortList, startItem, maxItems):
    """ Select existing alarms suitable for the Web monitoring
    """ 
    
    order = ''
    if sortList:
      order = sortList[0]
      
    startID = selectDict.get('StartID',0)
    if startID:
      del selectDict['StartID']
    startTime = selectDict.get('FromDate','')
    if startTime:
      del selectDict['FromDate']
    endTime = selectDict.get('ToDate',0)
    if endTime:
      del selectDict['ToDate']
      
    result = notificationDB.selectAlarms(selectDict,order,startID,startTime,endTime) 
    if not result['OK']:
      return result
    parameters = result['Value']['ParameterNames']
    
    records = result['Value']['Records']
    nRecs = len(records)
    if startItem > nRecs:
      return S_ERROR('Start item is higher than the number of alarms')
    lastRecord = startItem+maxItems
    if lastRecord > nRecs:
      lastRecord = nRecs

    records = records[startItem:lastRecord]
    resultDict = result['Value']
    resultDict['Records'] = records

    return S_OK(resultDict)     
          
  ###########################################################################
  types_getAlarms = [IntType, StringType, StringType]
  def export_getAlarms(self,startID,startTime,group):
    """ Get alarms for the alarm notifier
    """
    
    result = notificationDB.getAlarms(startID=startID,startTime=startTime,group=group)
    return result
    
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
