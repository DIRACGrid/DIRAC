########################################################################
# $Id: NotificationHandler.py,v 1.1 2009/01/21 11:27:59 rgracian Exp $
########################################################################

""" The Notification service provides a toolkit to contact people via email
    (eventually SMS etc.) to trigger some actions.

    The original motivation for this is due to some sites restricting the
    sending of email but it is useful for e.g. crash reports to get to their
    destination.

    Another use-case is for users to request an email notification for the
    completion of their jobs.  When output data files are uploaded to the
    Grid, an email could be sent by default with the metadata of the file.
"""

__RCSID__ = "$Id: NotificationHandler.py,v 1.1 2009/01/21 11:27:59 rgracian Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC import gConfig, gLogger, S_OK, S_ERROR

def initializeNotificationHandler( serviceInfo ):

  return S_OK()

class NotificationHandler( RequestHandler ):

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

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#