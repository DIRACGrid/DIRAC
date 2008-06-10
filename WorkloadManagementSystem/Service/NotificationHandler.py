########################################################################
# $Id: NotificationHandler.py,v 1.3 2008/06/10 14:56:14 paterson Exp $
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

__RCSID__ = "$Id: NotificationHandler.py,v 1.3 2008/06/10 14:56:14 paterson Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Mail import Mail
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
      gLogger.verbose('Mail sent successfully to %s with subject %s' %(address,subject))
      gLogger.debug(result['Value'])

    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#