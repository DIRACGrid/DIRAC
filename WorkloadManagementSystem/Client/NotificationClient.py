########################################################################
# $Id: NotificationClient.py,v 1.2 2008/06/10 14:58:35 paterson Exp $
########################################################################

""" DIRAC WMS Notification Client class encapsulates the methods exposed
    by the Notification service.
"""

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC import gLogger, S_OK, S_ERROR

import os

class NotificationClient:

  #############################################################################
  def __init__(self):
    """ Notification Client constructor
    """
    self.log = gLogger.getSubLogger('NotificationClient')

  #############################################################################
  def sendMail(self,address,subject,body,fromAddress=None,localAttempt=True):
    """ Send an e-mail with subject and body to the specified address. Try to send
        from local area before central service by default.
    """
    self.log.verbose('Received signal to send the following mail to %s:\nSubject = %s\n%s' %(address,subject,body))
    result = S_ERROR()
    if localAttempt:
      try:
        m = Mail()
        m._subject = subject
        m._message = body
        m._mailAddress = address
        if fromAddress:
          m._fromAddress = fromAddress
        result = m._send()
      except Exception,x:
        self.log.warn('Sending mail failed with exception:\n%s' %(str(x)))

      if result['OK']:
        self.log.info('Mail sent successfully from local host to %s with subject %s' %(address,subject))
        self.log.debug(result['Value'])
        return result

      self.log.warn('Could not send mail with the following message:\n%s\n will attempt to send via NotificationService' %result['Message'])

    notify = RPCClient('WorkloadManagement/Notification',useCertificates=False)
    result = notify.sendMail(address,subject,body,str(fromAddress))
    if not result['OK']:
      self.log.error('Could not send mail via central Notification service',result['Message'])
    else:
      self.log.info(result['Value'])

    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#