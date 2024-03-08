""" The Notification service provides a toolkit to contact people via email
    to trigger some actions.
"""
from DIRAC import S_OK, gConfig
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.Mail import Mail


class NotificationHandlerMixin:
    @classmethod
    def initializeHandler(cls, serviceInfo):
        """Handler initialization"""

        cls.mailCache = DictCache()

        return S_OK()

    ###########################################################################
    types_sendMail = [str, str, str, str]

    def export_sendMail(self, address, subject, body, fromAddress):
        """Send an email with supplied body to the specified address using the Mail utility.

        :param str address: recipient addresses
        :param str subject: subject of letter
        :param str body: body of letter
        :param str fromAddress: sender address, if "", will be used default from CS

        :return: S_OK(str)/S_ERROR() -- str is status message
        """
        self.log.verbose(f"Received signal to send the following mail to {address}:\nSubject = {subject}\n{body}")
        if self.mailCache.exists(hash(address + subject + body)):
            return S_OK("Email with the same content already sent today to current addresses, come back tomorrow")
        eMail = Mail()
        notificationSection = PathFinder.getServiceSection("Framework/Notification")
        csSection = notificationSection + "/SMTP"
        eMail._smtpHost = gConfig.getValue(f"{csSection}/Host")
        eMail._smtpPort = gConfig.getValue(f"{csSection}/Port")
        eMail._smtpLogin = gConfig.getValue(f"{csSection}/Login")
        eMail._smtpPasswd = gConfig.getValue(f"{csSection}/Password")
        eMail._smtpPtcl = gConfig.getValue(f"{csSection}/Protocol")
        eMail._subject = subject
        eMail._message = body
        eMail._mailAddress = address
        if fromAddress:
            eMail._fromAddress = fromAddress
        eMail._fromAddress = gConfig.getValue(f"{csSection}/FromAddress") or eMail._fromAddress
        result = eMail._send()
        if not result["OK"]:
            self.log.warn(f"Could not send mail with the following message:\n{result['Message']}")
        else:
            self.mailCache.add(hash(address + subject + body), 3600 * 24)
            self.log.info(f"Mail sent successfully to {address} with subject {subject}")
            self.log.debug(result["Value"])

        return result


class NotificationHandler(NotificationHandlerMixin, RequestHandler):
    pass
