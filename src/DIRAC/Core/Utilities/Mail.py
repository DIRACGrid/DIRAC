"""
    Extremely simple utility class to send mails
"""
import os
import socket

from smtplib import SMTP, SMTP_SSL
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from getpass import getuser

from DIRAC import gLogger, S_OK, S_ERROR


class Mail:
    def __init__(self):
        self._subject = ""
        self._message = ""
        self._mailAddress = ""
        self._html = False
        self._fromAddress = getuser() + "@" + socket.getfqdn()
        self._attachments = []
        self.esmtp_features = {}
        self._smtpPtcl = None
        self._smtpHost = None
        self._smtpPort = None
        self._smtpLogin = None
        self._smtpPasswd = None

    def _create(self, addresses):
        """create a mail object

        :param list addresses: addresses

        :return: S_OK(object)/S_ERROR() -- contain MIMEMultipart object
        """
        if not isinstance(addresses, list):
            addresses = [addresses]

        if not self._mailAddress:
            gLogger.warn("No mail address was provided. Mail not sent.")
            return S_ERROR("No mail address was provided. Mail not sent.")

        if not self._message:
            gLogger.warn("Message body is empty")
            if not self._subject:
                gLogger.warn("Subject and body empty. Mail not sent")
                return S_ERROR("Subject and body empty. Mail not sent")

        if self._html:
            mail = MIMEText(self._message, "html")
        else:
            mail = MIMEText(self._message, "plain")

        msg = MIMEMultipart()
        msg.attach(mail)

        msg["Subject"] = self._subject
        msg["From"] = self._fromAddress
        msg["To"] = ", ".join(addresses)

        for attachment in self._attachments:
            try:
                with open(attachment, "rb") as fil:
                    part = MIMEApplication(fil.read(), Name=os.path.basename(attachment))

                    part["Content-Disposition"] = 'attachment; filename="%s"' % os.path.basename(attachment)
                    msg.attach(part)
            except OSError as e:
                gLogger.exception("Could not attach %s" % attachment, lException=e)

        return S_OK(msg)

    def _send(self, msg=None):
        """send a single email message. If msg is in input, it is expected to be of email type, otherwise it will create it.

        :param object msg: MIMEMultipart object

        :return: S_OK()/S_ERROR()
        """

        if msg is None:
            addresses = self._mailAddress
            if isinstance(self._mailAddress, str):
                addresses = self._mailAddress.split(", ")

            result = self._create(addresses)
            if not result["OK"]:
                return result
            msg = result["Value"]

        if self._smtpPtcl == "SSL":
            smtp = SMTP_SSL()
        else:
            smtp = SMTP()
        smtp.set_debuglevel(0)
        try:
            connParams = {}
            if self._smtpHost:
                connParams["host"] = self._smtpHost
            if self._smtpPort:
                connParams["port"] = int(self._smtpPort)
            smtp.connect(**connParams)
            smtp.ehlo_or_helo_if_needed()
            if self._smtpPtcl == "TLS":
                smtp.starttls()
            if self._smtpLogin and self._smtpPasswd:
                smtp.login(self._smtpLogin, self._smtpPasswd)
            smtp.ehlo_or_helo_if_needed()
            smtp.sendmail(self._fromAddress, addresses, msg.as_string())
        except Exception as x:
            return S_ERROR("Sending mail failed %s" % str(x))

        smtp.quit()
        return S_OK("The mail was successfully sent")

    def __eq__(self, other):
        """Comparing an email object to another"""
        if isinstance(other, Mail):
            if self.__dict__ == other.__dict__:
                return True

        return False

    def __hash__(self):
        """Comparing for sets"""
        return hash(self._subject + self._message + self._fromAddress + self._mailAddress)
