""" DIRAC Notification Client class encapsulates the methods exposed
    by the Notification service.
"""
from DIRAC import gLogger, S_ERROR
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.Mail import Mail


@createClient("Framework/Notification")
class NotificationClient(Client):
    def __init__(self, **kwargs):
        """Notification Client constructor"""
        super().__init__(**kwargs)

        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.setServer("Framework/Notification")

    def sendMail(self, addresses, subject, body, fromAddress=None, localAttempt=True, html=False):
        """Send an e-mail with subject and body to the specified address. Try to send
        from local area before central service by default.
        """
        self.log.verbose(f"Received signal to send the following mail to {addresses}:\nSubject = {subject}\n{body}")
        result = S_ERROR()

        if not fromAddress:
            fromAddress = ""

        addresses = [addresses] if isinstance(addresses, str) else list(addresses)
        for address in addresses:

            if localAttempt:
                try:
                    m = Mail()
                    m._subject = subject
                    m._message = body
                    m._mailAddress = address
                    m._html = html
                    if fromAddress:
                        m._fromAddress = fromAddress
                    result = m._send()
                except Exception as x:
                    self.log.warn("Sending mail failed with exception:\n%s" % (str(x)))

                if result["OK"]:
                    self.log.verbose(f"Mail sent successfully from local host to {address} with subject {subject}")
                    self.log.debug(result["Value"])
                    return result

                self.log.warn(
                    "Could not send mail with the following message:\n%s\n will attempt to send via NotificationService"
                    % result["Message"]
                )

            result = self._getRPC().sendMail(address, subject, body, fromAddress)
            if not result["OK"]:
                self.log.error("Could not send mail via central Notification service", result["Message"])
                return result
            else:
                self.log.verbose(result["Value"])

        return result

    def sendSMS(self, userName, body, fromAddress=None):
        """Send an SMS with body to the specified DIRAC user name."""
        if not fromAddress:
            fromAddress = ""

        self.log.verbose(f"Received signal to send the following SMS to {userName}:\n{body}")
        result = self._getRPC().sendSMS(userName, body, fromAddress)
        if not result["OK"]:
            self.log.error("Could not send SMS via central Notification service", result["Message"])
        else:
            self.log.verbose(result["Value"])

        return result

    ###########################################################################
    # ALARMS
    ###########################################################################

    def newAlarm(self, subject, status, notifications, assignee, body, priority, alarmKey=""):
        if not isinstance(notifications, (list, tuple)):
            return S_ERROR(
                "Notifications parameter has to be a list or a tuple with a combination of [ 'Web', 'Mail', 'SMS' ]"
            )
        alarmDef = {
            "subject": subject,
            "status": status,
            "notifications": notifications,
            "assignee": assignee,
            "priority": priority,
            "body": body,
        }
        if alarmKey:
            alarmDef["alarmKey"] = alarmKey
        return self._getRPC().newAlarm(alarmDef)

    def updateAlarm(self, id=-1, alarmKey="", comment=False, modDict={}):
        if id == -1 and not alarmKey:
            return S_ERROR("Need either alarm id or key to update an alarm!")
        updateReq = {"comment": comment, "modifications": modDict}
        if id != -1:
            updateReq["id"] = id
        if alarmKey:
            updateReq["alarmKey"] = alarmKey
        return self._getRPC().updateAlarm(updateReq)

    ###########################################################################
    # MANAGE NOTIFICATIONS
    ###########################################################################

    def addNotificationForUser(self, user, message, lifetime=604800, deferToMail=True):
        try:
            lifetime = int(lifetime)
        except Exception:
            return S_ERROR("Message lifetime has to be a non decimal number")
        return self._getRPC().addNotificationForUser(user, message, lifetime, deferToMail)
