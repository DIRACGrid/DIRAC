""" DIRAC Notification Client class encapsulates the methods exposed
    by the Notification service.
"""
from DIRAC import S_ERROR, gLogger
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
                    self.log.warn(f"Sending mail failed with exception:\n{str(x)}")

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
