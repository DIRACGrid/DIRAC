"""  ProxyRenewalAgent keeps the proxy repository clean.

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN ProxyRenewalAgent
      :end-before: ##END
      :dedent: 2
      :caption: ProxyRenewalAgent options
"""
from DIRAC import S_OK

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB

DEFAULT_MAIL_FROM = "proxymanager@diracgrid.org"


class ProxyRenewalAgent(AgentModule):
    def initialize(self):
        requiredLifeTime = self.am_getOption("MinimumLifeTime", 3600)
        renewedLifeTime = self.am_getOption("RenewedLifeTime", 54000)
        mailFrom = self.am_getOption("MailFrom", DEFAULT_MAIL_FROM)
        self.proxyDB = ProxyDB(mailFrom=mailFrom)

        self.log.info(f"Minimum Life time      : {requiredLifeTime}")
        self.log.info(f"Life time on renew     : {renewedLifeTime}")

        return S_OK()

    def execute(self):
        """The main agent execution method"""
        self.log.verbose("Purging expired requests")
        res = self.proxyDB.purgeExpiredRequests()
        if not res["OK"]:
            self.log.error(res["Message"])
        else:
            self.log.info(f"Purged {res['Value']} requests")

        self.log.verbose("Purging expired proxies")
        res = self.proxyDB.purgeExpiredProxies()
        if not res["OK"]:
            self.log.error(res["Message"])
        else:
            self.log.info(f"Purged {res['Value']} proxies")

        self.log.verbose("Purging logs")
        res = self.proxyDB.purgeLogs()
        if not res["OK"]:
            self.log.error(res["Message"])

        return S_OK()
