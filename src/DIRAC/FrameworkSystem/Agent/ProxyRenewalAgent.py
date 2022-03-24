"""  ProxyRenewalAgent keeps the proxy repository clean.

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN ProxyRenewalAgent
      :end-before: ##END
      :dedent: 2
      :caption: ProxyRenewalAgent options
"""
import concurrent.futures

from DIRAC import S_OK

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB

DEFAULT_MAIL_FROM = "proxymanager@diracgrid.org"


class ProxyRenewalAgent(AgentModule):
    def initialize(self):

        requiredLifeTime = self.am_getOption("MinimumLifeTime", 3600)
        renewedLifeTime = self.am_getOption("RenewedLifeTime", 54000)
        mailFrom = self.am_getOption("MailFrom", DEFAULT_MAIL_FROM)
        self.useMyProxy = self.am_getOption("UseMyProxy", False)
        self.proxyDB = ProxyDB(useMyProxy=self.useMyProxy, mailFrom=mailFrom)

        self.log.info(f"Minimum Life time      : {requiredLifeTime}")
        self.log.info(f"Life time on renew     : {renewedLifeTime}")
        if self.useMyProxy:
            self.log.info(f"MyProxy server         : {self.proxyDB.getMyProxyServer()}")
            self.log.info(f"MyProxy max proxy time : {self.proxyDB.getMyProxyMaxLifeTime()}")

        return S_OK()

    def __renewProxyForCredentials(self, userDN, userGroup):
        lifeTime = self.am_getOption("RenewedLifeTime", 54000)
        self.log.info(f"Renewing for {userDN}@{userGroup} {lifeTime} secs")
        res = self.proxyDB.renewFromMyProxy(userDN, userGroup, lifeTime=lifeTime)
        if not res["OK"]:
            self.log.error("Failed to renew proxy", f"for {userDN}@{userGroup} : {res['Message']}")
        else:
            self.log.info(f"Renewed proxy for {userDN}@{userGroup}")

    def execute(self):
        """The main agent execution method"""
        self.log.verbose("Purging expired requests")
        res = self.proxyDB.purgeExpiredRequests()
        if not res["OK"]:
            self.log.error(res["Message"])
        else:
            self.log.info(f"Purged {res['Value']} requests")

        self.log.verbose("Purging expired tokens")
        res = self.proxyDB.purgeExpiredTokens()
        if not res["OK"]:
            self.log.error(res["Message"])
        else:
            self.log.info(f"Purged {res['Value']} tokens")

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

        if self.useMyProxy:
            res = self.proxyDB.getCredentialsAboutToExpire(self.am_getOption("MinimumLifeTime", 3600))
            if not res["OK"]:
                return res
            data = res["Value"]
            self.log.info(f"Renewing {len(data)} proxies...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for record in data:
                    userDN = record[0]
                    userGroup = record[1]
                    futures.append(executor.submit(self.__renewProxyForCredentials, userDN, userGroup))

        return S_OK()
