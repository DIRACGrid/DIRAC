"""  Proxy Renewal agent is the key element of the Proxy Repository
     which maintains the user proxies alive

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN MyProxyRenewalAgent
      :end-before: ##END
      :dedent: 2
      :caption: MyProxyRenewalAgent options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import concurrent.futures

from DIRAC import gLogger, S_OK

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB

DEFAULT_MAIL_FROM = "proxymanager@diracgrid.org"


class MyProxyRenewalAgent(AgentModule):
    def initialize(self):

        requiredLifeTime = self.am_getOption("MinimumLifeTime", 3600)
        renewedLifeTime = self.am_getOption("RenewedLifeTime", 54000)
        mailFrom = self.am_getOption("MailFrom", DEFAULT_MAIL_FROM)
        self.proxyDB = ProxyDB(useMyProxy=True, mailFrom=mailFrom)

        gLogger.info("Minimum Life time      : %s" % requiredLifeTime)
        gLogger.info("Life time on renew     : %s" % renewedLifeTime)
        gLogger.info("MyProxy server         : %s" % self.proxyDB.getMyProxyServer())
        gLogger.info("MyProxy max proxy time : %s" % self.proxyDB.getMyProxyMaxLifeTime())

        return S_OK()

    def __renewProxyForCredentials(self, userDN, userGroup):
        lifeTime = self.am_getOption("RenewedLifeTime", 54000)
        gLogger.info("Renewing for %s@%s %s secs" % (userDN, userGroup, lifeTime))
        retVal = self.proxyDB.renewFromMyProxy(userDN, userGroup, lifeTime=lifeTime)
        if not retVal["OK"]:
            gLogger.error("Failed to renew proxy", "for %s@%s : %s" % (userDN, userGroup, retVal["Message"]))
        else:
            gLogger.info("Renewed proxy for %s@%s" % (userDN, userGroup))

    def __treatRenewalCallback(self, oTJ, exceptionList):
        gLogger.exception(lException=exceptionList)

    def execute(self):
        """The main agent execution method"""
        self.proxyDB.purgeLogs()
        gLogger.info("Purging expired requests")
        retVal = self.proxyDB.purgeExpiredRequests()
        if retVal["OK"]:
            gLogger.info(" purged %s requests" % retVal["Value"])
        gLogger.info("Purging expired proxies")
        retVal = self.proxyDB.purgeExpiredProxies()
        if retVal["OK"]:
            gLogger.info(" purged %s proxies" % retVal["Value"])
        retVal = self.proxyDB.getCredentialsAboutToExpire(self.am_getOption("MinimumLifeTime", 3600))
        if not retVal["OK"]:
            return retVal
        data = retVal["Value"]
        gLogger.info("Renewing %s proxies..." % len(data))
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for record in data:
                userDN = record[0]
                userGroup = record[1]
                futures.append(executor.submit(self.__renewProxyForCredentials, userDN, userGroup))
        return S_OK()
