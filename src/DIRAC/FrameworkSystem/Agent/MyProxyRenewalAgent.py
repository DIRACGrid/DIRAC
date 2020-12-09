"""  Proxy Renewal agent is the key element of the Proxy Repository
     which maintains the user proxies alive
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB
from DIRAC.Core.Utilities.ThreadPool import ThreadPool


class MyProxyRenewalAgent(AgentModule):

  def initialize(self):

    requiredLifeTime = self.am_getOption("MinimumLifeTime", 3600)
    renewedLifeTime = self.am_getOption("RenewedLifeTime", 54000)
    self.proxyDB = ProxyDB(useMyProxy=True)

    gLogger.info("Minimum Life time      : %s" % requiredLifeTime)
    gLogger.info("Life time on renew     : %s" % renewedLifeTime)
    gLogger.info("MyProxy server         : %s" % self.proxyDB.getMyProxyServer())
    gLogger.info("MyProxy max proxy time : %s" % self.proxyDB.getMyProxyMaxLifeTime())

    self.__threadPool = ThreadPool(1, 10)
    return S_OK()

  def __renewProxyForCredentials(self, userDN, userGroup):
    lifeTime = self.am_getOption("RenewedLifeTime", 54000)
    gLogger.info("Renewing for %s@%s %s secs" % (userDN, userGroup, lifeTime))
    retVal = self.proxyDB.renewFromMyProxy(userDN,
                                           userGroup,
                                           lifeTime=lifeTime)
    if not retVal['OK']:
      gLogger.error("Failed to renew proxy", "for %s@%s : %s" % (userDN, userGroup, retVal['Message']))
    else:
      gLogger.info("Renewed proxy for %s@%s" % (userDN, userGroup))

  def __treatRenewalCallback(self, oTJ, exceptionList):
    gLogger.exception(lException=exceptionList)

  def execute(self):
    """ The main agent execution method
    """
    self.proxyDB.purgeLogs()
    gLogger.info("Purging expired requests")
    retVal = self.proxyDB.purgeExpiredRequests()
    if retVal['OK']:
      gLogger.info(" purged %s requests" % retVal['Value'])
    gLogger.info("Purging expired proxies")
    retVal = self.proxyDB.purgeExpiredProxies()
    if retVal['OK']:
      gLogger.info(" purged %s proxies" % retVal['Value'])
    retVal = self.proxyDB.getCredentialsAboutToExpire(self.am_getOption("MinimumLifeTime", 3600))
    if not retVal['OK']:
      return retVal
    data = retVal['Value']
    gLogger.info("Renewing %s proxies..." % len(data))
    for record in data:
      userDN = record[0]
      userGroup = record[1]
      self.__threadPool.generateJobAndQueueIt(self.__renewProxyForCredentials,
                                              args=(userDN, userGroup),
                                              oExceptionCallback=self.__treatRenewalCallback)
    self.__threadPool.processAllResults()
    return S_OK()
