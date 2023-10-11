"""
Handling the download of the shifter Proxy
"""
import os

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager


def getShifterProxy(shifterType, fileName=False):
    """This method returns a shifter's proxy

    :param str shifterType: ProductionManager / DataManager...
    :param str fileName: file name

    :return: S_OK(dict)/S_ERROR()
    """
    if fileName:
        mkDir(os.path.dirname(fileName))
    opsHelper = Operations()
    userName = opsHelper.getValue(cfgPath("Shifter", shifterType, "User"), "")
    if not userName:
        return S_ERROR(f"No shifter User defined for {shifterType}")
    result = Registry.getDNForUsername(userName)
    if not result["OK"]:
        return result
    userDN = result["Value"][0]
    result = Registry.findDefaultGroupForDN(userDN)
    if not result["OK"]:
        return result
    defaultGroup = result["Value"]
    userGroup = opsHelper.getValue(cfgPath("Shifter", shifterType, "Group"), defaultGroup)
    vomsAttr = Registry.getVOMSAttributeForGroup(userGroup)
    if vomsAttr:
        gLogger.info(f"Getting VOMS [{vomsAttr}] proxy for shifter {userName}@{userGroup} ({userDN})")
        result = gProxyManager.downloadVOMSProxyToFile(
            userDN, userGroup, filePath=fileName, requiredTimeLeft=86400, cacheTime=86400
        )
    else:
        gLogger.info(f"Getting proxy for shifter {userName}@{userGroup} ({userDN})")
        result = gProxyManager.downloadProxyToFile(
            userDN, userGroup, filePath=fileName, requiredTimeLeft=86400, cacheTime=86400
        )
    if not result["OK"]:
        return result
    chain = result["chain"]
    fileName = result["Value"]
    return S_OK({"DN": userDN, "username": userName, "group": userGroup, "chain": chain, "proxyFile": fileName})


def setupShifterProxyInEnv(shifterType, fileName=False):
    """Return the shifter's proxy and set it up as the default
    proxy via changing the environment.
    This method returns a shifter's proxy

    :param str shifterType: ProductionManager / DataManager...
    :param str fileName: file name

    :return: S_OK(dict)/S_ERROR()
    """
    result = getShifterProxy(shifterType, fileName)
    if not result["OK"]:
        return result
    proxyDict = result["Value"]
    os.environ["X509_USER_PROXY"] = proxyDict["proxyFile"]
    return result
