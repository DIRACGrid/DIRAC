"""
Set of utilities to retrieve Information from proxy
"""

import base64

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO

from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.DiracX import diracxTokenFromPEM
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Utilities import DErrno


def getProxyInfo(proxy=False, disableVOMS=False):
    """
    :Returns: a dict with all the proxy info:

      * values that will be there always
          * 'chain' : chain object containing the proxy
          * 'subject' : subject of the proxy
          * 'issuer' : issuer of the proxy
          * 'isProxy' : bool
          * 'isLimitedProxy' : bool
          * 'validDN' : Valid DN in DIRAC
          * 'validGroup' : Valid Group in DIRAC
          * 'secondsLeft' : Seconds left
          * 'hasDiracxToken'
      * values that can be there
          * 'path' : path to the file,
          * 'group' : DIRAC group
          * 'VO' : DIRAC VO
          * 'groupProperties' : Properties that apply to the DIRAC Group
          * 'username' : DIRAC username
          * 'identity' : DN that generated the proxy
          * 'hostname' : DIRAC host nickname
          * 'VOMS'

    """
    # Discover proxy location
    proxyLocation = False
    if isinstance(proxy, X509Chain):
        chain = proxy
    else:
        if not proxy:
            proxyLocation = Locations.getProxyLocation()
        elif isinstance(proxy, str):
            proxyLocation = proxy
        if not proxyLocation:
            return S_ERROR(DErrno.EPROXYFIND)
        chain = X509Chain()
        retVal = chain.loadProxyFromFile(proxyLocation)
        if not retVal["OK"]:
            return S_ERROR(DErrno.EPROXYREAD, f"{proxyLocation}: {retVal['Message']} ")

    retVal = chain.getCredentials()
    if not retVal["OK"]:
        return retVal

    infoDict = retVal["Value"]
    infoDict["chain"] = chain
    if proxyLocation:
        infoDict["path"] = proxyLocation

    if not disableVOMS and chain.isVOMS()["Value"]:
        infoDict["hasVOMS"] = True
        retVal = VOMS().getVOMSAttributes(chain)
        if retVal["OK"]:
            infoDict["VOMS"] = retVal["Value"]
        else:
            infoDict["VOMSError"] = retVal["Message"].strip()

    if "group" in infoDict:
        infoDict["VO"] = Registry.getVOForGroup(infoDict["group"])

    infoDict["hasDiracxToken"] = False
    if proxyLocation:
        infoDict["hasDiracxToken"] = bool(diracxTokenFromPEM(proxyLocation))

    return S_OK(infoDict)


def getProxyInfoAsString(proxyLoc=False, disableVOMS=False):
    """
    return the info as a printable string
    """
    retVal = getProxyInfo(proxyLoc, disableVOMS)
    if not retVal["OK"]:
        return retVal
    infoDict = retVal["Value"]
    return S_OK(formatProxyInfoAsString(infoDict))


def formatProxyInfoAsString(infoDict):
    """
    convert a proxy infoDict into a string
    """
    leftAlign = 13
    contentList = []
    for field in (
        "subject",
        "issuer",
        "identity",
        ("secondsLeft", "timeleft"),
        ("group", "DIRAC group"),
        ("hasDiracxToken", "DiracX"),
        "rfc",
        "path",
        "username",
        ("groupProperties", "properties"),
        ("hasVOMS", "VOMS"),
        ("VOMS", "VOMS fqan"),
        ("VOMSError", "VOMS Error"),
    ):
        if isinstance(field, str):
            dispField = field
        else:
            dispField = field[1]
            field = field[0]
        if field not in infoDict:
            continue
        if field == "secondsLeft":
            secs = infoDict[field]
            hours = int(secs / 3600)
            secs -= hours * 3600
            mins = int(secs / 60)
            secs -= mins * 60
            value = "%02d:%02d:%02d" % (hours, mins, secs)
        elif field == "groupProperties":
            value = ", ".join(infoDict[field])
        else:
            value = infoDict[field]
        contentList.append(f"{dispField.ljust(leftAlign)}: {value}")
    return "\n".join(contentList)


def getProxyStepsInfo(chain):
    """
    Extended information of all Steps in the ProxyChain
    Returns a list of dictionary with Info for each Step.
    """
    infoList = []
    nC = chain.getNumCertsInChain()["Value"]
    for i in range(nC):
        cert = chain.getCertInChain(i)["Value"]
        stepInfo = {}
        stepInfo["subject"] = cert.getSubjectDN()["Value"]
        stepInfo["issuer"] = cert.getIssuerDN()["Value"]
        stepInfo["serial"] = cert.getSerialNumber()["Value"]
        stepInfo["not before"] = cert.getNotBeforeDate()["Value"]
        stepInfo["not after"] = cert.getNotAfterDate()["Value"]
        stepInfo["lifetime"] = cert.getRemainingSecs()["Value"]
        stepInfo["extensions"] = cert.getExtensions()["Value"]
        dG = cert.getDIRACGroup(ignoreDefault=True)["Value"]
        if dG:
            stepInfo["group"] = dG
        if cert.hasVOMSExtensions()["Value"]:
            stepInfo["VOMS ext"] = True
        infoList.append(stepInfo)
    return S_OK(infoList)


def formatProxyStepsInfoAsString(infoList):
    """
    Format the List of Proxy steps dictionaries as a printable string.
    """
    contentsList = []
    for i in range(len(infoList)):
        contentsList.append(f" + Step {i}")
        stepInfo = infoList[i]
        for key in (
            "subject",
            "issuer",
            "serial",
            "not after",
            "not before",
            "group",
            "VOMS ext",
            "lifetime",
            "extensions",
        ):
            if key in stepInfo:
                value = stepInfo[key]
                if key == "serial":
                    try:
                        # b16encode needs a string, while the serial
                        # may be a long
                        value = base64.b16encode(f"{value}")
                    except Exception as e:
                        gLogger.exception("Could not read serial:", lException=e)
                if key == "lifetime":
                    secs = value
                    hours = int(secs / 3600)
                    secs -= hours * 3600
                    mins = int(secs / 60)
                    secs -= mins * 60
                    value = "%02d:%02d:%02d" % (hours, mins, secs)
                if key == "extensions":
                    value = "\n   %s" % "\n   ".join(
                        [f"{extName.strip().rjust(20)} = {extValue.strip()}" for extName, extValue in value]
                    )
                contentsList.append(f"  {key.ljust(10).capitalize()} : {value}")
    return "\n".join(contentsList)


def getVOfromProxyGroup():
    """
    Return the VO associated to the group in the proxy
    """

    ret = getProxyInfo(disableVOMS=True)
    if not ret["OK"] or "group" not in ret["Value"]:
        voName = getVO()
    else:
        voName = Registry.getVOForGroup(ret["Value"]["group"])

    return S_OK(voName)
