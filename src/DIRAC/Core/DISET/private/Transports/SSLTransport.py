import os

from DIRAC import gLogger
from DIRAC.Core.Security import Locations
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.X509Certificate import X509Certificate  # pylint: disable=import-error


# Eventhough SSLTransport is not used in this file, it is imported in other module from there,
# so do not remove these imports !
from DIRAC.Core.DISET.private.Transports.M2SSLTransport import SSLTransport


def delegate(delegationRequest, kwargs):
    """
    Check delegate!
    """
    if kwargs.get("useCertificates"):
        chain = X509Chain()
        certTuple = Locations.getHostCertificateAndKeyLocation()
        chain.loadChainFromFile(certTuple[0])
        chain.loadKeyFromFile(certTuple[1])
    elif "proxyObject" in kwargs:
        chain = kwargs["proxyObject"]
    else:
        if "proxyLocation" in kwargs:
            procLoc = kwargs["proxyLocation"]
        else:
            procLoc = Locations.getProxyLocation()
        chain = X509Chain()
        chain.loadChainFromFile(procLoc)
        chain.loadKeyFromFile(procLoc)
    return chain.generateChainFromRequestString(delegationRequest)


def checkSanity(urlTuple, kwargs):
    """
    Check that all ssl environment is ok
    """
    useCerts = False
    certFile = ""
    if kwargs.get("proxyLocation"):
        certFile = kwargs["proxyLocation"]
    elif "useCertificates" in kwargs and kwargs["useCertificates"]:
        certTuple = Locations.getHostCertificateAndKeyLocation()
        if not certTuple:
            gLogger.error("No cert/key found! ")
            return S_ERROR("No cert/key found! ")
        certFile = certTuple[0]
        useCerts = True
    elif "proxyString" in kwargs:
        if not isinstance(kwargs["proxyString"], str):
            gLogger.error("proxyString parameter is not a valid type", str(type(kwargs["proxyString"])))
            return S_ERROR("proxyString parameter is not a valid type")
    else:
        certFile = Locations.getProxyLocation()
        if not certFile:
            gLogger.error("No proxy found")
            return S_ERROR("No proxy found")
        elif not os.path.isfile(certFile):
            gLogger.error("Proxy file does not exist", certFile)
            return S_ERROR(f"{certFile} proxy file does not exist")

    # For certs always check CA's. For clients skipServerIdentityCheck
    if "skipCACheck" not in kwargs or not kwargs["skipCACheck"]:
        if not Locations.getCAsLocation():
            gLogger.error("No CAs found!")
            return S_ERROR("No CAs found!")

    if "proxyString" in kwargs:
        certObj = X509Chain()
        retVal = certObj.loadChainFromString(kwargs["proxyString"])
        if not retVal["OK"]:
            gLogger.error("Can't load proxy string")
            return S_ERROR("Can't load proxy string")
    else:
        if useCerts:
            certObj = X509Certificate()
            certObj.loadFromFile(certFile)
        else:
            certObj = X509Chain()
            certObj.loadChainFromFile(certFile)

    retVal = certObj.hasExpired()
    if not retVal["OK"]:
        gLogger.error("Can't verify proxy or certificate file", f"{certFile}:{retVal['Message']}")
        return S_ERROR(f"Can't verify file {certFile}:{retVal['Message']}")
    else:
        if retVal["Value"]:
            notAfter = certObj.getNotAfterDate()
            if notAfter["OK"]:
                notAfter = notAfter["Value"]
            else:
                notAfter = "unknown"
            gLogger.error("PEM file has expired", f"{certFile} is not valid after {notAfter}")
            return S_ERROR(f"PEM file {certFile} has expired, not valid after {notAfter}")

    idDict = {}
    retVal = certObj.getDIRACGroup(ignoreDefault=True)
    if retVal["OK"] and retVal["Value"] is not False:
        idDict["group"] = retVal["Value"]
    if useCerts:
        idDict["DN"] = certObj.getSubjectDN()["Value"]
    else:
        idDict["DN"] = certObj.getIssuerCert()["Value"].getSubjectDN()["Value"]

    return S_OK(idDict)
