########################################################################
# File :   IdProviderFactory.py
# Author : A.T.
########################################################################

"""  The Identity Provider Factory instantiates IdProvider objects
     according to their configuration
"""
import jwt

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ObjectLoader, ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Resources.IdProvider.Utilities import getProviderInfo, getSettingsNamesForIdPIssuer
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import getDIRACClients
from DIRAC.FrameworkSystem.private.authorization.utils.Utilities import collectMetadata

gCacheMetadata = ThreadSafe.Synchronizer()


class IdProviderFactory:
    def __init__(self):
        """Standard constructor"""
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.cacheMetadata = DictCache()

    @gCacheMetadata
    def getMetadata(self, idP):
        return self.cacheMetadata.get(idP) or {}

    @gCacheMetadata
    def addMetadata(self, idP, data, time=24 * 3600):
        if data:
            self.cacheMetadata.add(idP, time, data)

    def getIdProviderForToken(self, token):
        """This method returns a IdProvider instance corresponding to the supplied
        issuer in a token.

        :param token: access token or dict with access_token key

        :return: S_OK(IdProvider)/S_ERROR()
        """
        if isinstance(token, dict):
            token = token["access_token"]

        data = {}

        # Read token without verification to get issuer
        issuer = jwt.decode(token, leeway=300, options=dict(verify_signature=False, verify_aud=False))["iss"].strip("/")

        result = getSettingsNamesForIdPIssuer(issuer)
        if not result["OK"]:
            return result
        return self.getIdProvider(result["Value"])

    def getIdProvider(self, name, **kwargs):
        """This method returns a IdProvider instance corresponding to the supplied
        name.

        :param str name: the name of the Identity Provider client

        :return: S_OK(IdProvider)/S_ERROR()
        """
        if not name:
            return S_ERROR("Identity Provider client name must be not None.")
        # Get Authorization Server metadata
        try:
            asMetaDict = collectMetadata(kwargs.get("issuer"), ignoreErrors=True)
        except Exception as e:
            return S_ERROR(str(e))
        self.log.debug("Search configuration for", name)
        clients = getDIRACClients()
        if name in clients:
            # If it is a DIRAC default pre-registred client
            pDict = asMetaDict
            pDict.update(clients[name])
        else:
            # if it is external identity provider client
            result = getProviderInfo(name)
            if not result["OK"]:
                self.log.error("Failed to read configuration", "{}: {}".format(name, result["Message"]))
                return result
            pDict = result["Value"]
            # Set default redirect_uri
            pDict["redirect_uri"] = pDict.get("redirect_uri", asMetaDict["redirect_uri"])

        pDict.update(kwargs)
        pDict["ProviderName"] = name

        self.log.verbose("Creating IdProvider of {} type with the name {}".format(pDict["ProviderType"], name))
        subClassName = "%sIdProvider" % pDict["ProviderType"]

        objectLoader = ObjectLoader.ObjectLoader()
        result = objectLoader.loadObject("Resources.IdProvider.%s" % subClassName, subClassName)
        if not result["OK"]:
            self.log.error("Failed to load object", "{}: {}".format(subClassName, result["Message"]))
            return result

        pClass = result["Value"]
        try:
            provider = pClass(**pDict)
        except Exception as x:
            msg = f"IdProviderFactory could not instantiate {subClassName} object: {str(x)}"
            self.log.exception()
            self.log.warn(msg)
            return S_ERROR(msg)

        return S_OK(provider)
