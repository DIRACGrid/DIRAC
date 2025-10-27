"""The Identity Provider Factory instantiates IdProvider objects
according to their configuration
"""

import jwt

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities import ObjectLoader
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import getDIRACClients
from DIRAC.FrameworkSystem.private.authorization.utils.Utilities import collectMetadata
from DIRAC.Resources.IdProvider.Utilities import getIdProviderIdentifierFromIssuerAndClientID


class IdProviderFactory:
    def __init__(self):
        """Standard constructor"""
        self.log = gLogger.getSubLogger(self.__class__.__name__)

    def getIdProviderFromToken(self, accessToken):
        """This method returns a IdProvider instance corresponding to the supplied
        issuer in a token.

        :param token: access token or dict with access_token key

        :return: S_OK(IdProvider)/S_ERROR()
        """
        # Read token without verification to get issuer & client_id
        try:
            payload = jwt.decode(accessToken, leeway=300, options=dict(verify_signature=False, verify_aud=False))
        except jwt.exceptions.DecodeError as e:
            return S_ERROR(f"The provided token cannot be decoded: {e}")

        issuer = payload.get("iss", "").strip("/")
        clientID = payload.get("client_id", "")
        if not issuer or not clientID:
            return S_ERROR(f"Cannot retrieve the IdProvider that emitted {accessToken}")

        # Find a corresponding IdProvider identifier
        result = getIdProviderIdentifierFromIssuerAndClientID(issuer, clientID)
        if not result["OK"]:
            return result
        return self.getIdProvider(result["Value"])

    def getIdProvider(self, name, client_name_prefix="", **kwargs):
        """This method returns a IdProvider instance corresponding to the supplied
        name.

        :param str name: the name of the Identity Provider client
        :param str client_name_prefix: name of the client of the IdP

        :return: S_OK(IdProvider)/S_ERROR()
        """
        if not name:
            return S_ERROR("Identity Provider client name must be not None.")
        self.log.debug("Search configuration for", name)

        clients = getDIRACClients()
        if name in clients:
            # If it is a DIRAC default pre-registered client
            # Get Authorization Server metadata
            try:
                asMetaDict = collectMetadata(kwargs.get("issuer"), ignoreErrors=True)
            except Exception as e:
                return S_ERROR(str(e))
            pDict = asMetaDict
            pDict.update(clients[name])
        else:
            # If it is external identity provider client
            result = gConfig.getOptionsDict(f"/Resources/IdProviders/{name}")
            if not result["OK"]:
                self.log.error("Failed to read configuration", f"{name}: {result['Message']}")
                return result

            pDict = result["Value"]

            if client_name_prefix:
                client_name_prefix = client_name_prefix + "_"
            pDict["client_id"] = pDict[f"{client_name_prefix}client_id"]
            pDict["client_secret"] = pDict[f"{client_name_prefix}client_secret"]

        pDict.update(kwargs)
        pDict["ProviderName"] = name

        # Instantiating the IdProvider
        # By default, OAuth2IdProvider is used
        providerType = pDict.get("ProviderType", "OAuth2")
        self.log.verbose(f"Creating IdProvider of {providerType} type with the name {name}")
        subClassName = f"{providerType}IdProvider"

        objectLoader = ObjectLoader.ObjectLoader()
        result = objectLoader.loadObject(f"Resources.IdProvider.{subClassName}", subClassName)
        if not result["OK"]:
            self.log.error("Failed to load object", f"{subClassName}: {result['Message']}")
            return result

        idProviderClass = result["Value"]
        try:
            provider = idProviderClass(**pDict)
        except Exception as x:
            msg = f"IdProviderFactory could not instantiate {subClassName} object: {str(x)}"
            self.log.exception()
            self.log.warn(msg)
            return S_ERROR(msg)

        return S_OK(provider)
