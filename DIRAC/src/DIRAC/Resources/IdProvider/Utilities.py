""" Utilities for the IdProvider package
"""
from DIRAC import S_OK, S_ERROR, gConfig


def getIdProviderIdentifiers(providerType=None):
    """Get providers for instance

    :param str instance: instance of what this providers
    :param str providerType: provider type

    :return: list of IdProviders
    """
    providers = []
    result = gConfig.getSections("/Resources/IdProviders")

    # Return an error if the section does not exist
    if not result["OK"] or not result["Value"] or not providerType:
        return result

    # Select only the IdProviders attached to the given providerType
    for name in result["Value"]:
        if providerType == gConfig.getValue(f"/Resources/IdProviders/{name}/ProviderType"):
            providers.append(name)
    return S_OK(providers)


def getIdProviderIdentifierFromIssuerAndClientID(issuer, clientID):
    """Get identity provider for issuer

    :param str issuer: issuer
    :param str clientID: client ID

    :return: S_OK(str)/S_ERROR()
    """
    result = getIdProviderIdentifiers()
    if not result["OK"]:
        return result

    for identifier in result["Value"]:
        testedClientID = gConfig.getValue(f"/Resources/IdProviders/{identifier}/client_id")
        # clientID is not always available, e.g. in case of a token of particular user
        if not clientID or (testedClientID and testedClientID == clientID):
            # Found the client ID but need to check the issuer
            # 2 different issuers could theoretically have a same client ID
            testedIssuer = gConfig.getValue(f"/Resources/IdProviders/{identifier}/issuer")

            if testedIssuer and testedIssuer.strip("/") == issuer.strip("/"):
                return S_OK(identifier)

    return S_ERROR("IdProvider not found")
