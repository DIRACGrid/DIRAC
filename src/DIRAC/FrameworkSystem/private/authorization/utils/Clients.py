from DIRAC.Core.Security import Locations
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthorizationServerMetadata


# Description of default DIRAC OAuth2 clients
DEFAULT_CLIENTS = {
    # Description of default public DIRAC client which is installed in the terminal
    "DIRACCLI": dict(
        client_id="DIRAC_CLI",
        scope="proxy g: lifetime:",
        response_types=["device"],
        grant_types=["urn:ietf:params:oauth:grant-type:device_code", "refresh_token"],
        token_endpoint_auth_method="none",  # nosec: B106
        ProviderType="OAuth2",
        verify=Locations.getCAsLocation(),
    ),
    # These settings are for the web server
    "DIRACWeb": dict(
        client_id="DIRAC_Web",
        scope="g:",
        response_types=["code"],
        grant_types=["authorization_code", "refresh_token"],
        ProviderType="OAuth2",
        verify=Locations.getCAsLocation(),
    ),
}


def getDIRACClients():
    """Get DIRAC authorization clients

    :return: S_OK(dict)/S_ERROR()
    """
    clients = DEFAULT_CLIENTS.copy()
    result = getAuthorizationServerMetadata(ignoreErrors=True)
    confClients = result.get("Value", {}).get("Clients", {})
    for cli in confClients:
        if cli not in clients:
            clients[cli] = confClients[cli]
        else:
            clients[cli].update(confClients[cli])
    return clients
