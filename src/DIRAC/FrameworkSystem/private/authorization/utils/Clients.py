import time

from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope
from authlib.integrations.sqla_oauth2 import OAuth2ClientMixin

from DIRAC import gLogger
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
        token_endpoint_auth_method="none",
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


class Client(OAuth2ClientMixin):
    """This class describes the OAuth2 client."""

    def __init__(self, params):
        """C'r

        :param dict params: client parameters
        """
        if params.get("redirect_uri") and not params.get("redirect_uris"):
            params["redirect_uris"] = [params["redirect_uri"]]
        self.set_client_metadata(params)
        self.client_id = params["client_id"]
        self.client_secret = params.get("client_secret", "")
        self.client_id_issued_at = params.get("client_id_issued_at", int(time.time()))
        self.client_secret_expires_at = params.get("client_secret_expires_at", 0)

    def get_allowed_scope(self, scope):
        """Get allowed scope. Has been slightly modified to accommodate parametric scopes.

        :param str scope: requested scope

        :return: str -- scopes
        """
        if not isinstance(scope, str):
            scope = list_to_scope(scope)
        allowed = scope_to_list(super().get_allowed_scope(scope))
        for s in scope_to_list(scope):
            for def_scope in scope_to_list(self.scope):
                if s.startswith(def_scope) and s not in allowed:
                    allowed.append(s)
        gLogger.debug('Try to allow "%s" scope:' % scope, allowed)
        return list_to_scope(list(set(allowed)))
