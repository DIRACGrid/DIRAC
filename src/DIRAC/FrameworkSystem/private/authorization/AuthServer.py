""" This class provides authorization server activity. """
import re
import json
import pprint
from dominate import tags as dom

from authlib.jose import JsonWebKey, jwt
from authlib.oauth2 import AuthorizationServer as _AuthorizationServer
from authlib.oauth2.base import OAuth2Error
from authlib.oauth2.rfc7636 import CodeChallenge
from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB
from DIRAC.Resources.IdProvider.Utilities import getIdProviderIdentifiers
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.ConfigurationSystem.Client.Utilities import isDownloadProxyAllowed
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (
    getUsernameForDN,
    getEmailsForGroup,
    wrapIDAsDN,
    getDNForUsername,
    getIdPForGroup,
    getGroupOption,
)
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.FrameworkSystem.Client.TokenManagerClient import TokenManagerClient
from DIRAC.Core.Tornado.Server.private.BaseRequestHandler import TornadoResponse
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import getDIRACClients, Client
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import OAuth2Request, createOAuth2Request
from DIRAC.FrameworkSystem.private.authorization.utils.Utilities import collectMetadata, getHTML
from DIRAC.FrameworkSystem.private.authorization.grants.RevokeToken import RevocationEndpoint
from DIRAC.FrameworkSystem.private.authorization.grants.RefreshToken import RefreshTokenGrant
from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import DeviceAuthorizationEndpoint, DeviceCodeGrant
from DIRAC.FrameworkSystem.private.authorization.grants.AuthorizationCode import AuthorizationCodeGrant

try:
    from authlib.oauth2 import JsonRequest
except ImportError:
    from authlib.oauth2 import HttpRequest as JsonRequest

sLog = gLogger.getSubLogger(__name__)


class AuthServer(_AuthorizationServer):
    """Implementation of the :class:`authlib.oauth2.rfc6749.AuthorizationServer`.

    This framework has been changed and simplified to be used for DIRAC purposes,
    namely authorization on the third party side and saving the received extended
    long-term access tokens on the DIRAC side with the possibility of their future
    use on behalf of the user without his participation.

    The idea is that DIRAC itself is not an identity provider and relies on third-party
    resources such as EGI Checkin or WLCG IAM.

    Initialize::

      server = AuthServer()
    """

    LOCATION = None

    def __init__(self):
        self.db = AuthDB()  # place to store session information
        self.log = sLog
        self.idps = IdProviderFactory()
        self.proxyCli = ProxyManagerClient()  # take care about proxies
        self.tokenCli = TokenManagerClient()  # take care about tokens
        # The authorization server has its own settings, but they are standardized
        self.metadata = collectMetadata()
        self.metadata.validate()
        # Initialize AuthorizationServer
        _AuthorizationServer.__init__(self, scopes_supported=self.metadata["scopes_supported"])
        # authlib requires the following methods:
        # The following `save_token` method is called when requesting a new access token to save it after it is generated.
        # Let's skip this step, because getting tokens and saving them if necessary has already taken place in `generate_token` method.
        self.save_token = lambda x, y: None
        # Framework integration can re-implement this method to support signal system.
        # But in this implementation, this system is not used.
        self.send_signal = lambda *x, **y: None
        # The main method that will return an access token to the user (this can be a proxy)
        self.generate_token = self.generateProxyOrToken
        # Register configured grants
        self.register_grant(RefreshTokenGrant)  # Enable refreshing tokens
        # Enable device code flow
        self.register_grant(DeviceCodeGrant)
        self.register_endpoint(DeviceAuthorizationEndpoint)
        self.register_endpoint(RevocationEndpoint)  # Enable revokation tokens
        self.register_grant(AuthorizationCodeGrant, [CodeChallenge(required=True)])  # Enable authorization code flow

    # pylint: disable=method-hidden
    def query_client(self, client_id):
        """Search authorization client.

        :param str clientID: client ID

        :return: client as object or None
        """
        gLogger.debug(f"Try to query {client_id} client")
        clients = getDIRACClients()
        for cli in clients:
            if client_id == clients[cli]["client_id"]:
                gLogger.debug(f"Found {cli} client:\n", pprint.pformat(clients[cli]))
                # Authorization successful
                return Client(clients[cli])
        # Authorization failed, client not found
        return None

    def _getScope(self, scope, param):
        """Get parameter scope

        :param str scope: scope
        :param str param: parameter scope

        :return: str or None
        """
        try:
            return [s.split(":")[1] for s in scope_to_list(scope) if s.startswith(f"{param}:") and s.split(":")[1]][0]
        except Exception:
            return None

    def generateProxyOrToken(
        self, client, grant_type, user=None, scope=None, expires_in=None, include_refresh_token=True
    ):
        """Generate proxy or tokens after authorization

        :param client: instance of the IdP client
        :param grant_type: authorization grant type (unused)
        :param str user: user identifier
        :param str scope: requested scope
        :param expires_in: when the token should expire (unused)
        :param bool include_refresh_token: to include refresh token (unused)

        :return: dict or str -- will return tokens as dict or proxy as string
        """
        # Read requested scopes
        group = self._getScope(scope, "g")
        lifetime = self._getScope(scope, "lifetime")
        # Found provider name for group
        provider = getIdPForGroup(group)

        # Search DIRAC username by user ID
        result = getUsernameForDN(wrapIDAsDN(user))
        if not result["OK"]:
            raise OAuth2Error(result["Message"])
        userName = result["Value"]

        # User request a proxy
        if "proxy" in scope_to_list(scope):
            # Try to return user proxy if proxy scope present in the authorization request
            if not isDownloadProxyAllowed():
                raise OAuth2Error("You can't get proxy, configuration(allowProxyDownload) not allow to do that.")
            sLog.debug(f"Try to query {user}@{group} proxy{f'with lifetime:{lifetime}' if lifetime else ''}")
            # Get user DNs
            result = getDNForUsername(userName)
            if not result["OK"]:
                raise OAuth2Error(result["Message"])
            userDNs = result["Value"]
            err = []
            # Try every DN to generate a proxy
            for dn in userDNs:
                sLog.debug(f"Try to get proxy for {dn}")
                params = {}
                if lifetime:
                    params["requiredTimeLeft"] = int(lifetime)
                # if the configuration describes adding a VOMS extension, we will do so
                if getGroupOption(group, "AutoAddVOMS", False):
                    result = self.proxyCli.downloadVOMSProxy(dn, group, **params)
                else:
                    # otherwise we will return the usual proxy
                    result = self.proxyCli.downloadProxy(dn, group, **params)
                if not result["OK"]:
                    err.append(result["Message"])
                else:
                    sLog.info("Proxy was created.")
                    result = result["Value"].dumpAllToString()
                    if not result["OK"]:
                        raise OAuth2Error(result["Message"])
                    # Proxy generated
                    return {
                        "proxy": result["Value"].decode() if isinstance(result["Value"], bytes) else result["Value"]
                    }
            # Proxy cannot be generated or not found
            raise OAuth2Error("; ".join(err))

        # User request a tokens
        else:
            # Ask TokenManager to generate new tokens for user
            result = self.tokenCli.getToken(userName, group)
            if not result["OK"]:
                raise OAuth2Error(result["Message"])
            token = result["Value"]
            # Wrap the refresh token and register it to protect against reuse
            result = self.registerRefreshToken(
                dict(sub=user, scope=scope, provider=provider, azp=client.get_client_id()), token
            )
            if not result["OK"]:
                raise OAuth2Error(result["Message"])
            # Return tokens as dictionary
            return result["Value"]

    def __signToken(self, payload):
        """Sign token

        :param dict payload: payload

        :return: S_OK(str)/S_ERROR()
        """
        result = self.db.getPrivateKey()
        if not result["OK"]:
            return result
        key = result["Value"]
        try:
            return S_OK(jwt.encode(dict(alg="RS256", kid=key.thumbprint()), payload, key).decode("utf-8"))
        except Exception as e:
            sLog.exception(e)
            return S_ERROR(repr(e))

    def readToken(self, token):
        """Decode self token

        :param str token: token to decode

        :return: S_OK(dict)/S_ERROR()
        """
        result = self.db.getKeySet()
        if not result["OK"]:
            return result
        try:
            return S_OK(jwt.decode(token, JsonWebKey.import_key_set(result["Value"].as_dict())))
        except Exception as e:
            sLog.exception(e)
            return S_ERROR(repr(e))

    def registerRefreshToken(self, payload, token):
        """Register refresh token to protect it from reuse

        :param dict payload: payload
        :param dict token: token as a dictionary

        :return: S_OK(dict)S_ERROR()
        """
        result = self.db.storeRefreshToken(token, payload.get("jti"))
        if result["OK"]:
            payload.update(result["Value"])
            result = self.__signToken(payload)
        if not result["OK"]:
            if token.get("refresh_token"):
                prov = self.idps.getIdProvider(payload["provider"])
                if prov["OK"]:
                    prov["Value"].revokeToken(token["refresh_token"])
                    prov["Value"].revokeToken(token["access_token"], "access_token")
            return result
        token["refresh_token"] = result["Value"]
        return S_OK(token)

    def getIdPAuthorization(self, provider, request):
        """Submit subsession to authorize with chosen provider and return dict with authorization url and session number

        :param str provider: provider name
        :param object request: main session request

        :return: S_OK(response)/S_ERROR() -- dictionary contain response generated by `handle_response`
        """
        result = self.idps.getIdProvider(provider)
        if not result["OK"]:
            raise Exception(result["Message"])
        idpObj = result["Value"]
        authURL, state, session = idpObj.submitNewSession()
        session["state"] = state
        session["Provider"] = provider
        session["firstRequest"] = request if isinstance(request, dict) else request.toDict()

        sLog.verbose("Redirect to", authURL)
        return self.handle_response(302, {}, [("Location", authURL)], session)

    def parseIdPAuthorizationResponse(self, response, session):
        """Fill session by user profile, tokens, comment, OIDC authorize status, etc.
        Prepare dict with user parameters, if DN is absent there try to get it.
        Create new or modify existing DIRAC user and store the session

        :param dict response: authorization response
        :param str session: session

        :return: S_OK(dict)/S_ERROR()
        """
        providerName = session.pop("Provider")
        sLog.debug(f"Try to parse authentication response from {providerName}:\n", pprint.pformat(response))
        # Parse response
        result = self.idps.getIdProvider(providerName)
        if not result["OK"]:
            return result
        idpObj = result["Value"]
        result = idpObj.parseAuthResponse(response, session)
        if not result["OK"]:
            return result

        # FINISHING with IdP
        # As a result of authentication we will receive user credential dictionary
        credDict, payload = result["Value"]

        sLog.debug("Read profile:", pprint.pformat(credDict))
        # Is ID registered?
        result = getUsernameForDN(credDict["DN"])
        if not result["OK"]:
            comment = f"ID {credDict['ID']} is not registered in DIRAC. "
            payload.update(idpObj.getUserProfile(idpObj.token.get("access_token")).get("Value", {}))
            result = self.__registerNewUser(providerName, payload)

            if result["OK"]:
                comment += "Administrators have been notified about you."
            else:
                comment += "Please, contact the DIRAC administrators."

            # Notify user about problem
            html = getHTML("unregistered user!", info=comment, theme="warning")
            return S_ERROR(html)

        credDict["username"] = result["Value"]

        # Update token for user. This token will be stored separately in the database and
        # updated from time to time. This token will never be transmitted,
        # it will be used to make exchange token requests.
        result = self.tokenCli.updateToken(idpObj.token, credDict["ID"], idpObj.name)
        return S_OK(credDict) if result["OK"] else result

    def create_oauth2_request(self, request, method_cls=OAuth2Request, use_json=False):
        """Parse request. Rewrite authlib method."""
        self.log.debug("Create OAuth2 request", "with json" if use_json else "")
        return createOAuth2Request(request, method_cls, use_json)

    def create_json_request(self, request):
        """Parse request. Rewrite authlib method."""
        return self.create_oauth2_request(request, JsonRequest, True)

    def validate_requested_scope(self, scope):
        """See :func:`authlib.oauth2.rfc6749.authorization_server.validate_requested_scope`"""
        # We also consider parametric scope containing ":" charter
        extended_scope = list_to_scope(
            [re.sub(r":.*$", ":", s) for s in scope_to_list((scope or "").replace("+", " "))]
        )
        super().validate_requested_scope(extended_scope)

    def handle_response(self, status_code=None, payload=None, headers=None, newSession=None, delSession=None):
        """Handle response

        :param int status_code: http status code
        :param payload: response payload
        :param list headers: headers
        :param dict newSession: session data to store

        :return: TornadoResponse()
        """
        resp = TornadoResponse(payload, status_code)
        if not isinstance(payload, dict):
            sLog.debug(
                f"Handle authorization response with {status_code} status code:",
                "HTML page" if payload.startswith("<!DOCTYPE html>") else payload,
            )
        elif "error" in payload:
            resp.clear_cookie("auth_session")  # pylint: disable=no-member
            sLog.error(f"{payload['error']}: {payload.get('error_description', 'unknown')}")
        if headers:
            sLog.debug("Headers:", headers)
            for key, value in headers:
                resp.set_header(key, value)  # pylint: disable=no-member
        if newSession:
            sLog.debug("Initialize new session:", newSession)
            # pylint: disable=no-member
            resp.set_secure_cookie("auth_session", json.dumps(newSession), secure=True, httponly=True)
        if delSession:
            resp.clear_cookie("auth_session")  # pylint: disable=no-member
        return resp

    def create_authorization_response(self, response, username):
        """Rewrite original Authlib method
        `authlib.authlib.oauth2.rfc6749.authorization_server.create_authorization_response`
        to catch errors and remove authorization session.

        :return: TornadoResponse object
        """
        try:
            response = super().create_authorization_response(response, username)
            response.clear_cookie("auth_session")
            return response
        except Exception as e:
            sLog.exception(e)
            return self.handle_response(
                payload=getHTML("server error", theme="error", body="traceback", info=repr(e)), delSession=True
            )

    def validate_consent_request(self, request, provider=None):
        """Validate current HTTP request for authorization page. This page
        is designed for resource owner to grant or deny the authorization::

        :param object request: tornado request
        :param provider: provider

        :return: response generated by `handle_response` or S_ERROR or html
        """
        try:
            request = self.create_oauth2_request(request)
            # Check Identity Provider
            req = self.validateIdentityProvider(request, provider)

            # If return HTML page with IdP selector
            if isinstance(req, str):
                return req

            sLog.info("Validate consent request for ", req.state)
            grant = self.get_authorization_grant(req)
            sLog.debug("Use grant:", grant.GRANT_TYPE)
            grant.validate_consent_request()
            if not hasattr(grant, "prompt"):
                grant.prompt = None

            # Submit second auth flow through IdP
            return self.getIdPAuthorization(req.provider, req)

        except OAuth2Error as error:
            self.db.removeSession(request.sessionID)
            code, body, _ = error(None)
            return self.handle_response(
                payload=getHTML(repr(error), state=code, body=body, info="OAuth2 error."), delSession=True
            )
        except Exception as e:
            self.db.removeSession(request.sessionID)
            sLog.exception(e)
            return self.handle_response(
                payload=getHTML("server error", theme="error", body="traceback", info=repr(e)), delSession=True
            )

    def validateIdentityProvider(self, request, provider):
        """Check if identity provider registered in DIRAC

        :param object request: request
        :param str provider: provider name

        :return: OAuth2Request object or HTML -- new request with provider name or provider selector
        """
        if provider:
            request.provider = provider

        # Find identity provider for group
        groupProvider = getIdPForGroup(request.group) if request.groups else None

        # If access token is requested for a group that is not registered in any identity provider
        # or the requested provider does not match the group, return error
        if request.group and not groupProvider and "proxy" not in request.scope:
            raise Exception(f"The {request.group} group belongs to the VO that is not tied to any Identity Provider.")

        sLog.debug(f"Check if {request.provider} identity provider registered in DIRAC...")
        # Research supported IdPs
        result = getIdProviderIdentifiers()
        if not result["OK"]:
            raise Exception(result["Message"])

        idPs = result["Value"]
        if not idPs:
            raise Exception("No identity providers found.")

        if request.provider:
            if request.provider not in idPs:
                raise Exception(f"{request.provider} identity provider is not registered.")
            elif groupProvider and request.provider != groupProvider:
                raise Exception(
                    'The %s group Identity Provider is "%s" and not "%s".'
                    % (request.group, groupProvider, request.provider)
                )
            return request

        # If no identity provider is specified, it must be assigned
        if groupProvider:
            request.provider = groupProvider
            return request

        # If only one identity provider is registered, then choose it
        if len(idPs) == 1:
            request.provider = idPs[0]
            return request

        # Choose IdP HTML interface
        with dom.div(cls="row m-5 justify-content-md-center") as tag:
            for idP in idPs:
                result = gConfig.getOptionsDict(f"/Resources/IdProviders/{idP}")
                if result["OK"]:
                    logo = result["Value"].get("logoURL")
                    with dom.div(cls="col-md-6 p-2").add(dom.div(cls="card shadow-lg h-100 border-0")):
                        with dom.div(cls="row m-2 justify-content-md-center align-items-center h-100"):
                            with dom.div(cls="col-auto"):
                                dom.h2(idP)
                                dom.a(
                                    href=f"{self.LOCATION}/authorization/{idP}?{request.query}",
                                    cls="stretched-link",
                                )
                            if logo:
                                dom.div(dom.img(src=logo, cls="card-img"), cls="col-auto")

        # Render into HTML
        return getHTML(
            "Identity Provider selection..",
            body=tag,
            icon="fingerprint",
            info="Dirac itself is not an Identity Provider. " "You will need to select one to continue.",
        )

    def __registerNewUser(self, provider, payload):
        """Register new user

        :param str provider: provider
        :param dict payload: user information dictionary

        :return: S_OK()/S_ERROR()
        """
        from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

        username = payload["sub"]

        mail = {}
        mail["subject"] = f"[DIRAC AS] User {username} to be added."
        mail["body"] = f"User {username} was authenticated by {provider}."
        mail["body"] += f"\n\nNew user to be added with the following information:\n{pprint.pformat(payload)}"
        mail["body"] += f"\n\nPlease, add '{wrapIDAsDN(username)}' to /Register/Users/<username>/DN option.\n"
        mail["body"] += "\n\n------"
        mail["body"] += "\n This is a notification from the DIRAC authorization service, please do not reply.\n"
        result = S_OK()
        for addresses in getEmailsForGroup("dirac_admin"):
            result = NotificationClient().sendMail(addresses, mail["subject"], mail["body"], localAttempt=False)
            if not result["OK"]:
                sLog.error(result["Message"])
        if result["OK"]:
            sLog.info(result["Value"], "administrators have been notified about a new user.")
        return result
