""" IdProvider based on OAuth2 protocol
"""
import re
import time
import pprint
import requests
from authlib.jose import JsonWebKey, jwt
from authlib.jose.errors import DecodeError
from authlib.common.urls import url_decode
from authlib.common.security import generate_token
from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope
from authlib.oauth2.rfc6749.parameters import prepare_token_request
from authlib.oauth2.rfc8628 import DEVICE_CODE_GRANT_TYPE
from authlib.integrations.base_client.errors import MissingTokenError, OAuthError
from authlib.integrations.requests_client import OAuth2Session as _OAuth2Session
from authlib.oidc.discovery.well_known import get_well_known_url
from authlib.oauth2.rfc7636 import create_s256_code_challenge

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (
    getVOMSRoleGroupMapping,
    wrapIDAsDN,
    getVOs,
    getGroupOption,
    getAllGroups,
)
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import createOAuth2Request

DEFAULT_HEADERS = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}

gJWKs = ThreadSafe.Synchronizer()
gMetadata = ThreadSafe.Synchronizer()
gRefreshToken = ThreadSafe.Synchronizer()


def claimParser(claimDict, attributes):
    """Parse claims to dictionary with certain keys

    :param dict claimDict: claims
    :param dict attributes: contain claim and regex to parse it

    :return: dict
    """
    profile = {}
    for claim, reg in attributes.items():
        if claim not in claimDict:
            continue
        profile[claim] = {}
        if isinstance(claimDict[claim], dict):
            result = claimParser(claimDict[claim], reg)
            if result:
                profile[claim] = result
        elif isinstance(claimDict[claim], str):
            result = re.compile(reg).match(claimDict[claim])
            if result:
                for k, v in result.groupdict().items():
                    profile[claim][k] = v
        else:
            profile[claim] = []
            for claimItem in claimDict[claim]:
                if isinstance(reg, dict):
                    result = claimParser(claimItem, reg)
                    if result:
                        profile[claim].append(result)
                else:
                    result = re.compile(reg).match(claimItem)
                    if result:
                        profile[claim].append(result.groupdict())

    return profile


class OAuth2Session(_OAuth2Session):
    """Authlib does not yet know about the token exchange flow:
    https://github.com/lepture/authlib/tree/master/authlib/oauth2/rfc8693

    so we will add auxiliary methods to implement this flow.
    """

    def exchange_token(
        self,
        url,
        subject_token=None,
        subject_token_type=None,
        body="",
        auth=None,
        headers=None,
        **kwargs,
    ):
        """Exchange a new access token

        :param url: Exchange Token endpoint, must be HTTPS.
        :param str subject_token: subject_token
        :param str subject_token_type: token type https://tools.ietf.org/html/rfc8693#section-3
        :param body: Optional application/x-www-form-urlencoded body to add the
                     include in the token request. Prefer kwargs over body.
        :param str refresh_token: refresh token
        :param str access_token: access token
        :param auth: An auth tuple or method as accepted by requests.
        :param headers: Dict to default request headers with.
        :return: A :class:`OAuth2Token` object (a dict too).
        """
        session_kwargs = self._extract_session_request_params(kwargs)
        subject_token_type = subject_token_type or "urn:ietf:params:oauth:token-type:access_token"
        if "scope" not in kwargs and self.scope:
            kwargs["scope"] = self.scope

        body = prepare_token_request(
            "urn:ietf:params:oauth:grant-type:token-exchange",
            body,
            subject_token=subject_token,
            subject_token_type=subject_token_type,
            **kwargs,
        )

        if headers is None:
            headers = DEFAULT_HEADERS

        for hook in self.compliance_hook.get("exchange_token_request", []):
            url, headers, body = hook(url, headers, body)

        if auth is None:
            auth = self.client_auth(self.token_endpoint_auth_method)

        return self._exchange_token(url, body=body, headers=headers, auth=auth, **session_kwargs)

    def _exchange_token(self, url, body="", headers=None, auth=None, **kwargs):
        resp = self.session.post(url, data=dict(url_decode(body)), headers=headers, auth=auth, **kwargs)

        for hook in self.compliance_hook.get("exchange_token_response", []):
            resp = hook(resp)

        return self.parse_response_token(resp)


class OAuth2IdProvider(OAuth2Session):
    """Base class to describe the configuration of the OAuth2 client of the corresponding provider."""

    JWKS_REFRESH_RATE = 24 * 3600
    METADATA_REFRESH_RATE = 24 * 3600
    DEFAULT_METADATA = {}

    def __init__(self, **kwargs):
        """Initialization"""
        OAuth2Session.__init__(self, **kwargs)
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.metadata_fetch_last = 0
        self.issuer = self.metadata.get("issuer", "")
        self.scope = self.scope or ""
        self.jwks = kwargs.get("jwks")
        self.verify = kwargs.get("verify", True)  # Decide if need to check CAs
        self.token_placement = kwargs.get("token_placement", "header")
        self.code_challenge_method = "S256"
        self.server_metadata_url = kwargs.get(
            "server_metadata_url", get_well_known_url(self.metadata.get("issuer", ""), True)
        )
        self.jwks_fetch_last = time.time() - self.JWKS_REFRESH_RATE
        meta = self.DEFAULT_METADATA
        meta.update(kwargs)
        self.setParameters(meta)
        self._initialize()
        self.metadata_fetch_last = time.time() - self.METADATA_REFRESH_RATE
        self.log.debug(
            f'"{self.name}" OAuth2 IdP initialization done:',
            f"\nclient_id: {self.client_id}\nmetadata:\n{pprint.pformat(self.metadata)}",
        )

    def _initialize(self):
        """Initialization"""
        self.name = self.parameters.get("ProviderName")

    def setParameters(self, parameters: dict):
        """Set parameters

        :param dict parameters: parameters of the identity Provider
        """
        self.parameters = parameters

    #############################################################################

    def _handleRequest(self, url, withholdToken=False):
        """Get data from url

        :param str url: URL used to get data
        :return: data
        """
        try:
            response = self.get(url, withhold_token=withholdToken, timeout=5)
            data = response.json()
            if "error" in data:
                return S_ERROR(f"Successful connection but error returned: {data['error_description']}")
            return S_OK(data)
        except MissingTokenError as e:
            return S_ERROR(f"Missing token to get access to {url}: {e}")
        except requests.Timeout as e:
            return S_ERROR(f"Connection failed: request timed out, {e}")
        except requests.ConnectionError as e:
            return S_ERROR(f"Connection failed, consider checking the state of the provider: {e}")
        except requests.JSONDecodeError as e:
            return S_ERROR(f"Result cannot be JSON-parsed:\n{response.__dict__}\n{e}")
        except requests.RequestException as e:
            return S_ERROR(f"Request exception: {e}")

    #############################################################################

    def get_metadata(self, option=None):
        """Get metadata

        :param str option: option
        :return: option value
        """
        if self.metadata_fetch_last < (time.time() - self.METADATA_REFRESH_RATE):
            self.log.verbose("Refreshing metadata to get latest values")
            result = self.fetch_metadata()
            if not result["OK"]:
                self.log.verbose("Failed to fetch metadata from the server", result["Message"])
            else:
                self.metadata_fetch_last = time.time()

        return self.metadata.get(option)

    @gMetadata
    def fetch_metadata(self):
        """Fetch metadata"""
        result = self._handleRequest(self.server_metadata_url, withholdToken=True)
        if not result["OK"]:
            return result
        self.metadata.update(result["Value"])
        return S_OK()

    def getJWKs(self):
        """Get JWKs"""
        if self.jwks_fetch_last < (time.time() - self.JWKS_REFRESH_RATE):
            self.log.verbose("Refreshing JWKs to get latest values")
            result = self.fetchJWKs()
            if not result["OK"]:
                self.log.verbose("Failed to fetch JWKs from the server", result["Message"])
            else:
                self.jwks_fetch_last = time.time()

        return self.jwks

    @gJWKs
    def fetchJWKs(self):
        """Fetch JWKs"""
        result = self._handleRequest(self.get_metadata("jwks_uri"), withholdToken=True)
        if not result["OK"]:
            return result
        self.jwks = result["Value"]
        return S_OK()

    #############################################################################

    def getUserProfile(self, accessToken):
        """Get user profile

        :param str accessToken:
        :return: S_OK()/S_ERROR()
        """
        self.token = {"access_token": accessToken}
        return self._handleRequest(self.get_metadata("userinfo_endpoint"))

    def getUserGroups(self, accessToken):
        """Get user groups

        :param str payload: token payload
        :param str token: access token

        :return: S_OK(dict)/S_ERROR()
        """
        result = self.verifyToken(accessToken)
        if not result["OK"]:
            return result

        payload = result["Value"]

        credDict = self._parseBasic(payload)
        if not credDict.get("DIRACGroups"):
            credDict.update(self._parseEduperson(payload))
        if credDict.get("DIRACGroups"):
            self.log.debug("Found groups:", ", ".join(credDict["DIRACGroups"]))
            credDict["group"] = credDict["DIRACGroups"][0]
        return S_OK(credDict)

    def _parseBasic(self, claimDict):
        """Parse basic claims

        :param dict claimDict: claims

        :return: S_OK(dict)/S_ERROR()
        """
        self.log.debug("Token payload:", pprint.pformat(claimDict))
        credDict = {}
        credDict["ID"] = claimDict["sub"]
        credDict["DN"] = wrapIDAsDN(credDict["ID"])
        if claimDict.get("scope"):
            self.log.debug(f"Search groups for {claimDict['scope']} scope.")
            credDict["DIRACGroups"] = self.getScopeGroups(claimDict["scope"])
        return credDict

    def _parseEduperson(self, claimDict):
        """Parse eduperson claims

        :return: dict
        """
        vos = {}
        credDict = {}
        attributes = {
            "eduperson_unique_id": "^(?P<ID>.*)",
            "eduperson_entitlement": "%s:%s"
            % (
                "^(?P<NAMESPACE>[A-z,.,_,-,:]+):(group:registry|group)",
                "(?P<VO>[A-z,.,_,-]+):role=(?P<VORole>[A-z,.,_,-]+)[:#].*",
            ),
        }
        self.log.debug("Try to parse eduperson claims..")
        # Parse eduperson claims
        resDict = claimParser(claimDict, attributes)
        if resDict.get("eduperson_unique_id"):
            self.log.debug("Found eduperson_unique_id claim:", pprint.pformat(resDict["eduperson_unique_id"]))
            credDict["ID"] = resDict["eduperson_unique_id"]["ID"]
        if resDict.get("eduperson_entitlement"):
            self.log.debug("Found eduperson_entitlement claim:", pprint.pformat(resDict["eduperson_entitlement"]))
            for voDict in resDict["eduperson_entitlement"]:
                if voDict["VO"] not in vos:
                    vos[voDict["VO"]] = {"VORoles": []}
                if voDict["VORole"] not in vos[voDict["VO"]]["VORoles"]:
                    vos[voDict["VO"]]["VORoles"].append(voDict["VORole"])

            allowedVOs = getVOs()["Value"]  # Always return S_OK()
            # Search DIRAC groups
            for vo in vos:
                # Skip VO if it absent in Registry
                if vo in allowedVOs and (result := getVOMSRoleGroupMapping(vo))["OK"]:
                    for role in vos[vo]["VORoles"]:
                        groups = result["Value"]["VOMSDIRAC"].get(f"/{vo}/{role}")
                        if groups:
                            credDict["DIRACGroups"] = list(set(credDict.get("DIRACGroups", []) + groups))
        return credDict

    @deprecated("Use getUserProfile instead")
    def researchGroup(self, payload=None, token=None):
        if not token:
            token = self.token
        self.getUserProfile(token.get("access_token"))

    #############################################################################

    def getGroupScopes(self, group: str) -> list[str]:
        """Get group scopes

        :param group: DIRAC group
        """
        idPScope = getGroupOption(group, "IdPRole")
        return scope_to_list(idPScope) if idPScope else []

    def getScopeGroups(self, scope: str) -> list[str]:
        """Get DIRAC groups related to scope"""
        groups = []
        for group in getAllGroups():
            if (g_scope := self.getGroupScopes(group)) and set(g_scope).issubset(scope_to_list(scope)):
                groups.append(group)
        return groups

    #############################################################################

    def verifyToken(self, accessToken):
        """Verify access token

        :param str accessToken: access token
        :param dict jwks: JWKs

        :return: dict
        """
        if not accessToken:
            return S_ERROR("Access token is empty")

        # Get JWKs
        result = self.getJWKs()
        if not result:
            return S_ERROR("JWKs not found")
        jwks = result

        # Try to decode and verify an access token
        self.log.debug(f"Try to decode token {accessToken} with JWKs:\n", pprint.pformat(jwks))
        try:
            return S_OK(jwt.decode(accessToken, JsonWebKey.import_key_set(jwks)))
        except DecodeError as e:
            return S_ERROR(f"The provided token cannot be decoded: {e}")

    #############################################################################

    @gRefreshToken
    def refreshToken(self, refreshToken, group=None, **kwargs):
        """Refresh token

        :param str token: refresh_token
        :param str group: DIRAC group

        :return: dict
        """
        if not refreshToken:
            return S_ERROR("Refresh token is empty")
        if group:
            # If group set add group scopes to request
            if not (groupScopes := self.getGroupScopes(group)):
                return S_ERROR(f"No scope found for {group}")

            kwargs.update(dict(scope=list_to_scope(groupScopes)))

        try:
            token = self.refresh_token(self.get_metadata("token_endpoint"), refresh_token=refreshToken, **kwargs)
            return S_OK(OAuth2Token(dict(token)))
        except OAuthError as e:
            return S_ERROR(f"Invalid refresh token: {e}")
        except Exception as e:
            self.log.exception(e)
            return S_ERROR(repr(e))

    @gRefreshToken
    def fetchToken(self, **kwargs):
        """Fetch token

        :return: dict
        """
        try:
            self.fetch_access_token(self.get_metadata("token_endpoint"), **kwargs)
        except OAuthError as e:
            return S_ERROR(f"Cannot fetch access token: {e}")
        except Exception as e:
            self.log.exception(e)
            return S_ERROR(repr(e))
        self.token["client_id"] = self.client_id
        self.token["provider"] = self.name
        return S_OK(OAuth2Token(dict(self.token)))

    def exchangeToken(self, accessToken, group=None, scope=None):
        """Get new tokens for group scope

        :param str accessToken: access token
        :param str group: requested group
        :param list scope: requested scope

        :return: dict -- token
        """
        if not accessToken:
            return S_ERROR("Access token is empty")

        scope = scope or scope_to_list(self.scope)
        if group:
            if not (groupScopes := self.getGroupScopes(group)):
                return S_ERROR(f"No scope found for {group}")
            scope = list(set(scope + groupScopes))
        scope = list_to_scope(scope)

        try:
            token = self.exchange_token(
                self.get_metadata("token_endpoint"),
                subject_token=accessToken,
                subject_token_type="urn:ietf:params:oauth:token-type:access_token",
                scope=scope,
            )
            return S_OK(OAuth2Token(dict(token)))
        except OAuthError as e:
            return S_ERROR(f"Invalid access token: {e}")
        except Exception as e:
            self.log.exception(e)
            return S_ERROR(repr(e))

    def revokeToken(self, token=None, tokenTypeHint="refresh_token"):
        """Revoke token

        :param str token: access or refresh token
        :param str tokenTypeHint: token type

        :return: S_OK()/S_ERROR()
        """
        if not token:
            return S_ERROR("Token is empty")

        try:
            self.revoke_token(self.get_metadata("revocation_endpoint"), token=token, token_type_hint=tokenTypeHint)
        except Exception as e:
            self.log.exception(e)
            return S_ERROR(repr(e))
        return S_OK()

    #############################################################################

    def deviceAuthorization(self, group=None):
        """Authorization through DeviceCode flow"""
        result = self.submitDeviceCodeAuthorizationFlow(group)
        if not result["OK"]:
            return result
        response = result["Value"]

        # Notify user to go to authorization endpoint
        if response.get("verification_uri_complete"):
            showURL = f"Use the following link to continue\n{response['verification_uri_complete']}"
        else:
            showURL = 'Use the following link to continue, your user code is "{}"\n{}'.format(
                response["user_code"],
                response["verification_uri"],
            )
        self.log.notice(showURL)
        try:
            return self.waitFinalStatusOfDeviceCodeAuthorizationFlow(response["device_code"])
        except KeyboardInterrupt:
            return S_ERROR("User canceled the operation..")

    def submitNewSession(self, pkce=True):
        """Submit new authorization session

        :param bool pkce: use PKCE

        :return: S_OK(str)/S_ERROR()
        """
        session = {}
        params = dict(state=generate_token(10))
        # Create PKCE verifier
        if pkce:
            session["code_verifier"] = generate_token(48)
            params["code_challenge_method"] = "S256"
            params["code_challenge"] = create_s256_code_challenge(session["code_verifier"])
        url, state = self.create_authorization_url(self.get_metadata("authorization_endpoint"), **params)
        return url, state, session

    def parseAuthResponse(self, response, session=None):
        """Make user info dict:

        :param dict response: response on request to get user profile
        :param object session: session

        :return: S_OK((dict, dict))/S_ERROR()
        """
        response = createOAuth2Request(response)

        self.log.debug("Try to parse authentication response:", pprint.pformat(response.data))

        if not session:
            session = {}

        self.log.debug("Current session is:\n", pprint.pformat(session))

        self.fetchToken(authorization_response=response.uri, code_verifier=session.get("code_verifier"))

        result = self.verifyToken(self.token["access_token"])
        if not result["OK"]:
            return result
        payload = result["Value"]
        result = self.getUserGroups(self.token.get("access_token"))
        if not result["OK"]:
            return result
        credDict = result["Value"]
        self.log.debug("Got response dictionary:\n", pprint.pformat(credDict))

        # Store token
        self.token["user_id"] = credDict["ID"]

        return S_OK((credDict, payload))

    def submitDeviceCodeAuthorizationFlow(self, group=None):
        """Submit authorization flow

        :return: S_OK(dict)/S_ERROR() -- dictionary with device code flow response
        """
        groupScopes = []
        if group:
            if not (groupScopes := self.getGroupScopes(group)):
                return S_ERROR(f"No scope found for {group}")

        try:
            r = requests.post(
                self.get_metadata("device_authorization_endpoint"),
                data=dict(client_id=self.client_id, scope=list_to_scope(scope_to_list(self.scope) + groupScopes)),
                verify=self.verify,
            )
            r.raise_for_status()
            deviceResponse = r.json()
            if "error" in deviceResponse:
                return S_ERROR(f"{deviceResponse['error']}: {deviceResponse.get('description', '')}")

            # Check if all main keys are present here
            for k in ["user_code", "device_code", "verification_uri"]:
                if not deviceResponse.get(k):
                    return S_ERROR(f"Mandatory {k} key is absent in authentication response.")

            return S_OK(deviceResponse)
        except requests.exceptions.Timeout:
            return S_ERROR("Authentication server is not answer, timeout.")
        except requests.exceptions.RequestException as ex:
            return S_ERROR(repr(ex))
        except Exception as ex:
            return S_ERROR(f"Cannot read authentication response: {repr(ex)}")

    def waitFinalStatusOfDeviceCodeAuthorizationFlow(self, deviceCode, interval=5, timeout=300):
        """Submit waiting loop process, that will monitor current authorization session status

        :param str deviceCode: received device code
        :param int interval: waiting interval
        :param int timeout: max time of waiting

        :return: S_OK(dict)/S_ERROR() - dictionary contain access/refresh token and some metadata
        """
        __start = time.time()

        self.log.notice("Authorization pending.. (use CNTL + C to stop)")
        while True:
            time.sleep(int(interval))
            if time.time() - __start > timeout:
                return S_ERROR("Time out.")
            r = requests.post(
                self.get_metadata("token_endpoint"),
                data=dict(client_id=self.client_id, grant_type=DEVICE_CODE_GRANT_TYPE, device_code=deviceCode),
                verify=self.verify,
            )
            token = r.json()
            if not token:
                return S_ERROR("Received token is empty!")
            if "error" not in token:
                self.token = token
                return S_OK(token)
            if token["error"] != "authorization_pending":
                return S_ERROR((token.get("error") or "unknown") + " : " + (token.get("error_description") or ""))
