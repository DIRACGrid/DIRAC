""" This handler basically provides a REST interface to interact with the OAuth 2 authentication server

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN Auth:
      :end-before: ##END
      :dedent: 2
      :caption: Auth options
"""
import json
import pprint

from dominate import tags as dom

from DIRAC import gConfig
from DIRAC.Core.Tornado.Server.TornadoREST import location, TornadoREST
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getIdPForGroup, getGroupsForUser
from DIRAC.FrameworkSystem.private.authorization.AuthServer import AuthServer
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import createOAuth2Request
from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import DeviceAuthorizationEndpoint
from DIRAC.FrameworkSystem.private.authorization.grants.RevokeToken import RevocationEndpoint
from DIRAC.FrameworkSystem.private.authorization.utils.Utilities import getHTML


class AuthHandler(TornadoREST):
    # Authorization access to all methods handled by AuthServer instance
    DEFAULT_AUTHENTICATION = ["JWT", "VISITOR"]
    DEFAULT_AUTHORIZATION = "all"
    DEFAULT_LOCATION = "/auth"

    @classmethod
    def initializeHandler(cls, serviceInfo):
        """This method is called only one time, at the first request

        :param dict ServiceInfoDict: infos about services
        """
        cls.server = AuthServer()
        cls.server.LOCATION = cls.DEFAULT_LOCATION

    def initializeRequest(self):
        """Called at every request"""
        self.currentPath = self.request.protocol + "://" + self.request.host + self.request.path

    @location(".well-known/(?:oauth-authorization-server|openid-configuration)")
    def get_index(self):
        """Well known endpoint, specified by
        `RFC8414 <https://tools.ietf.org/html/rfc8414#section-3>`_

        Request examples::

          GET: LOCATION/.well-known/openid-configuration
          GET: LOCATION/.well-known/oauth-authorization-server

        Responce::

          HTTP/1.1 200 OK
          Content-Type: application/json

          {
            "registration_endpoint": "https://domain.com/auth/register",
            "userinfo_endpoint": "https://domain.com/auth/userinfo",
            "jwks_uri": "https://domain.com/auth/jwk",
            "code_challenge_methods_supported": [
              "S256"
            ],
            "grant_types_supported": [
              "authorization_code",
              "code",
              "refresh_token"
            ],
            "token_endpoint": "https://domain.com/auth/token",
            "response_types_supported": [
              "code",
              "device",
              "id_token token",
              "id_token",
              "token"
            ],
            "authorization_endpoint": "https://domain.com/auth/authorization",
            "issuer": "https://domain.com/auth"
          }
        """
        if self.request.method == "GET":
            resDict = dict(
                setups=gConfig.getSections("DIRAC/Setups").get("Value", []),
                configuration_server=gConfig.getValue("/DIRAC/Configuration/MasterServer", ""),
            )
            resDict.update(self.server.metadata)
            resDict.pop("Clients", None)
            return resDict

    def get_jwk(self):
        """JWKs endpoint

        Request example::

          GET LOCATION/jwk

        Response::

          HTTP/1.1 200 OK
          Content-Type: application/json

          {
            "keys": [
              {
                "e": "AQAB",
                "kty": "RSA",
                "n": "3Vv5h5...X3Y7k"
              }
            ]
          }
        """
        result = self.server.db.getKeySet()
        return result["Value"].as_dict() if result["OK"] else {}

    def post_revoke(self):
        """Revocation endpoint

        Request example::

          GET LOCATION/revoke

        Response::

          HTTP/1.1 200 OK
          Content-Type: application/json
        """
        self.log.verbose("Initialize a Device authentication flow.")
        return self.server.create_endpoint_response(RevocationEndpoint.ENDPOINT_NAME, self.request)

    def get_userinfo(self):
        """The UserInfo endpoint can be used to retrieve identity information about a user,
        see `spec <https://openid.net/specs/openid-connect-core-1_0.html#UserInfo>`_

        GET LOCATION/userinfo

        Parameters:
        +---------------+--------+---------------------------------+--------------------------------------------------+
        | **name**      | **in** | **description**                 | **example**                                      |
        +---------------+--------+---------------------------------+--------------------------------------------------+
        | Authorization | header | Provide access token            | Bearer jkagfbfd3r4ubf887gqduyqwogasd87           |
        +---------------+--------+---------------------------------+--------------------------------------------------+

        Request example::

          GET LOCATION/userinfo
          Authorization: Bearer <access_token>

        Response::

          HTTP/1.1 200 OK
          Content-Type: application/json

          {
            "sub": "248289761001",
            "name": "Bob Smith",
            "given_name": "Bob",
            "family_name": "Smith",
            "group": [
              "dirac_user",
              "dirac_admin"
            ]
          }
        """
        return self.getRemoteCredentials()

    def post_device(self, provider=None, user_code=None, client_id=None):
        """The device authorization endpoint can be used to request device and user codes.
        This endpoint is used to start the device flow authorization process and user code verification.

        POST LOCATION/device/<provider>?<query>

        Parameters:
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | **name**       | **in** | **description**                           | **example**                           |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | user code      | query  | in the last step to confirm recived user  | WE8R-WEN9                             |
        |                |        | code put it as query parameter (optional) |                                       |
        |                |        | It's possible to add it interactively.    |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | client_id      | query  | The public client ID                      | 3f6eNw0E6JGq1VuzRkpWUL9XTxhL86efZw    |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | scope          | query  | list of scoupes separated by a space, to  | g:dirac_user                          |
        |                |        | add a group you must add "g:" before the  |                                       |
        |                |        | group name                                |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | provider       | path   | identity provider to autorize (optional)  | CheckIn                               |
        |                |        | It's possible to add it interactively.    |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+

        Request example, to initialize a Device authentication flow::

          POST LOCATION/device/CheckIn_dev?client_id=3f1DAj8z6eNw0E6JGq1Vu6efZwyV&scope=g:dirac_admin

        Response::

          HTTP/1.1 200 OK
          Content-Type: application/json

          {
            "device_code": "TglwLiow0HUwowjB9aHH5HqH3bZKP9d420LkNhCEuR",
            "verification_uri": "https://marosvn32.in2p3.fr/auth/device",
            "interval": 5,
            "expires_in": 1800,
            "verification_uri_complete": "https://marosvn32.in2p3.fr/auth/device/WSRL-HJMR",
            "user_code": "WSRL-HJMR"
          }

        Request example, to confirm the user code::

          POST LOCATION/device/CheckIn_dev/WSRL-HJMR

        Response::

          HTTP/1.1 200 OK
        """
        self.log.verbose("Initialize a Device authentication flow.")
        return self.server.create_endpoint_response(DeviceAuthorizationEndpoint.ENDPOINT_NAME, self.request)

    def get_device(self, provider=None, user_code=None, client_id=None):
        """The device authorization endpoint can be used to request device and user codes.
        This endpoint is used to start the device flow authorization process and user code verification.

        User code confirmation::

          GET LOCATION/device/<provider>?user_code=<user code>

        Response::

          HTTP/1.1 200 OK
        """

        if user_code:
            # If received a request with a user code, then prepare a request to authorization endpoint.
            self.log.verbose("User code verification.")
            result = self.server.db.getSessionByUserCode(user_code)
            if not result["OK"] or not result["Value"]:
                return getHTML(
                    "session is expired.",
                    theme="warning",
                    body=result.get("Message"),
                    info="Seems device code flow authorization session %s expired." % user_code,
                )
            session = result["Value"]
            # Get original request from session
            req = createOAuth2Request(dict(method="GET", uri=session["uri"]))
            req.setQueryArguments(id=session["id"], user_code=user_code)

            # Save session to cookie and redirect to authorization endpoint
            authURL = "{}?{}".format(req.path.replace("device", "authorization"), req.query)
            return self.server.handle_response(302, {}, [("Location", authURL)], session)

        # If received a request without a user code, then send a form to enter the user code
        with dom.div(cls="row mt-5 justify-content-md-center") as tag:
            with dom.div(cls="col-auto"):
                dom.div(
                    dom.form(
                        dom._input(type="text", name="user_code"),
                        dom.button("Submit", type="submit", cls="btn btn-submit"),
                        action=self.currentPath,
                        method="GET",
                    ),
                    cls="card",
                )
        return getHTML(
            "user code verification..",
            body=tag,
            icon="ticket-alt",
            info="Device flow required user code. You will need to type user code to continue.",
        )

    def get_authorization(self, provider=None, **kwargs):
        """Authorization endpoint

        GET: LOCATION/authorization/<provider>

        Parameters:
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | **name**       | **in** | **description**                           | **example**                           |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | response_type  | query  | informs of the desired grant type         | code                                  |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | client_id      | query  | The client ID                             | 3f6eNw0E6JGq1VuzRkpWUL9XTxhL86efZw    |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | scope          | query  | list of scoupes separated by a space, to  | g:dirac_user                          |
        |                |        | add a group you must add "g:" before the  |                                       |
        |                |        | group name                                |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | provider       | path   | identity provider to autorize (optional)  | CheckIn                               |
        |                |        | It's possible to add it interactively.    |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+

        General options:
          provider -- identity provider to autorize

        Device flow:
          &user_code=..                         (required)

        Authentication code flow:
          &scope=..                             (optional)
          &redirect_uri=..                      (optional)
          &state=..                             (main session id, optional)
          &code_challenge=..                    (PKCE, optional)
          &code_challenge_method=(pain|S256)    ('pain' by default, optional)
        """
        return self.server.validate_consent_request(self.request, provider)

    def get_redirect(self, state, error=None, error_description="", chooseScope=[]):
        """Redirect endpoint.
        After a user successfully authorizes an application, the authorization server will redirect
        the user back to the application with either an authorization code or access token in the URL.
        The full URL of this endpoint must be registered in the identity provider.

        Read more in `oauth.com <https://www.oauth.com/oauth2-servers/redirect-uris/>`_.
        Specified by `RFC6749 <https://tools.ietf.org/html/rfc6749#section-3.1.2>`_.

        GET LOCATION/redirect

        :param str state: Current IdP session state
        :param str error: IdP error response
        :param str error_description: error description
        :param list chooseScope: to specify new scope(group in our case) (optional)

        :return: S_OK()/S_ERROR()
        """
        # Check current auth session that was initiated for the selected external identity provider
        session = self.get_secure_cookie("auth_session")
        if not session:
            return self.server.handle_response(
                payload=getHTML(
                    "session is expired.",
                    theme="warning",
                    state=400,
                    info="Seems %s session is expired, please, try again." % state,
                ),
                delSession=True,
            )

        sessionWithExtIdP = json.loads(session)
        if state and not sessionWithExtIdP.get("state") == state:
            return self.server.handle_response(
                payload=getHTML(
                    "session is expired.",
                    theme="warning",
                    state=400,
                    info="Seems %s session is expired, please, try again." % state,
                ),
                delSession=True,
            )

        # Try to catch errors if the authorization on the selected identity provider was unsuccessful
        if error:
            provider = sessionWithExtIdP.get("Provider")
            return self.server.handle_response(
                payload=getHTML(
                    error,
                    theme="error",
                    body=error_description,
                    info=f"Seems {state} session is failed on the {provider}'s' side.",
                ),
                delSession=True,
            )

        if not sessionWithExtIdP.get("authed"):
            # Parse result of the second authentication flow
            self.log.info("%s session, parsing authorization response:\n" % state, self.request.uri)

            result = self.server.parseIdPAuthorizationResponse(self.request, sessionWithExtIdP)
            if not result["OK"]:
                if result["Message"].startswith("<!DOCTYPE html>"):
                    return self.server.handle_response(payload=result["Message"], delSession=True)
                return self.server.handle_response(
                    payload=getHTML("server error", state=500, info=result["Message"]), delSession=True
                )
            # Return main session flow
            sessionWithExtIdP["authed"] = result["Value"]

        # Research group
        grant_user, response = self.__researchDIRACGroup(sessionWithExtIdP, chooseScope, state)
        if not grant_user:
            return response

        # RESPONSE to basic DIRAC client request
        resp = self.server.create_authorization_response(response, grant_user)
        if isinstance(resp.payload, str) and not resp.payload.startswith("<!DOCTYPE html>"):
            resp.payload = getHTML("authorization response", state=resp.status_code, body=resp.payload)
        return resp

    def post_token(self):
        """The token endpoint, the description of the parameters will differ depending on the selected grant_type

        POST LOCATION/token

        Parameters:
        +----------------+--------+-------------------------------+---------------------------------------------------+
        | **name**       | **in** | **description**               | **example**                                       |
        +----------------+--------+-------------------------------+---------------------------------------------------+
        | grant_type     | query  | grant type to use             | urn:ietf:params:oauth:grant-type:device_code      |
        +----------------+--------+-------------------------------+---------------------------------------------------+
        | client_id      | query  | The public client ID          | 3f1DAj8z6eNw0E6JGq1VuzRkpWUL9XTxhL86efZw          |
        +----------------+--------+-------------------------------+---------------------------------------------------+
        | device_code    | query  | device code                   | uW5xL4hr2tqwBPKL5d0JO9Fcc67gLqhJsNqYTSp           |
        +----------------+--------+-------------------------------+---------------------------------------------------+

        :mod:`Supported grant types <DIRAC.FrameworkSystem.private.authorization.grants>`

        Request example::

          POST LOCATION/token?client_id=L86..yV&grant_type=urn:ietf:params:oauth:grant-type:device_code&device_code=uW5

        Response::

          HTTP/1.1 400 OK
          Content-Type: application/json

          {
            "error": "authorization_pending"
          }
        """
        return self.server.create_token_response(self.request)

    def __researchDIRACGroup(self, extSession, chooseScope, state):
        """Research DIRAC groups for authorized user

        :param dict extSession: ended authorized external IdP session

        :return: -- will return (None, response) to provide error or group selector
                    will return (grant_user, request) to contionue authorization with choosed group
        """
        # Base DIRAC client auth session
        firstRequest = createOAuth2Request(extSession["firstRequest"])
        # Read requested groups by DIRAC client or user
        firstRequest.addScopes(chooseScope)
        # Read already authed user
        username = extSession["authed"]["username"]
        # Requested arguments in first request
        provider = firstRequest.provider
        self.log.debug("Next groups has been found for %s:" % username, ", ".join(firstRequest.groups))

        # Researche Group
        result = getGroupsForUser(username)
        if not result["OK"]:
            return None, self.server.handle_response(
                payload=getHTML("server error", theme="error", info=result["Message"]), delSession=True
            )
        groups = result["Value"]

        validGroups = [
            group for group in groups if (getIdPForGroup(group) == provider) or ("proxy" in firstRequest.scope)
        ]
        if not validGroups:
            return None, self.server.handle_response(
                payload=getHTML(
                    "groups not found.",
                    theme="error",
                    info=f"No groups found for {username} and for {provider} Identity Provider.",
                ),
                delSession=True,
            )

        self.log.debug("The state of %s user groups has been checked:" % username, pprint.pformat(validGroups))

        # If group already defined in first request, just return it
        if firstRequest.groups:
            return extSession["authed"], firstRequest

        # If not and we found only one valid group, apply this group
        if len(validGroups) == 1:
            firstRequest.addScopes(["g:%s" % validGroups[0]])
            return extSession["authed"], firstRequest

        # Else give user chanse to choose group in browser
        with dom.div(cls="row mt-5 justify-content-md-center align-items-center") as tag:
            for group in sorted(validGroups):
                vo, gr = group.split("_")
                with dom.div(cls="col-auto p-2").add(dom.div(cls="card shadow-lg border-0 text-center p-2")):
                    dom.h4(vo.upper() + " " + gr, cls="p-2")
                    dom.a(href=f"{self.currentPath}?state={state}&chooseScope=g:{group}", cls="stretched-link")

        html = getHTML(
            "group selection..",
            body=tag,
            icon="users",
            info="Dirac use groups to describe permissions. " "You will need to select one of the groups to continue.",
        )

        return None, self.server.handle_response(payload=html, newSession=extSession)
