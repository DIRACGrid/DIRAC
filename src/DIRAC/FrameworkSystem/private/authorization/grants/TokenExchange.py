"""
    authlib.oauth2.rfc6749.grants.refresh_token
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    A special grant endpoint for refresh_token grant_type. Refreshing an
    Access Token per `Section 6`_.

    .. _`Section 6`: https://tools.ietf.org/html/rfc6749#section-6
"""

import logging
from authlib.oauth2.rfc6749.grants.base import BaseGrant, TokenEndpointMixin
from authlib.oauth2.rfc6749.util import scope_to_list
from authlib.oauth2.rfc6749.errors import (
    InvalidRequestError,
    InvalidScopeError,
    InvalidGrantError,
    UnauthorizedClientError,
)
log = logging.getLogger(__name__)

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import BearerTokenValidator


class _TokenExchangeGrant(BaseGrant, TokenEndpointMixin):
  """ A special grant endpoint for urn:ietf:params:oauth:grant-type:token-exchange grant_type.
      Exchanging an Access Token per `Section 6`_.

  .. _`Section 6`: https://tools.ietf.org/html/rfc6749#section-6
  """
  GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:token-exchange'

  #: The authorization server MAY issue a new refresh token
  INCLUDE_NEW_REFRESH_TOKEN = False

  def _validate_request_client(self):
    # require client authentication for confidential clients or for any
    # client that was issued client credentials (or with other
    # authentication requirements)
    client = self.authenticate_token_endpoint_client()
    log.debug('Validate token request of %r', client)

    if not client.check_grant_type(self.GRANT_TYPE):
      raise UnauthorizedClientError()

    return client

  def _validate_request_token(self, client):
    subject_token = self.request.form.get('subject_token')
    if subject_token is None:
      raise InvalidRequestError('Missing "subject_token" in request.')

    subject_token_type = self.request.form.get('subject_token_type')
    if subject_token_type is None:
      raise InvalidRequestError('Missing "subject_token_type" in request.')

    actor_token = self.request.form.get('actor_token')
    actor_token_type = self.request.form.get('actor_token_type')
    if actor_token and actor_token_type is None:
      raise InvalidRequestError('Missing "actor_token_type" in request.')

    token = self.authenticate_subject_token(subject_token, subject_token_type)
    if not token or token.get_client_id() != client.get_client_id():
      raise InvalidGrantError()
    return token

  def _validate_token_scope(self, token):
    scope = self.request.scope
    if not scope:
      return

    original_scope = token.get_scope()
    if not original_scope:
      raise InvalidScopeError()

  def validate_token_request(self):
    """ A client requests a security token by making a token request to the
        authorization server's token endpoint using the extension grant type
        mechanism defined in Section 4.5 of [RFC6749].

        Client authentication to the authorization server is done using the
        normal mechanisms provided by OAuth 2.0.  Section 2.3.1 of [RFC6749]
        defines password-based authentication of the client, however, client
        authentication is extensible and other mechanisms are possible.  For
        example, [RFC7523] defines client authentication using bearer JSON
        Web Tokens (JWTs) [JWT].  The supported methods of client
        authentication and whether or not to allow unauthenticated or
        unidentified clients are deployment decisions that are at the
        discretion of the authorization server.  Note that omitting client
        authentication allows for a compromised token to be leveraged via an
        STS into other tokens by anyone possessing the compromised token.
        Thus, client authentication allows for additional authorization
        checks by the STS as to which entities are permitted to impersonate
        or receive delegations from other entities.

        The client makes a token exchange request to the token endpoint with
        an extension grant type using the HTTP "POST" method.  The following
        parameters are included in the HTTP request entity-body using the
        "application/x-www-form-urlencoded" format with a character encoding
        of UTF-8 as described in Appendix B of [RFC6749], per Section 6:

        grant_type
            REQUIRED.  The value "urn:ietf:params:oauth:grant-type:token-
            exchange" indicates that a token exchange is being performed.

        resource
            OPTIONAL.  A URI that indicates the target service or resource
            where the client intends to use the requested security token.
            This enables the authorization server to apply policy as
            appropriate for the target, such as determining the type and
            content of the token to be issued or if and how the token is to be
            encrypted.  In many cases, a client will not have knowledge of the
            logical organization of the systems with which it interacts and
            will only know a URI of the service where it intends to use the
            token.  The "resource" parameter allows the client to indicate to
            the authorization server where it intends to use the issued token
            by providing the location, typically as an https URL, in the token
            exchange request in the same form that will be used to access that
            resource.  The authorization server will typically have the
            capability to map from a resource URI value to an appropriate
            policy.  The value of the "resource" parameter MUST be an absolute
            URI, as specified by Section 4.3 of [RFC3986], that MAY include a
            query component and MUST NOT include a fragment component.
            Multiple "resource" parameters may be used to indicate that the
            issued token is intended to be used at the multiple resources
            listed.  See [OAUTH-RESOURCE] for additional background and uses
            of the "resource" parameter.

        audience
            OPTIONAL.  The logical name of the target service where the client
            intends to use the requested security token.  This serves a
            purpose similar to the "resource" parameter but with the client
            providing a logical name for the target service.  Interpretation
            of the name requires that the value be something that both the
            client and the authorization server understand.  An OAuth client
            identifier, a SAML entity identifier [OASIS.saml-core-2.0-os], and
            an OpenID Connect Issuer Identifier [OpenID.Core] are examples of
            things that might be used as "audience" parameter values.
            However, "audience" values used with a given authorization server
            must be unique within that server to ensure that they are properly
            interpreted as the intended type of value.  Multiple "audience"
            parameters may be used to indicate that the issued token is
            intended to be used at the multiple audiences listed.  The
            "audience" and "resource" parameters may be used together to
            indicate multiple target services with a mix of logical names and
            resource URIs.

        scope
            OPTIONAL.  A list of space-delimited, case-sensitive strings, as
            defined in Section 3.3 of [RFC6749], that allow the client to
            specify the desired scope of the requested security token in the
            context of the service or resource where the token will be used.
            The values and associated semantics of scope are service specific
            and expected to be described in the relevant service
            documentation.

        requested_token_type
            OPTIONAL.  An identifier, as described in Section 3, for the type
            of the requested security token.  If the requested type is
            unspecified, the issued token type is at the discretion of the
            authorization server and may be dictated by knowledge of the
            requirements of the service or resource indicated by the
            "resource" or "audience" parameter.

        subject_token
            REQUIRED.  A security token that represents the identity of the
            party on behalf of whom the request is being made.  Typically, the
            subject of this token will be the subject of the security token
            issued in response to the request.

        subject_token_type
            REQUIRED.  An identifier, as described in Section 3, that
            indicates the type of the security token in the "subject_token"
            parameter.

        actor_token
            OPTIONAL.  A security token that represents the identity of the
            acting party.  Typically, this will be the party that is
            authorized to use the requested security token and act on behalf
            of the subject.

        actor_token_type
            An identifier, as described in Section 3, that indicates the type
            of the security token in the "actor_token" parameter.  This is
            REQUIRED when the "actor_token" parameter is present in the
            request but MUST NOT be included otherwise.
    """
    client = self._validate_request_client()
    self.request.client = client
    token = self._validate_request_token(client)
    self._validate_token_scope(token)
    self.request.credential = token

  def create_token_response(self):
    """If valid and authorized, the authorization server issues an access
    token as described in Section 5.1.  If the request failed
    verification or is invalid, the authorization server returns an error
    response as described in Section 5.2.
    """
    credential = self.request.credential
    user = self.authenticate_user(credential)
    if not user:
      raise InvalidRequestError('There is no "user" for this token.')

    client = self.request.client
    token = self.issue_token(user, credential)
    log.debug('Issue token %r to %r', token, client)

    self.request.user = user
    self.save_token(token)
    self.execute_hook('process_token', token=token)
    self.revoke_old_credential(credential)
    return 200, token, self.TOKEN_RESPONSE_HEADER

  def issue_token(self, user, credential):
    expires_in = credential.get_expires_in()
    scope = self.request.scope
    if not scope:
      scope = credential.get_scope()

    token = self.generate_token(user=user, expires_in=expires_in, scope=scope,
                                include_refresh_token=self.INCLUDE_NEW_REFRESH_TOKEN)
    return token

  def authenticate_subject_token(self, subject_token, subject_token_type):
    """Get token information with subject_token string. Developers MUST
    implement this method in subclass::

        def authenticate_subject_token(self, subject_token, subject_token_type):
            item = Token.get(**{subject_token_type: subject_token)
            if item and item.is_refresh_token_active():
                return item

    :param subject_token: The token issued to the client
    :param subject_token_type: The type of the token issued to the client
    :return: token
    """
    raise NotImplementedError()

  def authenticate_user(self, credential):
    """Authenticate the user related to this credential. Developers MUST
    implement this method in subclass::

        def authenticate_user(self, credential):
            return User.query.get(credential.user_id)

    :param credential: Token object
    :return: user
    """
    raise NotImplementedError()

  def revoke_old_credential(self, credential):
    """The authorization server MAY revoke the old refresh token after
    issuing a new refresh token to the client. Developers MUST implement
    this method in subclass::

        def revoke_old_credential(self, credential):
            credential.revoked = True
            credential.save()

    :param credential: Token object
    """
    raise NotImplementedError()


TOKEN_TYPE_IDENTIFIERS = [
    # Indicates that the token is an OAuth 2.0 access token issued by
    # the given authorization server.
    'urn:ietf:params:oauth:token-type:access_token',
    # Indicates that the token is an OAuth 2.0 refresh token issued by
    # the given authorization server.
    'urn:ietf:params:oauth:token-type:refresh_token',
    # Indicates that the token is an ID Token as defined in Section 2 of
    # [OpenID.Core].
    'urn:ietf:params:oauth:token-type:id_token',
    # Indicates that the token is a base64url-encoded SAML 1.1
    # [OASIS.saml-core-1.1] assertion.
    'urn:ietf:params:oauth:token-type:saml1',
    # Indicates that the token is a base64url-encoded SAML 2.0
    # [OASIS.saml-core-2.0-os] assertion.
    'urn:ietf:params:oauth:token-type:saml2'
]


class TokenExchangeGrant(_TokenExchangeGrant):
  def __init__(self, *args, **kwargs):
    super(TokenExchangeGrant, self).__init__(*args, **kwargs)
    self.validator = BearerTokenValidator()

  def authenticate_subject_token(self, subject_token, subject_token_type):
    """ Get credential for token

        :param str subject_token: subject_token
        :param str subject_token_type: token type https://tools.ietf.org/html/rfc8693#section-3

        :return: object
    """
    if subject_token_type.split(':')[-1] != 'refresh_token':
      raise InvalidRequestError('Please set refresh_token to "subject_token" in request.')

    # Check auth session
    session = self.server.getSession(subject_token)
    if not session:
      return None

    # Check token
    token = self.validator(subject_token, self.request.scope, self.request, 'OR')
    # token = session.token

    # To special flow to change group
    if not self.request.scope:
      return token

    scopes = scope_to_list(self.request.scope)
    reqGroups = [s.split(':')[1] for s in scopes if s.startswith('g:')]
    if len(reqGroups) != 1 or not reqGroups[0]:
      return None
    group = reqGroups[0]
    result = Registry.getUsernameForID(token['sub'])
    if not result['OK']:
      return None
    result = gProxyManager.getGroupsStatusByUsername(result['Value'], [group])
    if not result['OK']:
      return None
    if result['Value'][group]['Status'] not in ['ready', 'unknown']:
      return None
    return token

  def authenticate_user(self, credential):
    """ Authorize user

        :param object credential: credential

        :return: str
    """
    return credential.sub

  def revoke_old_credential(self, credential):
    """ Remove old credential

        :param object credential: credential
    """
    self.server.removeSession(credential['access_token'])
