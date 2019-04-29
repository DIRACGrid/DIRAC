'''OAuth2

  OAuth2 included all methods to work with OID providers.
'''
import random
import string
import requests

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers import Resources
from DIRAC.ConfigurationSystem.Client.Utilities import getOAuthAPI


def getIdPWellKnownDict(idp=None, issuer=None, well_known_url=None):
  """ Get the dict of all the settings IdP defined in the IdPs site """
  if idp:
    result = Resources.getIdPOption(idp, 'well_known')
    if not result['OK']:
      result = Resources.getIdPOption(idp, 'issuer')
      if result['OK']:
        result = ''.join([result, '/.well-known/openid-configuration'])
    url = result
  elif issuer:
    url = ''.join([issuer, '/.well-known/openid-configuration'])
  elif well_known_url:
    url = well_known_url
  # FIXME: in production need to remove 'verify' parametr
  r = requests.get(url, verify=False)
  if not r.status_code == 200:
    return S_ERROR(r.status_code)
  if not r.json():
    return S_ERROR('No expected response.')
  return S_OK(r.json())


def getIdPSyntax(idp, option):
  """ Get claim and regexs from CS to parse value """
  resDict = {}
  result = Resources.getIdPSections(idp)
  if not result['OK']:
    return result
  result = Resources.getIdPSections(idp, '/Syntax')
  if not result['OK']:
    return result
  opts = result['Value']
  if option not in opts:
    return S_ERROR('Option %s not evalible in CFG' % option)
  result = Resources.getIdPOptions(idp, '/Syntax/VOMS')
  if not result['OK']:
    return result
  keys = result['Value']
  if 'claim' not in keys:
    return S_ERROR('No claim found for %s in CFG.' % option)
  resDict['claim'] = Resources.getIdPOption(idp, '/Syntax/%s/claim' % option)
  for key in keys:
    resDict[key] = Resources.getIdPOption(idp, '/Syntax/%s/%s' % (option, key))
  return S_OK(resDict)


class OIDCClient(requests.Session):

  def __init__(self, idp=None, client_id=None, client_secret=None, redirect_uri=None,
               scope=None, issuer=None, authorization_endpoint=None, token_endpoint=None,
               introspection_endpoint=None, proxy_endpoint=None, max_proxylifetime=None,
               response_types_supported=None, grant_types_supported=None, revocation_endpoint=None,
               userinfo_endpoint=None, jwks_uri=None, registration_endpoint=None, **kwargs):
    """ OIDCClient constructor """

    optns = {}
    if type(idp) is dict:
      for key, value in idp.iteritems():
        optns[key] = value
      idp = None
    elif Resources.getIdPDict(idp)['OK']:
      optns = Resources.getIdPDict(idp)['Value']
    elif Resources.getProxyProviderDict(idp)['OK']:
      optns = Resources.getProxyProviderDict(idp)['Value']
    if kwargs is not None:
      for key, value in kwargs.iteritems():
        optns[key] = value

    self.issuer = issuer or 'issuer' in optns and optns['issuer']
    if self.issuer:
      remoteIdPDict = getIdPWellKnownDict(well_known_url=self.issuer + '/.well-known/openid-configuration')
      if remoteIdPDict['OK'] and type(remoteIdPDict['Value']) is dict:
        for key, value in remoteIdPDict['Value'].iteritems():
          optns[key] = value

    self.name = idp or 'idp' in optns and optns['idp']
    self.client_id = client_id or 'client_id' in optns and optns['client_id']
    if not self.client_id:
      raise Exception('client_id parameter is absent.')
    self.scope = scope or 'scope' in optns and optns['scope'].split(',')
    if 'scopes_supported' in optns:
      for s in optns['scopes_supported']:
        if not self.scope:
          self.scope = []
        self.scope.append(s)
    self.redirect_uri = redirect_uri or 'redirect_uri' in optns and optns['redirect_uri']
    self.client_secret = client_secret or 'client_secret' in optns and optns['client_secret']
    self.token_endpoint = token_endpoint or 'token_endpoint' in optns and optns['token_endpoint']
    self.proxy_endpoint = proxy_endpoint or 'proxy_endpoint' in optns and optns['proxy_endpoint']
    self.userinfo_endpoint = userinfo_endpoint or 'userinfo_endpoint' in optns and optns['userinfo_endpoint']
    self.max_proxylifetime = max_proxylifetime or 'max_proxylifetime' in optns and optns['max_proxylifetime'] or 86400
    self.revocation_endpoint = revocation_endpoint or 'revocation_endpoint' in optns and optns['revocation_endpoint']
    self.registration_endpoint = registration_endpoint or \
        'registration_endpoint' in optns and optns['registration_endpoint']
    self.authorization_endpoint = authorization_endpoint or \
        'authorization_endpoint' in optns and optns['authorization_endpoint']
    self.introspection_endpoint = introspection_endpoint or \
        'introspection_endpoint' in optns and optns['introspection_endpoint']


class OAuth2(OIDCClient):

  def __init__(self, idp=None, state=None, client_id=None, client_secret=None, redirect_uri=None,
               scope=None, issuer=None, authorization_endpoint=None, token_endpoint=None, introspection_endpoint=None,
               proxy_endpoint=None, max_proxylifetime=None, response_types_supported=None, grant_types_supported=None,
               revocation_endpoint=None, userinfo_endpoint=None, jwks_uri=None, registration_endpoint=None, **kwargs):
    """ OAuth2 constructor """

    super(OAuth2, self).__init__(idp, client_id, client_secret, redirect_uri, scope, issuer, proxy_endpoint,
                                 max_proxylifetime, authorization_endpoint, token_endpoint, introspection_endpoint,
                                 response_types_supported, grant_types_supported, revocation_endpoint,
                                 userinfo_endpoint, jwks_uri, registration_endpoint, **kwargs)
    self.state = state or self.new_state()
    self.idp = idp
    oauthAPI = getOAuthAPI()
    if oauthAPI:
      self.redirect_uri = '%s/redirect' % oauthAPI

  def create_auth_request_uri(self, uri=None, **kwargs):
    """ Create link for authorization """
    uri = uri or self.authorization_endpoint
    url = '%s?state=%s&response_type=code&client_id=%s' % (uri, self.state, self.client_id)
    url += '&access_type=offline&prompt=consent'
    if 'redirect_uri' not in kwargs:
      kwargs['redirect_uri'] = self.redirect_uri
    if 'scope' not in kwargs:
      kwargs['scope'] = ''
    for s in self.scope:
      kwargs['scope'] += '+%s' % s
    for key in kwargs:
      url += '&%s=%s' % (key, kwargs[key])
    return url, self.state

  def parse_auth_response(self, code):
    """ Collecting information about user """
    oaDict = {}
    # Get tokens
    result = self.fetch_token(code)
    if not result['OK']:
      return result
    oaDict['Tokens'] = result['Value']
    # Get user profile
    result = self.get_usr_profile(oaDict['Tokens']['access_token'])
    if not result['OK']:
      return result
    oaDict['UserProfile'] = result['Value']
    oaDict['idp'] = self.idp
    return S_OK(oaDict)

  def get_proxy(self, access_token, proxylifetime=None, voms=None, **kwargs):
    """ Get user proxy from IdP """
    proxylifetime = proxylifetime or self.max_proxylifetime
    proxy_endpoint = self.proxy_endpoint
    if not proxy_endpoint:
      return S_ERROR('No get proxy endpoind found for %s IdP.' % self.idp)
    client_id = self.client_id
    client_secret = self.client_secret
    params = '?client_id=%s&client_secret=%s' % (client_id, client_secret)
    params += '&access_token=%s&proxylifetime=%s' % (access_token, proxylifetime)
    params += '&access_type=offline&prompt=consent'
    if voms:
      result = Registry.getVOs()
      if not result['OK']:
        return result
      if voms not in result['Value']:
        return S_ERROR('%s vo is not registred in DIRAC.' % voms)
      result = Registry.getVOMSServerInfo(voms)
      if not result['OK']:
        return result
      gLogger.info(result['Value'])
      vomsname = result['Value'][voms]['VOMSName']
      hostname = result['Value'][voms]['Servers'][0]
      hostDN = result['Value'][voms]['Servers'][hostname]['DN']
      port = result['Value'][voms]['Servers'][hostname]['Port']
      vomses = '"%s" "%s" "%s" "%s" "%s"' % (vomsname, hostname, port, hostDN, vomsname)
      params = '%s&voname=%s&vomses=%s' % (params, vomsname, vomses)
    for key in kwargs:
      if kwargs[key]:
        params += '&%s=%s' % (key, kwargs[key])
    url = proxy_endpoint + params
    gLogger.notice('Url for get proxy: %s' % url)
    r = requests.get(url, verify=False)
    if not r.status_code == 200:
      return S_ERROR(r.status_code)
    return S_OK(r.text)

  def get_usr_profile(self, access_token, userinfo_endpoint=None):
    """ Get user profile """
    userinfo_endpoint = userinfo_endpoint or self.userinfo_endpoint
    headers = {'Authorization': 'Bearer ' + access_token}
    r = requests.get(userinfo_endpoint, headers=headers, verify=False)
    if not r.status_code == 200:
      return S_ERROR(r.status_code)
    if not r.json():
      return S_ERROR('No expected response.')
    return S_OK(r.json())

  def revoke_token(self, access_token=None, refresh_token=None):
    """ Revoke token """
    tDict = {'access_token': access_token, 'refresh_token': refresh_token}
    if not self.revocation_endpoint:
      return S_ERROR('Not found revocation endpoint.')
    for key in tDict:
      r = requests.post("%s?token=%s&token_type_hint=%s" %
                        (self.revocation_endpoint, tDict[key], key), verify=False)
    return S_OK()

  def fetch_token(self, code=None, refresh_token=None, token_endpoint=None,
                  client_secret=None, client_id=None,
                  redirect_uri=None):
    """ Make token request """
    token_endpoint = token_endpoint or self.token_endpoint
    client_secret = client_secret or self.client_secret
    redirect_uri = redirect_uri or self.redirect_uri
    client_id = client_id or self.client_id
    # FIXME: in production need to remove 'verify' parametr
    uri = "%s?access_type=offline&prompt=consent" % token_endpoint
    uri += "&client_id=%s&client_secret=%s" % (client_id, client_secret)
    if code:
      uri += "&grant_type=authorization_code&code=%s&redirect_uri=%s" % (code, redirect_uri)
    else:
      uri += "&grant_type=refresh_token&refresh_token=%s" % refresh_token
    r = requests.post(uri, verify=False)
    if not r.status_code == 200:
      return S_ERROR(r.status_code)
    if 'access_token' not in r.json():
      return S_ERROR('No expected response.')
    return S_OK(r.json())

  def new_state(self):
    """ Generates a state string to be used in authorizations. """
    return ''.join(random.choice(string.ascii_letters + string.digits) for x in range(30))
