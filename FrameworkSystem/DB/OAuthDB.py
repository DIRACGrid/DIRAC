""" OAuth class is a front-end to the OAuth Database
"""

__RCSID__ = "$Id$"

import re
import time
import json

from ast import literal_eval
from datetime import datetime

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers import Resources
from DIRAC.FrameworkSystem.Utilities.OAuth2 import OAuth2, getIdPSyntax


gCSAPI = CSAPI()


class OAuthDB(DB):
  """ OAuthDB class is a front-end to the OAuth Database
  """
  tableDict = {'Tokens': {'Fields': {'Id': 'INTEGER AUTO_INCREMENT NOT NULL',
                                     'State': 'VARCHAR(64) NOT NULL',
                                     'Status': 'VARCHAR(32) DEFAULT "prepared"',
                                     'Comment': 'VARCHAR(1000) DEFAULT ""',
                                     'OAuthProvider': 'VARCHAR(255) NOT NULL',
                                     'Token_type': 'VARCHAR(32) DEFAULT "bearer"',
                                     'Access_token': 'VARCHAR(1000)',
                                     'Expires_in': 'DATETIME',
                                     'Refresh_token': 'VARCHAR(1000)',
                                     'Sub': 'VARCHAR(128)',
                                     'UserName': 'VARCHAR(16)',
                                     'UserDN': 'VARCHAR(128)',
                                     'UserSetup': 'VARCHAR(32)',
                                     'Pem': 'BLOB',
                                     'LastAccess': 'DATETIME',
                                     },
                          'PrimaryKey': 'Id',
                          'Engine': 'InnoDB',
                          },
               }

  def __init__(self):
    """ Constructor
    """
    self.__oauth = None
    self.__permValues = ['USER', 'GROUP', 'VO', 'ALL']
    self.__permAttrs = ['ReadAccess', 'PublishAccess']
    DB.__init__(self, 'OAuthDB', 'Framework/OAuthDB')
    retVal = self.__initializeDB()
    if not retVal['OK']:
      raise Exception("Can't create tables: %s" % retVal['Message'])

  def _checkTable(self):
    """ Make sure the tables are created
    """
    return self.__initializeDB()

  def __initializeDB(self):
    """ Create the tables
    """
    retVal = self._query("show tables")
    if not retVal['OK']:
      return retVal

    tablesInDB = [t[0] for t in retVal['Value']]
    tablesD = {}

    if 'Tokens' not in tablesInDB:
      tablesD['Tokens'] = self.tableDict['Tokens']

    return self._createTables(tablesD)

  def cleanZombie(self):
    """ Kill old states """
    result = self._getFromWhere('State', conn='TIMESTAMPDIFF(SECOND,LastAccess,UTC_TIMESTAMP()) > 43200')
    if not result['OK']:
      return result
    states = result['Value']
    if states is not None:
      if len(states) > 1:
        gLogger.debug('Found old states: %s' % str(states))
        for i in range(0, len(states)):
          gLogger.debug('Kill %s state..' % str(states[i][0]))
          result = self.kill_state(states[i][0])
          if not result['OK']:
            return result
    return S_OK()

  def get_auth_request_uri(self, OAuthProvider, state=None):
    """ Register new session """
    url, state = OAuth2(OAuthProvider, state=state).create_auth_request_uri()
    self.insertFields('Tokens', ['State', 'OAuthProvider', 'Comment', 'LastAccess'],
                                [state, OAuthProvider, url, 'UTC_TIMESTAMP()'])
    return S_OK({'url': url, 'state': state})

  def get_link_by_state(self, state):
    """ Get authentification link """
    result = self._getFromWhere('Comment', 'Tokens', State=state,
                                conn='Status = "prepared" and TIMESTAMPDIFF(SECOND,LastAccess,UTC_TIMESTAMP()) < 300')
    if not result['OK']:
      return result
    if result['Value'] is None:
      return S_ERROR('No link found.')
    return S_OK(result['Value'][0][0])

  def get_proxy_dn_exptime(self, proxyProvider, DN=None, proxylivetime=None, voms=None,
                           access_token=None, username=None, ID=None, state=None):
    _conn = ''
    _params = {'OAuthProvider': proxyProvider}
    result = Resources.getProxyProviders()
    if not result['OK']:
      return result
    if proxyProvider not in result['Value']:
      return S_ERROR('%s is not proxy provider.' % proxyProvider)
    if not access_token:
      if state:
        _params['State'] = state
      else:
        _conn += 'Status = "authed"'
        if DN:
          _params['UserDN'] = DN
        elif username:
          _params['UserName'] = username
        else:
          return S_ERROR('DN or username need to set.')
        if ID:
          _params['Sub'] = ID
      result = self._getFromWhere(field='Access_token', conn=_conn, **_params)
      if not result['OK']:
        return result
      access_tokens = result['Value']
      gLogger.info(result)
      if access_tokens is None:
        return S_ERROR('No access_token found.')
      for i in range(0, len(access_tokens)):
        result = self.fetch_token(access_tokens[i][0])
        if result['OK']:
          access_token = result['Value']['Access_token']
          break
        else:
          self.kill_state(None, conn='Access_token = "%s"' % access_tokens[i][0])
      if not result['OK']:
        return result
      access_token = result['Value']['Access_token']
    result = OAuth2(proxyProvider).get_proxy(access_token, proxylivetime, voms)
    if not result['OK']:
      return result
    proxy = result['Value']
    chain = X509Chain()
    result = chain.loadProxyFromString(proxy)
    if not result['OK']:
      return result
    result = chain.getCredentials()
    if not result['OK']:
      return result
    DN = result['Value']['identity']
    self.updateFields('Tokens', ['Expires_in', 'UserDN', 'LastAccess'],
                                ['UTC_TIMESTAMP()', DN, 'UTC_TIMESTAMP()'],
                      {'Access_token': access_token})
    result = chain.getRemainingSecs()
    if not result['OK']:
      return result
    exptime = result['Value']
    return S_OK({'proxy': proxy, 'DN': DN, 'exptime': exptime})

  def parse_auth_response(self, code, state):
    """ Fill session of user profile """
    def _CRASH(m='', st='failed'):
      for _s in [state, state.replace('_proxy', '')]:
        self.updateFields('Tokens', ['Status', 'Comment', 'LastAccess'],
                                    [st, m, 'UTC_TIMESTAMP()'], {'State': _s})
    comment = ''
    status = 'prepared'
    exp_datetime = 'UTC_TIMESTAMP()'
    result = self._getFromWhere('OAuthProvider', 'Tokens', State=state)
    if not result['OK']:
      _CRASH(result['Message'])
      return result
    if result['Value'] is None:
      _CRASH('No any provider found.')
      return S_ERROR('No any provider found.')
    OAuthProvider = result['Value'][0][0]
    self.__oauth = OAuth2(OAuthProvider)
    # Parsing response
    gLogger.notice('%s: Parsing authentification response.' % state)
    result = self.__oauth.parse_auth_response(code)
    if not result['OK']:
      _CRASH(result['Message'])
      return result
    oauthDict = result['Value']
    oauthDict['redirect'] = ''
    oauthDict['messages'] = []
    oauthDict['username'] = False
    csModDict = {'UsrOptns': {}, 'Groups': []}
    if 'expires_in' in oauthDict['Tokens']:
      result = self._datetimePlusSeconds(oauthDict['Tokens']['expires_in'])
      if not result['OK']:
        _CRASH(result['Message'])
        return result
      exp_datetime = result['Value']
    if 'refresh_token' not in oauthDict['Tokens']:
      _CRASH('No refresh token')
      return S_ERROR('No refresh token')

    if OAuthProvider in Resources.getProxyProviders()['Value']:
      # For proxy provider
      gLogger.notice('%s: Proxy provider: %s' % (state, OAuthProvider))
      result = self._getFromWhere('Comment', 'Tokens', State=state.replace('_proxy', ''))
      if not result['OK']:
        _CRASH(result['Message'])
        return result
      if result['Value'] is None:
        _CRASH('Cannot get IdP info dict from "Comment" field: it`s empty')
        return S_ERROR('Cannot get IdP info dict from "Comment" field: it`s empty')
      try:
        csModDict = literal_eval(result['Value'][0][0])
      except Exception as ex:
        _CRASH('Cannot get IdP info dict from "Comment" field: %s' % ex)
        return S_ERROR('Cannot get IdP info dict from "Comment" field: %s' % ex)
      if type(csModDict) is not dict:
        _CRASH('Cannot get IdP info dict from "Comment" field: it`s not dict')
        return S_ERROR('Cannot get IdP info dict from "Comment" field: it`s not dict')
      status = 'authed'
      self.updateFields('Tokens', ['Status', 'Token_type', 'Access_token', 'Expires_in',
                                   'Refresh_token', 'Sub', 'UserName', 'LastAccess'],
                                  [status, oauthDict['Tokens']['token_type'], oauthDict['Tokens']['access_token'],
                                   exp_datetime, oauthDict['Tokens']['refresh_token'], oauthDict['UserProfile']['sub'],
                                   csModDict['username'], 'UTC_TIMESTAMP()'],
                        {'State': state})
      result = self.get_proxy_dn_exptime(OAuthProvider, state=state)
      if not result['OK']:
        _CRASH(result['Message'])
        return result
      proxyDN = result['Value']['DN']
      if proxyDN not in csModDict['UsrOptns']['DN']:
        csModDict['UsrOptns']['DN'].append(proxyDN)
      csModDict['UsrOptns']['DN'] = ','.join(csModDict['UsrOptns']['DN'])
      if 'Groups' not in csModDict['UsrOptns']:
        _CRASH('Cannot found any groups in IdP record field')
        return S_ERROR('Cannot found any groups in IdP record field')
      secDN = proxyDN.replace('/', '-').replace('=', '_')
      csModDict['UsrOptns']['DNProperties/%s/Groups' % secDN] = ','.join(csModDict['UsrOptns']['Groups'])
      csModDict['UsrOptns']['DNProperties/%s/ProxyProviders' % secDN] = OAuthProvider

    elif OAuthProvider in Resources.getIdPs()['Value']:
      # For IdP
      gLogger.notice('%s: Identity provider: %s' % (state, OAuthProvider))
      result = self.prepare_usr_parameters(OAuthProvider, **oauthDict['UserProfile'])
      if not result['OK']:
        _CRASH(result['Message'])
        return result
      csModDict = result['Value']
      oauthDict['username'] = csModDict['username']
      if not csModDict['UsrOptns']['Groups'] and not csModDict['Groups'] and not csModDict['UsrExist']:
        comment = 'We not found any registred DIRAC groups that mached with your profile. '
        comment += 'So, your profile has the same access that Visitor DIRAC user.'
        _CRASH(comment, st='visitor')
        return S_OK({'redirect': '', 'Messages': comment})
      else:
        # Set redirect if IdP have oidc proxy provider
        pP = Resources.getIdPOption(OAuthProvider, 'proxy_provider')
        if pP:
          method = Resources.getProxyProviderOption(pP, 'method')
          if method == 'oAuth2':
            result = self.get_proxy_dn_exptime(pP, username=csModDict['username'], ID=csModDict['UsrOptns']['ID'])
            gLogger.info('Trying to find proxy prov tokens')
            gLogger.info(result)
            if result['OK']:
              proxyDN = result['Value']['DN']
              if proxyDN not in csModDict['UsrOptns']['DN']:
                csModDict['UsrOptns']['DN'].append(proxyDN)
              csModDict['UsrOptns']['DN'] = ','.join(csModDict['UsrOptns']['DN'])
              if 'Groups' not in csModDict['UsrOptns']:
                _CRASH('Cannot found any groups in IdP record field')
                return S_ERROR('Cannot found any groups in IdP record field')
              secDN = proxyDN.replace('/', '-').replace('=', '_')
              csModDict['UsrOptns']['DNProperties/%s/Groups' % secDN] = ','.join(csModDict['UsrOptns']['Groups'])
              csModDict['UsrOptns']['DNProperties/%s/ProxyProviders' % secDN] = pP
              self.updateFields('Tokens', ['UserDN', 'LastAccess'],
                                          [proxyDN, 'UTC_TIMESTAMP()'],
                                {'State': state})
            else:
              result = self.get_auth_request_uri(pP, state + '_proxy')
              if not result['OK']:
                _CRASH(result['Message'])
                return result
              oauthDict['redirect'] = result['Value']['url']
              comment = json.dumps(csModDict)
          elif not method:
            # FIXME: getProxyDNExptime(csModDict['username'],csModDict['UsrOptns']['EMail'])
            # csModDict['UsrOptns']['DN'] = result['Value']['DN']
            pass
          elif not csModDict['UsrOptns']['DN']:
            _CRASH('No DN returned from %s OAuth provider' % OAuthProvider)
            return S_ERROR('No DN returned from %s OAuth provider' % OAuthProvider)
        elif not csModDict['UsrOptns']['DN']:
          _CRASH('No DN returned from %s OAuth provider' % OAuthProvider)
          return S_ERROR('No DN returned from %s OAuth provider' % OAuthProvider)
      self.updateFields('Tokens', ['Status', 'Comment', 'Token_type', 'Access_token', 'Expires_in',
                                   'Refresh_token', 'Sub', 'UserName', 'LastAccess'],
                                  [status, comment, oauthDict['Tokens']['token_type'],
                                   oauthDict['Tokens']['access_token'], exp_datetime,
                                   oauthDict['Tokens']['refresh_token'], oauthDict['UserProfile']['sub'],
                                   oauthDict['username'], 'UTC_TIMESTAMP()'],
                        {'State': state})
    else:
      _CRASH('No configuration found for %s provider' % OAuthProvider)
      return S_ERROR('No configuration found for %s provider' % OAuthProvider)

    if not oauthDict['redirect']:
      # Add new user or modify old
      gLogger.debug("%s: Prepring parameters for registeration new DIRAC user:\n %s" % (state, csModDict))
      if 'noregvos' in csModDict:
        msg = '%s unsupported by DIRAC. ' % str(csModDict['noregvos'])
        msg += 'Please contact with administrators of this VOs to register it in DIRAC.'
        oauthDict['messages'].append(msg)
      for group in csModDict['Groups']:
        result = gCSAPI.addGroup(group, csModDict['Groups'][group])
        if not result['OK']:
          _CRASH(result['Message'])
          return result
      result = gCSAPI.modifyUser(csModDict['username'], csModDict['UsrOptns'], True)
      if not result['OK']:
        _CRASH(result['Message'])
        return result
      result = gCSAPI.commitChanges()
      if not result['OK']:
        _CRASH(result['Message'])
        return result
      _CRASH(st='authed')
    return S_OK({'redirect': oauthDict['redirect'], 'Messages': oauthDict['messages']})

  def get_by_state(self, state, value=['OAuthProvider', 'Sub', 'State', 'Status', 'Comment', 'Token_type',
                                       'Access_token', 'Expires_in', 'Refresh_token', 'UserName', 'LastAccess']):
    """ Get filds from session """
    self.updateFields('Tokens', ['LastAccess'], ['UTC_TIMESTAMP()'], {'State': state})
    if not type(value) == list:
      value = list(value)
    return self._getListFromWhere(value, 'Tokens', State=state)

  def kill_state(self, state, conn=''):
    """ Remove session """
    if state:
      conn += ' AND State = "%s"' % state
    result = self._getListFromWhere(['OAuthProvider', 'Access_token', 'Refresh_token'], 'Tokens', conn=conn)
    if not result['OK']:
      return result
    tDict = result['Value']
    OAuth2(tDict['OAuthProvider']).revoke_token(tDict['Access_token'], tDict['Refresh_token'])
    return self.deleteEntries('Tokens', condDict={'State': state})

  def fetch_token(self, token=None, state=None):
    """ Refresh tokens """
    if state:
      params = {'State': state}
    else:
      params = {'Access_token': token}
    gLogger.notice("Trying to fetch tokens by %s" % params)
    result = self._getListFromWhere(['Access_token', 'Expires_in', 'Refresh_token', 'OAuthProvider'],
                                    'Tokens', conn='Status = "authed"', **params)
    if not result['OK']:
      return result
    resD = result['Value']
    if not resD['OAuthProvider']:
      return S_ERROR('No OAuthProvider found.')
    left = 0
    if resD['Expires_in']:
      result = self._leftSeconds(resD['Expires_in'])
      if not result['OK']:
        return result
      left = result['Value']
    gLogger.notice('Left seconds of access token: %s' % str(left))
    tD = {}
    if left < 1800:
      # refresh token
      gLogger.notice('Fetching...')
      result = OAuth2(resD['OAuthProvider']).fetch_token(refresh_token=resD['Refresh_token'])
      if not result['OK']:
        return result
      tD = result['Value']
      exp_datetime = 'UTC_TIMESTAMP()'
      if 'expires_in' in tD:
        result = self._datetimePlusSeconds(tD['expires_in'])
        if not result['OK']:
          return result
        exp_datetime = result['Value']
      refresh_token = None
      if 'refresh_token' in tD:
        refresh_token = tD['refresh_token']
      self.updateFields('Tokens', ['Token_type', 'Access_token', 'Expires_in',
                                   'Refresh_token', 'LastAccess'],
                                  [tD['token_type'], tD['access_token'], exp_datetime,
                                   refresh_token, 'UTC_TIMESTAMP()'],
                        {'Access_token': token})
      for k in tD.keys():
        resD[k.capitalize()] = tD[k]
    return S_OK(resD)

  def prepare_usr_parameters(self, idp, **kwargs):
    """ Convert usrProfile to parameters needed for AddUser metod:
          username, DN as list, Groups, ID, email, etc.
    """
    prepDict = {'UsrOptns': {}, 'Groups': []}
    prepDict['noregvos'] = []
    prepDict['UsrExist'] = ''
    prepDict['UsrOptns']['DN'] = []
    prepDict['UsrOptns']['Groups'] = []
    for param in ['sub', 'email', 'name']:
      if param not in kwargs:
        return S_ERROR('No found %s parameter on dict.' % param)
    # Set ID, EMail
    prepDict['UsrOptns']['ID'] = kwargs['sub']
    prepDict['UsrOptns']['Email'] = kwargs['email']
    result = gCSAPI.listUsers()
    if not result['OK']:
      return result
    allusrs = result['Value']
    # Look username
    result = Registry.getUsernameForID(kwargs['sub'])
    if result['OK']:
      prepDict['UsrExist'] = 'Yes'
      pre_usrname = result['Value']
      result = Registry.getDNForUsername(pre_usrname)
      if not result['OK']:
        return result
      prepDict['UsrOptns']['DN'].extend(result['Value'])
    else:
      # Gernerate new username
      if 'preferred_username' in kwargs:
        pre_usrname = kwargs['preferred_username'].lower()
      else:
        if 'family_name' in kwargs and 'given_name' in kwargs:
          pre_usrname = '%s %s' % (kwargs['given_name'], kwargs['family_name'])
        else:
          pre_usrname = kwargs['name']
        pre_usrname = pre_usrname.lower().split(' ')[0][0] + pre_usrname.lower().split(' ')[1]
        pre_usrname = pre_usrname[:6]
      for i in range(0, 100):
        if pre_usrname not in allusrs:
          break
        pre_usrname = pre_usrname + str(i)
    # Set username
    prepDict['username'] = pre_usrname
    # Parse VO/Role from IdP
    result = getIdPSyntax(idp, 'VOMS')
    if not result['OK']:
      return result
    synDict = result['Value']
    if synDict['claim'] not in kwargs:
      return S_ERROR('No found needed claim: %s.' % synDict['claim'])
    voFromClaimList = kwargs[synDict['claim']]
    if not isinstance(voFromClaimList, (list,)):
      voFromClaimList = voFromClaimList.split(',')
    for item in voFromClaimList:
      r = synDict['vo'].split('<VALUE>')
      if not re.search(r[0], item):
        continue
      # Parse VO
      vo = re.sub(r[1], '', re.sub(r[0], '', item))
      allvos = Registry.getVOs()
      if not allvos['OK']:
        return allvos
      if vo not in allvos['Value']:
        prepDict['noregvos'].append(vo)
        continue
      r = synDict['role'].split('<VALUE>')
      # Parse Role
      role = re.sub(r[1], '', re.sub(r[0], '', item))
      result = Registry.getVOMSRoleGroupMapping(vo)
      if not result['OK']:
        return result
      roleGroup = result['Value']['VOMSDIRAC']
      groupRole = result['Value']['DIRACVOMS']
      noVoms = result['Value']['NoVOMS']
      for group in noVoms:
        # Set groups with no role
        prepDict['UsrOptns']['Groups'].append(group)
      if role not in roleGroup:
        # Create new group
        group = vo + '_' + role
        properties = {'VOMSRole': role, 'VOMSVO': vo, 'VO': vo, 'Properties': 'NormalUser', 'Users': pre_usrname}
        prepDict['Groups'].append({group: properties})
      else:
        for group in groupRole:
          if role == groupRole[group]:
            # Set groups with role
            prepDict['UsrOptns']['Groups'].append(group)
    # Set DN
    if 'DN' in kwargs:
      prepDict['UsrOptns']['DN'].append(kwargs['DN'])
    return S_OK(prepDict)

  def _getFromWhere(self, field='*', table='Tokens', conn='', **kwargs):
    if conn:
      conn += ' and '
    for key in kwargs:
      conn += '%s = "%s" and ' % (key, str(kwargs[key]))
    result = self._query('SELECT %s FROM %s WHERE %s True' % (field, table, conn))
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      result['Value'] = None
    else:
      result['Value'] = list(result['Value'])
    return result

  def _getListFromWhere(self, fields=[], table='Tokens', conn='', **kwargs):
    resD = {}
    for i in fields:
      result = self._getFromWhere(i, table, **kwargs)
      if not result['OK']:
        return result
      if result['Value'] is not None:
        resD[i] = result['Value'][0][0]
      else:
        resD[i] = None
    return S_OK(resD)

  def _datetimePlusSeconds(self, seconds):
    result = self._query('SELECT ADDDATE(UTC_TIMESTAMP(), INTERVAL %s SECOND)' % seconds)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      return S_OK(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return S_OK(result['Value'][0][0].strftime('%Y-%m-%d %H:%M:%S'))

  def _leftSeconds(self, date):
    result = self._query('SELECT TIMESTAMPDIFF(SECOND,UTC_TIMESTAMP(),"%s");' % date)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      return S_OK(0)
    return S_OK(result['Value'][0][0])
