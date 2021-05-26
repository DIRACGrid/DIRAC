from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import time
import datetime

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory

from authlib.oauth2.rfc6749.util import scope_to_list
from authlib.oauth2.rfc6749.wrappers import OAuth2Token as _OAuth2Token
from authlib.integrations.sqla_oauth2 import OAuth2TokenMixin


def getTokenLocation():
  """ Get the path of the currently active access token file
  """
  envVar = 'DIRAC_TOKEN_FILE'
  if envVar in os.environ:
    tokenPath = os.path.realpath(os.environ[envVar])
    if os.path.isfile(tokenPath):
      return tokenPath
  # /tmp/JWTup_u<uid>
  return "/tmp/JWTup_u%s" % os.getuid()


def readTokenFromFile(fileName=None):
  """ Read token from a file

      :param str fileName: filename to read

      :return: S_OK(dict)/S_ERROR()
  """
  fileName = fileName or getTokenLocation()
  try:
    with open(fileName, 'rt') as f:
      tokenDict = f.read()
    return S_OK(json.loads(tokenDict))
  except Exception as e:
    return S_ERROR('Cannot read token. %s' % repr(e))


def writeToTokenFile(tokenContents, fileName):
  """ Write a token string to file

      :param str tokenContents: token as string
      :param str fileName: filename to dump to

      :return: S_OK(str)/S_ERROR()
  """
  try:
    with open(fileName, 'wt') as fd:
      fd.write(tokenContents)
  except Exception as e:
    return S_ERROR(DErrno.EWF, " %s: %s" % (fileName, repr(e)))
  try:
    os.chmod(fileName, stat.S_IRUSR | stat.S_IWUSR)
  except Exception as e:
    return S_ERROR(DErrno.ESPF, "%s: %s" % (fileName, repr(e)))
  return S_OK(fileName)


def writeTokenDictToTokenFile(tokenDict, fileName=None):
  """ Write a token dict to file

      :param dict tokenDict: dict object to dump to file
      :param str fileName: filename to dump to

      :return: S_OK(str)/S_ERROR()
  """
  fileName = fileName or getTokenLocation()
  try:
    retVal = json.dumps(tokenDict)
  except Exception as e:
    return S_ERROR('Cannot dump token to string. %s' % repr(e))
  return writeToTokenFile(retVal, fileName)


def writeTokenDictToTemporaryFile(tokenDict):
  """ Write a token dict to a temporary file

      :param dict tokenDict: dict object to dump to file

      :return: S_OK(str)/S_ERROR() -- contain file name
  """
  try:
    fd, tokenLocation = tempfile.mkstemp()
    os.close(fd)
  except IOError:
    return S_ERROR(DErrno.ECTMPF)
  retVal = writeTokenDictToTokenFile(tokenDict, tokenLocation)
  if not retVal['OK']:
    try:
      os.unlink(tokenLocation)
    except Exception:
      pass
    return retVal
  return S_OK(tokenLocation)


def getTokenInfo(token=False):
  """ Return token info

      :param token: token location or token as dict

      :return: S_OK(dict)/S_ERROR()
  """
  # Discover token location
  if isinstance(token, dict):
    token = OAuth2Token(token)
  else:
    tokenLocation = token if isinstance(token, six.string_types) else getTokenLocation()
    if not tokenLocation:
      return S_ERROR("Cannot find token location.")
    result = readTokenFromFile(tokenLocation)
    if not result['OK']:
      return result
    token = OAuth2Token(result['Value'])['access_token']

  result = IdProviderFactory().getIdProviderForToken(token)
  if not result['OK']:
    return S_ERROR("Cannot load provider: %s" % result['Message'])
  cli = result['Value']
  cli.updateJWKs()
  payload = cli.verifyToken(token)

  result = Registry.getUsernameForDN('/O=DIRAC/CN=%s' % payload['sub'])
  if not result['OK']:
    return result
  payload['username'] = result['Value']
  if payload.get('group'):
    payload['properties'] = Registry.getPropertiesForGroup(payload['group'])
  return S_OK(payload)


def formatTokenInfoAsString(infoDict):
  """ Convert a token infoDict into a string

      :param dict infoDict: info

      :return: str
  """
  secsLeft = int(infoDict['exp']) - time.time()
  strTimeleft = datetime.datetime.fromtimestamp(secsLeft).strftime("%I:%M:%S")

  leftAlign = 13
  contentList = []
  contentList.append('%s: %s' % ('subject'.ljust(leftAlign), infoDict['sub']))
  contentList.append('%s: %s' % ('issuer'.ljust(leftAlign), infoDict['iss']))
  contentList.append('%s: %s' % ('timeleft'.ljust(leftAlign), strTimeleft))
  contentList.append('%s: %s' % ('username'.ljust(leftAlign), infoDict['username']))
  if infoDict.get('group'):
    contentList.append('%s: %s' % ('DIRAC group'.ljust(leftAlign), infoDict['group']))
  if infoDict.get('properties'):
    contentList.append('%s: %s' % ('properties'.ljust(leftAlign), ', '.join(infoDict['properties'])))
  return "\n".join(contentList)


class OAuth2Token(_OAuth2Token, OAuth2TokenMixin):
  """ Implementation a Token object """

  def __init__(self, params=None, **kwargs):
    kwargs.update(params or {})
    kwargs['revoked'] = False if kwargs.get('revoked', 'False') == 'False' else True
    self.sub = kwargs.get('sub')
    self.issuer = kwargs.get('iss')
    self.client_id = kwargs.get('client_id', kwargs.get('aud'))
    self.token_type = kwargs.get('token_type')
    self.access_token = kwargs.get('access_token')
    self.refresh_token = kwargs.get('refresh_token')
    self.scope = kwargs.get('scope')
    self.revoked = kwargs.get('revoked')
    self.issued_at = int(kwargs.get('issued_at', kwargs.get('iat', time.time())))
    self.expires_in = int(kwargs.get('expires_in', 0))
    self.expires_at = int(kwargs.get('expires_at', kwargs.get('exp', 0)))
    if not self.issued_at:
      raise Exception('Missing "iat" in token.')
    if not self.expires_at:
      if not self.expires_in:
        raise Exception('Cannot calculate token "expires_at".')
      self.expires_at = self.issued_at + self.expires_in
    if not self.expires_in:
      self.expires_in = self.expires_at - self.issued_at
    kwargs.update({'client_id': self.client_id,
                   'token_type': self.token_type,
                   'access_token': self.access_token,
                   'refresh_token': self.refresh_token,
                   'scope': self.scope,
                   'revoked': self.revoked,
                   'issued_at': self.issued_at,
                   'expires_in': self.expires_in,
                   'expires_at': self.expires_at})
    super(OAuth2Token, self).__init__(kwargs)

  @property
  def scopes(self):
    """ Get tokens scopes

        :return: list
    """
    return scope_to_list(self.scope) or []

  @property
  def groups(self):
    """ Get tokens groups

        :return: list
    """
    return [s.split(':')[1] for s in self.scopes if s.startswith('g:')]
