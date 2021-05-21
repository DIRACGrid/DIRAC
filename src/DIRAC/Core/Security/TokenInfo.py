"""
 Set of utilities to retrieve Information from token
"""
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

import six
import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security import Locations

from DIRAC.Core.Security.TokenFile import readTokenFromFile
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory

__RCSID__ = "$Id$"


def getTokenInfo(token=False):
  """ Return token info

      :param token: token location or token as dict

      :return: S_OK(dict)/S_ERROR()
  """
  # Discover token location
  if isinstance(token, dict):
    token = OAuth2Token(token)
  else:
    tokenLocation = token if isinstance(token, six.string_types) else Locations.getTokenLocation()
    if not tokenLocation:
      return S_ERROR("Cannot find token location.")
    result = readTokenFromFile(tokenLocation)
    if not result['OK']:
      return result
    token = OAuth2Token(result['Value'])

  result = IdProviderFactory().getIdProviderForToken(accessToken)
  if not result['OK']:
    return result
  cli = result['Value']
  payload = cli.verifyToken(accessToken)

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
  strTimeleft = datetime.fromtimestamp(secs).strftime("%I:%M:%S")

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
