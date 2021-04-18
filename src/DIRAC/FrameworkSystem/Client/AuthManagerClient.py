""" AuthManagerClient has the function to "talk" to the AuthManager service. Also, when requesting information
    about users, this information is cached in a separate class
    :mod:`AuthManagerData <FrameworkSystem.Client.AuthManagerData>`, and is used, in the Registry for example,
    to reduce the number of requests to the server part
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import six
import json
import requests
from authlib.common.security import generate_token

from diraccfg import CFG

from DIRAC import rootPath, S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities import DIRACSingleton
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.Client.AuthManagerData import gAuthManagerData
from DIRAC.FrameworkSystem.private.authorization.utils.Sessions import Session

__RCSID__ = "$Id$"


@createClient('Framework/AuthManager')
@six.add_metaclass(DIRACSingleton.DIRACSingleton)
class AuthManagerClient(Client):
  """ Authentication manager
  """

  def __init__(self, *args, **kwargs):
    """ Constructor
    """
    super(AuthManagerClient, self).__init__(*args, **kwargs)
    self.localCfg = CFG()
    self.cfgFile = os.path.join(rootPath, 'etc', 'dirac.cfg')
    self.localCfg.loadFromFile(self.cfgFile)
    self.setServer('Framework/AuthManager')
    self.idps = IdProviderFactory()

  def prepareClientCredentials(self):
    """ To interact with the server part through OAuth, you must at least be a registered client,
        prepare authentication client credentials

        :return: S_OK(dict)/S_ERROR()
    """
    clientMetadata = self.localCfg.getAsDict('/LocalInstallation/AuthorizationClient')

    if clientMetadata:
      return S_OK(clientMetadata)

    self.log.info('Register new authorization client..')

    try:
      # TODO: Fix hardcore url
      r = requests.post('https://marosvn32.in2p3.fr/DIRAC/auth/register', {'redirect_uri': ''}, verify=False)
      r.raise_for_status()
      clientMetadata = r.json()
    except requests.exceptions.Timeout:
      return S_ERROR('Authentication server is not answer.')
    except requests.exceptions.RequestException as ex:
      return S_ERROR(r.content or ex)
    except Exception as ex:
      return S_ERROR('Cannot read response: %s' % ex)

    if not clientMetadata:
      return S_ERROR('Cannot get authorization client credentials')

    self.log.debug('Store %s client to local configuration..' % clientMetadata['client_id'])

    data = CFG()
    data.loadFromDict(clientMetadata)
    comment = "Write fresh client credentials to /LocalInstallation section"
    self.localCfg.createNewSection('LocalConfiguration/AuthorizationClient', comment=comment, contents=data)
    self.localCfg.writeToFile(self.cfgFile)

    return S_OK(clientMetadata)

  def submitUserAuthorizationFlow(self, client=None, idP=None, group=None, grant='device'):
    """ Submit authorization flow
    """
    if not client:
      # Prepare client
      result = self.prepareClientCredentials()
      if not result['OK']:
        return result
      client = result['Value']

    # TODO: Fix hardcore url
    url = 'https://marosvn32.in2p3.fr/DIRAC/auth/device?client_id=%s' % client['client_id']
    if group:
      url += '&scope=g:%s' % group
    if idP:
      url += '&provider=%s' % idP
    try:
      r = requests.post(url, verify=False)
      r.raise_for_status()
      return S_OK(r.json())
    except requests.exceptions.Timeout:
      return S_ERROR('Authentication server is not answer.')
    except requests.exceptions.RequestException as ex:
      return S_ERROR(r.content or ex)
    except Exception as ex:
      return S_ERROR('Cannot read response: %s' % ex)

  def getIdPAuthorization(self, providerName, session):
    """ Submit subsession and return dict with authorization url and session number

        :param str providerName: provider name
        :param str mainSession: main session identificator

        :return: S_OK(dict)/S_ERROR() -- dictionary contain next keys:
                 Status -- session status
                 UserName -- user name, returned if status is 'ready'
                 Session -- session id, returned if status is 'needToAuth'
    """
    session = session or generate_token(10)
    result = self.idps.getIdProvider(providerName)  # , sessionManager=self.__db)
    return result['Value'].submitNewSession(session) if result['OK'] else result

  def parseAuthResponse(self, providerName, response, session):  # , username, userProfile):
    """ Fill session by user profile, tokens, comment, OIDC authorize status, etc.
        Prepare dict with user parameters, if DN is absent there try to get it.
        Create new or modify existing DIRAC user and store the session

        :param str providerName: identity provider name
        :param dict response: authorization response
        :param object session: session data dictionary

        :return: S_OK(dict)/S_ERROR()
    """
    result = self._getRPC().parseAuthResponse(providerName, response, dict(session))  # , username, userProfile)
    if result['OK']:
      username, userID, profile = result['Value']
      if username and profile:
        gAuthManagerData.updateProfiles(userID, profile)
    return S_OK((username, userID, profile)) if result['OK'] else result


gSessionManager = AuthManagerClient()
