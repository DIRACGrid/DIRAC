""" DIRAC AuthManager Client class encapsulates the methods exposed
    by the AuthManager service.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import six
import json
import requests
from authlib.common.security import generate_token

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities import DIRACSingleton
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
# from DIRAC.ConfigurationSystem.Client.Utilities import getAuthAPI
# from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
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
    self.setServer('Framework/AuthManager')
    self.idps = IdProviderFactory()

  def prepareClientCredentials(self):
    """ Prepare authentication client credentials

        :return: S_OK(dict)/S_ERROR()
    """
    clientMetadata = None
    #TODO: description client.cfg
    #      use client.json instead or write to dirac.cfg in LocalConfiguration
    clientCFG = '/opt/dirac/etc/client.cfg'
    if os.path.isfile(clientCFG) and os.stat(clientCFG).st_size > 0:
      try:
        with open('/opt/dirac/etc/client.cfg', 'rb') as f:
          clientMetadata = json.load(f)
      except IOError as e:
        self.log.error('Cannot read "%s" file with client configuration: %s' % (clientCFG, e))

    if not clientMetadata:
      self.log.info('Register new client')
      try:
        #TODO: Fix hardcore url
        r = requests.post('https://marosvn32.in2p3.fr/DIRAC/auth/register', {'redirect_uri': ''}, verify=False)
        r.raise_for_status()
        clientMetadata = r.json()
      except requests.exceptions.Timeout:
        return S_ERROR('Authentication server is not answer.')
      except requests.exceptions.RequestException as ex:
        return S_ERROR(r.content or ex)
      except Exception as ex:
        return S_ERROR('Cannot read response: %s' % ex)
      self.log.debug('Store %s client to %s' % (clientMetadata['client_id'], clientCFG))
      try:
        with open('/opt/dirac/etc/client.cfg', 'w') as f:
          json.dump(clientMetadata, f)
      except IOError as e:
        self.log.error('Cannot save "%s" file with client configuration: %s' % (clientCFG, e))

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

    #TODO: Fix hardcore url
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
    result = self.idps.getIdProvider(providerName)  #, sessionManager=self.__db)
    return result['Value'].submitNewSession(session) if result['OK'] else result

  def parseAuthResponse(self, providerName, response, session):  #, username, userProfile):
    """ Fill session by user profile, tokens, comment, OIDC authorize status, etc.
        Prepare dict with user parameters, if DN is absent there try to get it.
        Create new or modify existing DIRAC user and store the session

        :param str providerName: identity provider name
        :param dict response: authorization response
        :param object session: session data dictionary

        :return: S_OK(dict)/S_ERROR()
    """
    print('=== CLI parseAuthResponse')
    result = self._getRPC().parseAuthResponse(providerName, response, dict(session))  #, username, userProfile)
    if result['OK']:
      username, profile, sessionDict = result['Value']
      if username and profile:
        gAuthManagerData.updateProfiles(profile['ID'], profile)
    return S_OK((username, profile, Session(sessionDict))) if result['OK'] else result

gSessionManager = AuthManagerClient()
