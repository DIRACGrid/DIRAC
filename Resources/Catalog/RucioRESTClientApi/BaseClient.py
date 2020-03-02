# -*- coding: utf-8 -*-
# Copyright 2012-2020 CERN
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
# - Thomas Beermann <thomas.beermann@cern.ch>, 2012-2020
# - Vincent Garonne <vincent.garonne@cern.ch>, 2012-2018
# - Yun-Pin Sun <winter0128@gmail.com>, 2013
# - Mario Lassnig <mario.lassnig@cern.ch>, 2013-2020
# - Cedric Serfon <cedric.serfon@cern.ch>, 2014-2020
# - Ralph Vigne <ralph.vigne@cern.ch>, 2015
# - Joaquín Bogado <jbogado@linti.unlp.edu.ar>, 2015-2018
# - Martin Barisits <martin.barisits@cern.ch>, 2016-2020
# - Tobias Wegner <twegner@cern.ch>, 2017
# - Brian Bockelman <bbockelm@cse.unl.edu>, 2017-2018
# - Robert Illingworth <illingwo@fnal.gov>, 2018
# - Hannes Hansen <hannes.jakob.hansen@cern.ch>, 2018
# - Tomas Javurek <tomas.javurek@cern.ch>, 2019-2020
# - Brandon White <bjwhite@fnal.gov>, 2019
# - Ruturaj Gujar <ruturaj.gujar23@gmail.com>, 2019
# - Eric Vaandering <ewv@fnal.gov>, 2019
# - Jaroslav Guenther <jaroslav.guenther@cern.ch>, 2019-2020
# - Andrew Lister <andrew.lister@stfc.ac.uk>, 2019
# - Eli Chadwick <eli.chadwick@stfc.ac.uk>, 2020
# - Patrick Austin <patrick.austin@stfc.ac.uk>, 2020
# - Benedikt Ziemons <benedikt.ziemons@cern.ch>, 2020

'''
 Client class for callers of the Rucio system. Based on:

 https://github.com/rucio/rucio/blob/master/lib/rucio/client/baseclient.py

  Modified to be used by Dirac. Modifications include:
   - keep only x509 authentication,
   - eliminate any references to Rucio config file. All values needed to configure the client are
     obtained from Dirac CS,
    - avoid throwing exceptions. They are converted to Dirac S_ERROR or S_OK objects.

'''


from __future__ import print_function

import os
from os import environ, fdopen, makedirs, geteuid
from shutil import move
from tempfile import mkstemp
from urlparse import urlparse

from requests import Session
from requests.status_codes import codes, _codes
from requests.exceptions import ConnectionError, RequestException

from DIRAC.Core.Security import ProxyInfo
from DIRAC.Core.Security import Locations
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Catalog.RucioRESTClientApi import Utils


class BaseClient(object):

  AUTH_RETRIES, REQUEST_RETRIES = 2, 3
  TOKEN_PATH_PREFIX = Utils.getTempDir() + '/.rucio_'
  TOKEN_PREFIX = 'auth_token_'
  TOKEN_EXP_PREFIX = 'auth_token_exp_'

  def __init__(self, rucioHost=None, authHost=None, account=None, userAgent='rucio-clients',
               timeout=600):
    self.rucioHost = rucioHost
    self.authHost = authHost
    self.session = Session()
    self.account = account
    self.headers = {}
    self.timeout = timeout
    self.request_retries = self.REQUEST_RETRIES
    self.tokenExpEpoch = None
    self.tokenExpEpochFile = None
    self.log = gLogger.getSubLogger("FileCatalog")
    self.creds = {}
    self.userAgent = userAgent
    self.scriptID = 'python'
    self.tokenPath = self.TOKEN_PATH_PREFIX + self.account
    self.vo = None

  def authenticate(self):
    """
    Performs X509 authentication. Gets a Dirac proxy and maintains a token.

    :return: S_OK or S_ERROR Dirac object.
    :rtype: dict
    """

    # Get Dirac Proxy info:
    proxyInfo = ProxyInfo.getProxyInfo()
    if proxyInfo['OK']:
      value = proxyInfo['Value']
      path = value['path']
      self.creds['client_proxy'] = path
      timeleft = value['secondsLeft']
      if timeleft <= 0.0:
        self.log.error("Proxy expired")
        result = S_ERROR('Proxy expired')
      self.vo = ProxyInfo.getVOfromProxyGroup().get('Value', None)
      self.tokenPath += '@%s' % self.vo
      self.tokenFile = self.tokenPath + '/' + self.TOKEN_PREFIX + self.account
    else:
      result = S_ERROR(proxyInfo.get('Message', 'Cannot find a proxy file (reson unavailable'))

    # scheme logic
    rucio_scheme = urlparse(self.rucioHost).scheme
    auth_scheme = urlparse(self.authHost).scheme

    if rucio_scheme != 'http' and rucio_scheme != 'https':
      result = S_ERROR('rucio scheme \'%s\' not supported' % rucio_scheme)

    if auth_scheme != 'http' and auth_scheme != 'https':
      result = S_ERROR('auth scheme \'%s\' not supported' % auth_scheme)

    # CA cert directory
    self.caCertPath = Locations.getCAsLocation()

    # account ?
    if self.account is None:
      self.log.info('No account passed. Trying to get it from the environment')
      try:
        self.account = environ['RUCIO_ACCOUNT']
      except KeyError:
        self.log.error("No account can be determined. Set the env varriable ?")
        result = S_ERROR("No account can be determined. Set the env varriable ?")

    # Authenticate
    result = self.__authenticate()
    return result

  def __authenticate(self):
    """
    Main method for authentication. It first tries to read a locally saved token.
    If not available it requests a new one.
    """

    result = self.__read_token()
    if result['OK']:
      if not result['Value']:
        result = self.__get_token()
    return result

  def _sendRequest(self, url, headers=None, type='GET', data=None, params=None, stream=False):
    """
    Helper method to send requests to the rucio server.
    Gets a new token and retries if an unauthorized error is returned.

    :param url: the http url to use.
    :param headers: additional http headers to send.
    :param type: the http request type to use.
    :param data: post data.
    :param params: (optional) Dictionary or bytes to be sent in the url query string.
    :return: the HTTP return body.
    """
    hds = {'X-Rucio-Auth-Token': self.auth_token, 'X-Rucio-Account': self.account, 'X-Rucio-VO': self.vo,
           'Connection': 'Keep-Alive', 'User-Agent': self.userAgent,
           'X-Rucio-Script': self.scriptID}

    if headers is not None:
      hds.update(headers)

    result = None
    #
    for retry in range(self.AUTH_RETRIES + 1):
      try:
        if type == 'GET':  # was stream=True
          result = self.session.get(
              url,
              headers=hds,
              verify=self.caCertPath,
              timeout=self.timeout,
              params=params,
              stream=False)
        elif type == 'PUT':
          result = self.session.put(url, headers=hds, data=data, verify=self.caCertPath, timeout=self.timeout)
        elif type == 'POST':
          result = self.session.post(
              url,
              headers=hds,
              data=data,
              verify=self.caCertPath,
              timeout=self.timeout,
              stream=stream)
        elif type == 'DEL':
          result = self.session.delete(url, headers=hds, data=data, verify=self.caCertPath, timeout=self.timeout)
        else:
          return
      except ConnectionError as error:
        self.log.error('ConnectionError: ' + str(error))
        if retry > self.request_retries:
          return S_ERROR(str(error))
        continue

      if result is not None and result.status_code == codes.unauthorized:  # pylint: disable-msg=E1101
        self.session = Session()
        self.__get_token()
        hds['X-Rucio-Auth-Token'] = self.auth_token
      else:
        break

    if result is None:
      return S_ERROR('Rucio Server Connection Exception')
    return result

  def __get_token(self):
    """
    Calls the corresponding method to receive an auth token.
    To be used if a 401 - Unauthorized error is received.

    :return: Dirac S_OK on success and S_ERROR in cas of an error.
    """

    self.log.debug('get a new token')

    for retry in range(self.AUTH_RETRIES + 1):
      result = self.__get_token_x509()
      if not result['OK']:
        self.log.error('x509 authentication failed for account=%s with identity=%s' % (self.account,
                                                                                       self.creds))
        self.log.error(result['Message'])

      if self.auth_token is not None:
        self.__write_token()
        self.headers['X-Rucio-Auth-Token'] = self.auth_token
        break

    if self.auth_token is None:
      return S_ERROR('cannot get an auth token from server')

    return S_OK({})

  def __get_token_x509(self):
    """
    Sends a request to get an auth token from the server and stores it as a class
    attribute. Uses x509 authentication.

    :return: S_OK f the token was successfully received. S_ERROR otherwise.
    """

    headers = {'X-Rucio-Account': self.account, 'X-Rucio-VO': self.vo}

    client_cert = None
    url = os.path.join(self.authHost, 'auth/x509_proxy')
    client_cert = self.creds['client_proxy']

    if not os.path.exists(client_cert):
      self.log.error('given proxy cert (%s) doesn\'t exist' % client_cert)
      return S_ERROR('given proxy cert (%s) doesn\'t exist' % client_cert)

    result = None
    for retry in range(self.AUTH_RETRIES + 1):
      try:
        result = self.session.get(url, headers=headers, cert=client_cert, verify=self.caCertPath)
        break
      except ConnectionError as error:
        self.log.error(str(error))
        return S_ERROR(str(error))

    # Note a response object for a failed request evaluates to false, so we cannot
    # use "not result" here
    if result is None:
      self.log.error('Internal error: Request for authentication token returned no result!')
      return S_ERROR('Internal error: Request for authentication token returned no result!')

    if result.status_code != codes.ok:   # pylint: disable-msg=E1101
      return S_ERROR(self._getError(status_code=result.status_code, data=result.content))

    self.auth_token = result.headers['x-rucio-auth-token']
    return S_OK()

  def __read_token(self):
    """
    Checks if a local token file exists and reads the token from it.

    :return: True if a token could be read. False if no file exists.
    """
    if not os.path.exists(self.tokenFile):
      return S_OK(False)

    try:
      tokenFile_handler = open(self.tokenFile, 'r')
      self.auth_token = tokenFile_handler.readline()
      self.headers['X-Rucio-Auth-Token'] = self.auth_token
    except IOError as error:
      return S_ERROR("I/O error({0}): {1}".format(error.errno, error.strerror))
    except Exception:
      return S_ERROR("Exception when reading a token")
    self.log.debug('got token from file')
    return S_OK(True)

  def __write_token(self):
    """
    Write the current auth_token to the local token file.
    """
    # check if rucio temp directory is there. If not create it with permissions only for the current user
    if not os.path.isdir(self.tokenPath):
      try:
        self.log.debug('rucio token folder \'%s\' not found. Create it.' % self.tokenPath)
        makedirs(self.tokenPath, 0o700)
      except Exception as exc:
        return S_ERROR(str(exc))

    # if the file exists check if the stored token is valid. If not request a
    # new one and overwrite the file. Otherwise use the one from the file
    try:
      file_d, file_n = mkstemp(dir=self.tokenPath)
      with fdopen(file_d, "w") as f_token:
        f_token.write(self.auth_token)
      move(file_n, self.tokenFile)
      return S_OK()
    except IOError as error:
      return S_ERROR("I/O error({0}): {1}".format(error.errno, error.strerror))
    except Exception as exc:
      return S_ERROR(str(exc))

  def _getError(self, status_code=None, data=None):
    """
    Obtain detailed error message and possibly an exception class name (rather than propagating
    any Rucio exceptions)

    :param status_code: HTTP status code
    :type status_code: int
    :param data: exception data
    :type data: str
    :return: A combined error message
    :rtype: str
    """
    try:
      data = Utils.parseResponse(data)
    except ValueError:
      data = {}

    message = data.get('ExceptionMessage', str(_codes.get(status_code, None)))
    return data.get('ExceptionClass', 'Undefined') + ': ' + message
