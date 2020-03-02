# -*- coding: utf-8 -*-
# Copyright 2012-2018 CERN for the benefit of the ATLAS collaboration.
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
# - Thomas Beermann <thomas.beermann@cern.ch>, 2012
# - Vincent Garonne <vgaronne@gmail.com>, 2012-2018
# - Mario Lassnig <mario.lassnig@cern.ch>, 2012
# - Cedric Serfon <cedric.serfon@cern.ch>, 2014
# - Ralph Vigne <ralph.vigne@cern.ch>, 2015
# - Brian Bockelman <bbockelm@cse.unl.edu>, 2018
# - Martin Barisits <martin.barisits@cern.ch>, 2018
# - Andrew Lister <andrew.lister@stfc.ac.uk>, 2019
#
# PY3K COMPATIBLE

"""
Based on:

   https://github.com/rucio/rucio/blob/master/lib/rucio/client/scopeclient.py

  Modified to be used by Dirac. Modifications include:
     - only use a limited number of methods from the original class, which are needed for the Dirac Rucio
       File Catalog Client.
     - eliminate any references to Rucio config file. All values needed to configure the client are
       obtained from Dirac CS.
     - avoid throwing exceptions. They are converted to Dirac S_ERROR or S_OK objects.
"""

try:
  from urllib import quote_plus
except ImportError:
  from urllib.parse import quote_plus

import os
import random
from json import loads
from requests.status_codes import codes

from DIRAC.Resources.Catalog.RucioRESTClientApi.BaseClient import BaseClient
from DIRAC import gLogger, S_OK, S_ERROR


class ScopeClient(BaseClient):

  """Scope client class for working with rucio scopes"""

  SCOPE_BASEURL = 'accounts'

  def __init__(self, rucioHost=None, authHost=None, account=None):
    super(ScopeClient, self).__init__(rucioHost, authHost, account)

  def addScope(self, account, scope):
    """
    Sends the request to add a new scope.

    :param account: the name of the account to add the scope to.
    :param scope: the name of the new scope.
    :return: S_OK(True) if scope was created successfully or an S_ERROR object
    with an appropriate message.
    """

    path = '/'.join([self.SCOPE_BASEURL, account, 'scopes', quote_plus(scope)])
    os.path.join(self.rucioHost, path)
    r = self._sendRequest(url, type='POST')
    if r.status_code == codes.created:
      return S_OK(True)
    else:
      return S_ERROR(self._getError(status_code=r.status_code, data=r.content))

  def listScopes(self):
    """
    Sends the request to list all scopes.

    :return: Dirac S_OK object with a list containing the names of all scopes or
    a S_ERROR object if case of a failure.
    """

    path = '/'.join(['scopes/'])
    # # possibly os.path.join(choice(self.list_hosts),path)
    url = os.path.join(self.rucioHost, path)
    r = self._sendRequest(url)
    if r.status_code == codes.ok:
      scopes = loads(r.text)
      return S_OK(scopes)
    else:
      return S_ERROR(self._getError(status_code=r.status_code, data=r.content))

  def listScopesForAccount(self, account):
    """
    Sends the request to list all scopes for a rucio account.

    :param account: the rucio account to list scopes for.
    :return: a Dirac S_OK object with a list containing the names of all scopes for a rucio account.
    or a S_ERROR object if case of a failure.
    """

    path = '/'.join([self.SCOPE_BASEURL, account, 'scopes/'])

    url = os.path.join(self.rucioHost, path)
    r = self._sendRequest(url)
    if r.status_code == codes.ok:
      scopes = loads(r.text)
      return S_OK(scopes)
    else:
      return S_ERROR(self._getError(status_code=r.status_code, data=r.content))
