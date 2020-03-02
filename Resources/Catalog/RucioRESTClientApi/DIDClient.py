# -*- coding: utf-8 -*-
# Copyright 2013-2020 CERN
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
# - Vincent Garonne <vincent.garonne@cern.ch>, 2013-2018
# - Ralph Vigne <ralph.vigne@cern.ch>, 2013-2015
# - Mario Lassnig <mario.lassnig@cern.ch>, 2013-2020
# - Martin Barisits <martin.barisits@cern.ch>, 2013-2020
# - Yun-Pin Sun <winter0128@gmail.com>, 2013
# - Thomas Beermann <thomas.beermann@cern.ch>, 2013
# - Cedric Serfon <cedric.serfon@cern.ch>, 2014-2020
# - Joaquín Bogado <jbogado@linti.unlp.edu.ar>, 2014-2018
# - Brian Bockelman <bbockelm@cse.unl.edu>, 2018
# - Eric Vaandering <ewv@fnal.gov>, 2018-2020
# - asket <asket.agarwal96@gmail.com>, 2018
# - Hannes Hansen <hannes.jakob.hansen@cern.ch>, 2018
# - Andrew Lister <andrew.lister@stfc.ac.uk>, 2019
# - Eli Chadwick <eli.chadwick@stfc.ac.uk>, 2020
# - Aristeidis Fkiaras <aristeidis.fkiaras@cern.ch>, 2020
# - Alan Malta Rodrigues <alan.malta@cern.ch>, 2020
# - Benedikt Ziemons <benedikt.ziemons@cern.ch>, 2020

"""
  Based on:

   https://github.com/rucio/rucio/blob/master/lib/rucio/client/didclient.py

  Modified to be used by Dirac. Modifications include:
     - only use a limited number of methods from the original class, which are needed for the Dirac Rucio
       File Catalog Client.
     - eliminate any references to Rucio config file. All values needed to configure the client are
       obtained from Dirac CS,
     - avoid throwing exceptions. They are converted to Dirac S_ERROR or S_OK objects.

"""

try:
  from urllib import quote_plus
except ImportError:
  from urllib.parse import quote_plus
import os
import random
import json
from requests.status_codes import codes

from DIRAC.Resources.Catalog.RucioRESTClientApi.BaseClient import BaseClient
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Catalog.RucioRESTClientApi import Utils


class DIDClient(BaseClient):

  """DataIdentifier client class for working with data identifiers"""

  DIDS_BASEURL = 'dids'
  ARCHIVES_BASEURL = 'archives'

  def __init__(self, rucioHost=None, authHost=None, account=None):
    super(DIDClient, self).__init__(rucioHost, authHost, account)

  def scopeList(self, scope, name=None, recursive=False):
    """
    List data identifiers in a scope.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param recursive: boolean, True or False.
    :return: Dirac S_OK holding the response or S_ERROR object in case of an error
    """

    payload = {}
    path = '/'.join([self.DIDS_BASEURL, quote_plus(scope), ''])
    if name:
      payload['name'] = name
    if recursive:
      payload['recursive'] = True
    url = Utils.buildURL(self.rucioHost, path=path, params=payload)

    r = self._sendRequest(url, type='GET')
    if r.status_code == codes.ok:
      return S_OK([Utils.parseResponse(line) for line in r.text.split('\n') if line])
    else:
      S_ERROR(self._getError(status_code=r.status_code, data=r.content))

  def getMetadata(self, scope, name, plugin='DID_COLUMN'):
    """
    Get data identifier metadata.

    :param scope: The scope name.
    :param name: The data identifier name.
    """
    path = '/'.join([self.DIDS_BASEURL, quote_plus(scope), quote_plus(name), 'meta'])
    url = os.path.join(self.rucioHost, path)
    payload = {}
    payload['plugin'] = plugin
    r = self._sendRequest(url, type='GET', params=payload)
    if r.status_code == codes.ok:
      meta = Utils.parseResponse(r.text)  # self._load_json_data(r)
      return S_OK(meta)
    else:
      return S_ERROR(self._getError(status_code=r.status_code, data=r.content))
