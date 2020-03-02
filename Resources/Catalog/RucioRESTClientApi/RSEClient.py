# -*- coding: utf-8 -*-
# Copyright 2012-2020 CERN for the benefit of the ATLAS collaboration.
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
# - Vincent Garonne <vgaronne@gmail.com>, 2012-2018
# - Thomas Beermann <thomas.beermann@cern.ch>, 2012
# - Mario Lassnig <mario.lassnig@cern.ch>, 2012-2020
# - Ralph Vigne <ralph.vigne@cern.ch>, 2013-2015
# - Martin Barisits <martin.barisits@cern.ch>, 2013-2018
# - Cedric Serfon <cedric.serfon@cern.ch>, 2014
# - Wen Guan <wguan.icedew@gmail.com>, 2014
# - Hannes Hansen <hannes.jakob.hansen@cern.ch>, 2018
# - Andrew Lister <andrew.lister@stfc.ac.uk>, 2019
#

"""
  Based on:

   https://github.com/rucio/rucio/blob/master/lib/rucio/client/rseclient.py

  Modified to be used by Dirac. Modifications include:
     - only use a limited number of methods from the original class, which are needed for the Dirac Rucio
       File Catalog Client.
     - eliminate any references to Rucio config file. All values needed to configure the client are
       obtained from Dirac CS.
     - avoid throwing exceptions. They are converted to Dirac S_ERROR or S_OK objects.
"""

import random
import json
from requests.status_codes import codes

from DIRAC.Resources.Catalog.RucioRESTClientApi.BaseClient import BaseClient
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Catalog.RucioRESTClientApi import Utils


class RSEClient(BaseClient):
  """RSE client class for working with rucio RSEs"""

  RSE_BASEURL = 'rses'

  def __init__(self, rucioHost=None, authHost=None, account=None):
    super(RSEClient, self).__init__(rucioHost, authHost, account)

  def lfns2pfns(self, rse, lfns, protocol_domain='ALL', operation=None, scheme=None):
    """
    Dirac modified version. Returns S_OK with PFNs that should be used at a RSE,
    corresponding to requested LFNs.
    The PFNs are generated for the RSE *regardless* of whether a replica exists for the LFN.

    :param rse: the RSE name
    :param lfns: A list of LFN strings to translate to PFNs.
    :param protocol_domain: The scope of the protocol. Supported are 'LAN', 'WAN', and 'ALL' (as default).
    :param operation: The name of the requested operation (read, write, or delete).
                      If None, all operations are queried.
    :param scheme: The identifier of the requested protocol (gsiftp, https, davs, etc).
    :returns: S_OK with a dictionary of LFN / PFN pair or S_ERROR Dirac object with an appropriate message.
    """
    path = '/'.join([self.RSE_BASEURL, rse, 'lfns2pfns'])
    params = []
    if scheme:
      params.append(('scheme', scheme))
    if protocol_domain != 'ALL':
      params.append(('domain', protocol_domain))
    if operation:
      params.append(('operation', operation))
    for lfn in lfns:
      params.append(('lfn', lfn))

    url = Utils.buildURL(self.rucioHost, path=path, params=params, doseq=True)

    r = self._sendRequest(url, type='GET')
    if r.status_code == codes.ok:
      pfns = json.loads(r.text)
      return S_OK(pfns)
    else:
      S_ERROR(self._getError(status_code=r.status_code, data=r.content))
